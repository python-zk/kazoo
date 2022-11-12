from __future__ import annotations

import threading
import time
from unittest.mock import Mock

import pytest

from kazoo.client import KazooClient
from kazoo.exceptions import ConfigurationError, ConnectionLoss
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.protocol.connection import ConnectionHandler
from kazoo.protocol.states import KeeperState


class DummyRequest:
    type = 1

    def __init__(self, name: str) -> None:
        self.name = name

    def serialize(self) -> bytearray:
        return bytearray(b"\x00" * 4)


class TestRateControl:
    def test_client_init_concurrent_request_limit(self) -> None:
        # Default is None (disabled)
        client = KazooClient()
        assert client.concurrent_request_limit is None

        # Explicit limit
        client_limited = KazooClient(concurrent_request_limit=10)
        assert client_limited.concurrent_request_limit == 10

        # Explicit None
        client_none = KazooClient(concurrent_request_limit=None)
        assert client_none.concurrent_request_limit is None

        # Non-positive values must raise ConfigurationError
        with pytest.raises(ConfigurationError):
            KazooClient(concurrent_request_limit=0)

        with pytest.raises(ConfigurationError):
            KazooClient(concurrent_request_limit=-5)

    def test_async_calls_never_block(self) -> None:
        handler = SequentialThreadingHandler()
        client = KazooClient(
            hosts="127.0.0.1:2181",
            handler=handler,
            concurrent_request_limit=1,
        )
        client._state = KeeperState.CONNECTED
        client._live.set()

        mock_sock = Mock()
        mock_conn = Mock()
        mock_conn._write_sock = mock_sock
        client._connection = mock_conn

        # Submit 10 requests rapidly
        results = []
        start_time = time.monotonic()
        for i in range(10):
            res = handler.async_result()
            client._call(DummyRequest(f"req_{i}"), res)
            results.append(res)
        elapsed = time.monotonic() - start_time

        # Ensure all 10 calls were non-blocking (took negligible time)
        assert elapsed < 0.5
        assert len(client._queue) == 10
        assert mock_sock.send.call_count == 10

    def test_completion_worker_chained_requests_no_deadlock(self) -> None:
        handler = SequentialThreadingHandler()
        handler.start()
        try:
            client = KazooClient(
                hosts="127.0.0.1:2181",
                handler=handler,
                concurrent_request_limit=1,
            )
            client._state = KeeperState.CONNECTED
            client._live.set()

            mock_sock = Mock()
            mock_conn = Mock()
            mock_conn._write_sock = mock_sock
            client._connection = mock_conn

            res1 = handler.async_result()
            client._call(DummyRequest("req1"), res1)

            callback_done = threading.Event()

            def on_res1_complete(result: object) -> None:
                # Chained requests executed from within completion worker
                res2 = handler.async_result()
                client._call(DummyRequest("req2"), res2)

                res3 = handler.async_result()
                client._call(DummyRequest("req3"), res3)

                callback_done.set()

            res1.rawlink(on_res1_complete)

            # Complete req1
            res1.set("done1")

            # Must not deadlock; callback_done should be set within 2 seconds
            assert callback_done.wait(timeout=2.0), (
                "Completion worker deadlocked on chained requests"
            )
            assert len(client._queue) == 3
        finally:
            handler.stop()

    def test_connection_loss_drains_cleanly(self) -> None:
        handler = SequentialThreadingHandler()
        client = KazooClient(
            hosts="127.0.0.1:2181",
            handler=handler,
            concurrent_request_limit=2,
        )

        res_pending1 = handler.async_result()
        res_pending2 = handler.async_result()
        res_queued1 = handler.async_result()
        res_queued2 = handler.async_result()

        client._pending.append((DummyRequest("p1"), res_pending1, 1))
        client._pending.append((DummyRequest("p2"), res_pending2, 2))
        client._queue.append((DummyRequest("q1"), res_queued1))
        client._queue.append((DummyRequest("q2"), res_queued2))

        client._notify_pending(KeeperState.CONNECTING)

        assert len(client._pending) == 0
        assert len(client._queue) == 0

        for res in [res_pending1, res_pending2, res_queued1, res_queued2]:
            assert res.ready()
            assert isinstance(res.exception, ConnectionLoss)

    def test_wire_rate_limiting_select_gating(self) -> None:
        handler = SequentialThreadingHandler()
        client = KazooClient(
            hosts="127.0.0.1:2181",
            handler=handler,
            concurrent_request_limit=2,
        )
        conn = ConnectionHandler(client, client.retry)

        # Mock sockets
        dummy_socket = Mock()
        dummy_read_sock = Mock()
        conn._socket = dummy_socket
        conn._read_sock = dummy_read_sock

        captured_read_lists: list[list[object]] = []

        def mock_select(
            rlist: list[object] | list[Mock],
            wlist: list[object],
            xlist: list[object],
            timeout: float | None = None,
        ) -> tuple[list[object], list[object], list[object]]:
            captured_read_lists.append(list(rlist))
            return [], [], []

        def can_send() -> bool:
            limit = client.concurrent_request_limit
            return limit is None or len(client._pending) < limit

        def check_select() -> None:
            read_list = [dummy_socket]
            if can_send():
                read_list.append(dummy_read_sock)
            mock_select(read_list, [], [], 1.0)

        # Case 1: _pending has 0 items (< limit of 2) -> _read_sock included
        client._pending.clear()
        check_select()
        assert dummy_read_sock in captured_read_lists[-1]

        # Case 2: _pending has 1 item (< limit of 2) -> _read_sock included
        client._pending.append((DummyRequest("p1"), handler.async_result(), 1))
        check_select()
        assert dummy_read_sock in captured_read_lists[-1]

        # Case 3: _pending has 2 items (== limit of 2) -> _read_sock EXCLUDED
        client._pending.append((DummyRequest("p2"), handler.async_result(), 2))
        check_select()
        assert dummy_read_sock not in captured_read_lists[-1]

        # Case 4: _pending has 3 items (> limit of 2) -> _read_sock EXCLUDED
        client._pending.append((DummyRequest("p3"), handler.async_result(), 3))
        check_select()
        assert dummy_read_sock not in captured_read_lists[-1]

        # Case 5: Limit disabled (None) -> _read_sock always included
        client.concurrent_request_limit = None
        check_select()
        assert dummy_read_sock in captured_read_lists[-1]

    def test_wire_rate_limiting_pipeline_simulation(self) -> None:
        """Simulate pipeline draining with concurrent_request_limit=2."""
        handler = SequentialThreadingHandler()
        client = KazooClient(
            hosts="127.0.0.1:2181",
            handler=handler,
            concurrent_request_limit=2,
        )
        conn = ConnectionHandler(client, client.retry)

        # Mock read_sock and write_sock
        mock_read_sock = Mock()
        conn._read_sock = mock_read_sock
        conn._submit = Mock()  # type: ignore[method-assign]
        conn._xid = 0

        # Queue 5 requests
        results = []
        for i in range(5):
            res = handler.async_result()
            client._queue.append((DummyRequest(f"req_{i}"), res))
            results.append(res)

        # Helper to check if can_send
        def can_send() -> bool:
            limit = client.concurrent_request_limit
            return limit is None or len(client._pending) < limit

        # Tick 1: send request 0
        assert can_send()
        conn._send_request(10.0, 10.0)
        assert len(client._pending) == 1
        assert len(client._queue) == 4

        # Tick 2: send request 1
        assert can_send()
        conn._send_request(10.0, 10.0)
        assert len(client._pending) == 2
        assert len(client._queue) == 3

        # Tick 3: limit reached! can_send must be False
        assert not can_send()

        # Response arrives for request 0
        req, async_obj, xid = client._pending.popleft()
        async_obj.set("resp_0")
        assert len(client._pending) == 1

        # Now can_send is True again!
        assert can_send()

        # Tick 4: send request 2
        conn._send_request(10.0, 10.0)
        assert len(client._pending) == 2
        assert len(client._queue) == 2
        assert not can_send()

        # Complete remaining requests
        while client._pending or client._queue:
            if can_send() and client._queue:
                conn._send_request(10.0, 10.0)
            elif client._pending:
                req, async_obj, xid = client._pending.popleft()
                async_obj.set(f"resp_{xid}")

        assert len(client._pending) == 0
        assert len(client._queue) == 0
        for res in results:
            assert res.ready()

    def test_tree_cache_reconnect_simulation_no_deadlock(self) -> None:
        """Simulate TreeCache with limit=1 during reconnect."""
        from kazoo.recipe.cache import TreeCache, TreeNode
        from kazoo.protocol.states import ZnodeStat

        handler = SequentialThreadingHandler()
        handler.start()
        try:
            client = KazooClient(
                hosts="127.0.0.1:2181",
                handler=handler,
                concurrent_request_limit=1,
            )
            client._state = KeeperState.CONNECTED
            client._live.set()

            mock_sock = Mock()
            mock_conn = Mock()
            mock_conn._write_sock = mock_sock
            client._connection = mock_conn

            cache = TreeCache(client, "/test")
            # Build an in-memory tree of 5 child nodes
            root = cache._root
            for i in range(5):
                child_node = TreeNode(cache, f"/test/node_{i}", root)
                root._children[f"node_{i}"] = child_node

            # Start the cache background task thread
            cache._task_thread = client.handler.spawn(cache._do_background)

            # Trigger on_reconnected, which walks all nodes
            cache._in_background(root.on_reconnected)

            # Wait briefly for all requests to be enqueued in client._queue
            deadline = time.monotonic() + 2.0
            while len(client._queue) < 12 and time.monotonic() < deadline:
                time.sleep(0.01)

            # 6 nodes total (root + 5 children),
            # each calls get and get_children = 12 requests
            assert len(client._queue) == 12

            # Now simulate the server responding to each request
            dummy_stat = ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            while client._queue:
                req, async_obj = client._queue.popleft()
                is_get_children = (
                    "get_children" in getattr(req, "name", "")
                    or "children" in str(type(req)).lower()
                )
                if is_get_children:
                    async_obj.set([])
                else:
                    async_obj.set((b"data", dummy_stat))

            # Wait for TreeCache background worker to process all callbacks
            deadline = time.monotonic() + 2.0
            while cache._outstanding_ops > 0 and time.monotonic() < deadline:
                time.sleep(0.01)

            # Stop cache background thread
            cache._task_queue.put(cache._STOP)
            if cache._task_thread:
                cache._task_thread.join(timeout=2.0)

            # Outstanding ops should have reached 0
            assert cache._outstanding_ops == 0
        finally:
            handler.stop()
