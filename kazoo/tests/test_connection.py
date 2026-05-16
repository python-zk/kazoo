from __future__ import annotations

from collections import namedtuple, deque
import os
import threading
import time
import uuid
from unittest.mock import patch
import struct
import sys

from typing import Any, Iterable, Deque, Tuple
import pytest

from kazoo.client import KazooClient
from kazoo.exceptions import ConnectionLoss, NotReadOnlyCallError
from kazoo.interfaces import FdLike
from kazoo.protocol.serialization import (
    Connect,
    int_struct,
    write_string,
)
from kazoo.protocol.states import KazooState, KeeperState
from kazoo.protocol.connection import _CONNECTION_DROP
from kazoo.testing import KazooTestCase
from kazoo.tests.util import wait, CI_ZK_VERSION, CI


class Delete(namedtuple("Delete", "path version")):
    type = 2

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(self, bytes: bytes, offset: int) -> None:
        raise ValueError("oh my")


class TestConnectionHandler(KazooTestCase):
    def test_bad_deserialization(self) -> None:
        async_object = self.client.handler.async_result()
        self.client._queue.append(
            (Delete(self.client.chroot, -1), async_object)
        )
        assert self.client._connection._write_sock is not None
        self.client._connection._write_sock.send(b"\0")

        with pytest.raises(ValueError):
            async_object.get()

    def test_with_bad_sessionid(self) -> None:
        ev = threading.Event()

        def expired(state: KazooState) -> None:
            if state == KazooState.CONNECTED:
                ev.set()

        password = os.urandom(16)
        client = self._get_client(client_id=(82838284824, password))
        client.add_listener(expired)
        client.start()
        try:
            ev.wait(15)
            assert ev.is_set()
        finally:
            client.stop()

    def test_connection_read_timeout(self) -> None:
        client = self.client
        ev = threading.Event()
        path = "/" + uuid.uuid4().hex
        handler = client.handler
        _select = handler.select
        _socket = client._connection._socket

        def delayed_select(
            *args: Any, **kwargs: Any
        ) -> tuple[Iterable[FdLike], Iterable[FdLike], Iterable[FdLike]]:
            result = _select(*args, **kwargs)
            if len(args[0]) == 1 and _socket in args[0]:
                # for any socket read, simulate a timeout
                return [], [], []
            return result

        def back(state: KazooState) -> None:
            if state == KazooState.CONNECTED:
                ev.set()

        client.add_listener(back)
        client.create(path, b"1")
        try:
            handler.select = delayed_select  # type: ignore[method-assign]
            with pytest.raises(ConnectionLoss):
                client.get(path)
        finally:
            handler.select = _select  # type: ignore[method-assign]
        # the client reconnects automatically
        ev.wait(5)
        assert ev.is_set()
        assert client.get(path)[0] == b"1"

    def test_connection_write_timeout(self) -> None:
        client = self.client
        ev = threading.Event()
        path = "/" + uuid.uuid4().hex
        handler = client.handler
        _select = handler.select
        _socket = client._connection._socket

        def delayed_select(
            *args: Any, **kwargs: Any
        ) -> tuple[Iterable[FdLike], Iterable[FdLike], Iterable[FdLike]]:
            result = _select(*args, **kwargs)
            if _socket in args[1]:
                # for any socket write, simulate a timeout
                return [], [], []
            return result

        def back(state: KazooState) -> None:
            if state == KazooState.CONNECTED:
                ev.set()

        client.add_listener(back)

        try:
            handler.select = delayed_select  # type: ignore[method-assign]
            with pytest.raises(ConnectionLoss):
                client.create(path)
        finally:
            handler.select = _select  # type: ignore[method-assign]
        # the client reconnects automatically
        ev.wait(5)
        assert ev.is_set()
        assert client.exists(path) is None

    def test_connection_deserialize_fail(self) -> None:
        client = self.client
        ev = threading.Event()
        path = "/" + uuid.uuid4().hex
        handler = client.handler
        _select = handler.select
        _socket = client._connection._socket

        def delayed_select(
            *args: Any, **kwargs: Any
        ) -> tuple[Iterable[FdLike], Iterable[FdLike], Iterable[FdLike]]:
            result = _select(*args, **kwargs)
            if _socket in args[1]:
                # for any socket write, simulate a timeout
                return [], [], []
            return result

        def back(state: KazooState) -> None:
            if state == KazooState.CONNECTED:
                ev.set()

        client.add_listener(back)

        deserialize_ev = threading.Event()

        def bad_deserialize(_bytes: bytes, offset: int) -> None:
            deserialize_ev.set()
            raise struct.error()

        # force the connection to die but, on reconnect, cause the
        # server response to be non-deserializable. ensure that the client
        # continues to retry. This partially reproduces a rare bug seen
        # in production.

        with patch.object(Connect, "deserialize") as mock_deserialize:
            mock_deserialize.side_effect = bad_deserialize
            try:
                handler.select = delayed_select  # type: ignore[method-assign]
                with pytest.raises(ConnectionLoss):
                    client.create(path)
            finally:
                handler.select = _select  # type: ignore[method-assign]
            # the client reconnects automatically but the first attempt will
            # hit a deserialize failure. wait for that.
            deserialize_ev.wait(5)
            assert deserialize_ev.is_set()

        # this time should succeed
        ev.wait(5)
        assert ev.is_set()
        assert client.exists(path) is None

    def test_connection_close(self) -> None:
        with pytest.raises(Exception):
            self.client.close()
        self.client.stop()
        self.client.close()

        # should be able to restart
        self.client.start()

    def test_connection_sock(self) -> None:
        client = self.client
        read_sock = client._connection._read_sock
        write_sock = client._connection._write_sock

        assert read_sock is not None
        assert write_sock is not None

        # stop client and socket should not yet be closed
        client.stop()
        assert read_sock is not None
        assert write_sock is not None

        read_sock.getsockname()
        write_sock.getsockname()

        # close client, and sockets should be closed
        client.close()

        # Todo check socket closing

        # start client back up. should get a new, valid socket
        client.start()
        read_sock = client._connection._read_sock
        write_sock = client._connection._write_sock

        assert read_sock is not None
        assert write_sock is not None
        read_sock.getsockname()
        write_sock.getsockname()

    def test_dirty_sock(self) -> None:
        client = self.client
        read_sock = client._connection._read_sock
        write_sock = client._connection._write_sock
        assert read_sock is not None
        assert write_sock is not None

        # add a stray byte to the socket and ensure that doesn't
        # blow up client. simulates case where some error leaves
        # a byte in the socket which doesn't correspond to the
        # request queue.
        write_sock.send(b"\0")

        # eventually this byte should disappear from socket
        wait(lambda: client.handler.select([read_sock], [], [], 0)[0] == [])


class TestConnectionDrop(KazooTestCase):
    def test_connection_dropped(self) -> None:
        ev = threading.Event()

        def back(state: KazooState) -> None:
            if state == KazooState.CONNECTED:
                ev.set()

        # create a node with a large value and stop the ZK node
        path = "/" + uuid.uuid4().hex
        self.client.create(path)
        self.client.add_listener(back)
        result = self.client.set_async(path, b"a" * 1000 * 1024)
        self.client._call(_CONNECTION_DROP, None)  # type: ignore[arg-type]

        with pytest.raises(ConnectionLoss):
            result.get()
        # we have a working connection to a new node
        ev.wait(30)
        assert ev.is_set()


class TestReadOnlyMode(KazooTestCase):
    def setUp(self) -> None:
        os.environ["ZOOKEEPER_LOCAL_SESSION_RO"] = "true"
        self.setup_zookeeper()
        skip = False
        if CI_ZK_VERSION and CI_ZK_VERSION < (3, 4):
            skip = True
        elif CI_ZK_VERSION and CI_ZK_VERSION >= (3, 4):
            skip = False
        else:
            ver = self.client.server_version()
            if ver[1] < 4:
                skip = True
        if skip:
            pytest.skip("Must use Zookeeper 3.4 or above")

    def tearDown(self) -> None:
        self.client.stop()
        os.environ.pop("ZOOKEEPER_LOCAL_SESSION_RO", None)

    def test_read_only(self) -> None:
        if CI:
            # force some wait to make sure the data produced during the
            # `setUp()` step are replicated to all zk members
            # if not done the `get_children()` test may fail because the
            # node does not exist on the node that we will keep alive
            time.sleep(15)
        # do not keep the client started in the `setUp` step alive
        self.client.stop()
        client = self._get_client(connection_retry=None, read_only=True)
        ev = threading.Event()

        def listen(state: KazooState) -> bool | None:
            if client.client_state == KeeperState.CONNECTED_RO:
                ev.set()
            return None

        client.add_listener(listen)

        client.start()
        try:
            # stopping both nodes at the same time
            # else the test seems flaky when on CI hosts
            zk_stop_threads = []
            zk_stop_threads.append(
                threading.Thread(target=self.cluster[1].stop, daemon=True)
            )
            zk_stop_threads.append(
                threading.Thread(target=self.cluster[2].stop, daemon=True)
            )
            for thread in zk_stop_threads:
                thread.start()
            for thread in zk_stop_threads:
                thread.join()
            # stopping the client is *mandatory*, else the client might try to
            # reconnect using a xid that the server may endlessly refuse
            # restarting the client makes sure the xid gets reset
            client.stop()
            client.start()
            ev.wait(15)
            assert ev.is_set()
            assert client.client_state == KeeperState.CONNECTED_RO

            # Test read only command
            assert client.get_children("/") == []

            # Test error with write command
            with pytest.raises(NotReadOnlyCallError):
                client.create("/fred")

            # Wait for a ping
            time.sleep(15)
        finally:
            client.remove_listener(listen)
            self.cluster[1].run()
            self.cluster[2].run()


class TestUnorderedXids(KazooTestCase):
    def setUp(self) -> None:
        super(TestUnorderedXids, self).setUp()

        self.connection = self.client._connection
        self.connection_routine = self.connection._connection_routine

        self._pending = self.client._pending
        self.client._pending = _naughty_deque()

    def tearDown(self) -> None:
        self.client._pending = self._pending
        super(TestUnorderedXids, self).tearDown()

    def _get_client(self, **kwargs: Any) -> KazooClient:
        # overrides for patching zk_loop
        c = KazooTestCase._get_client(self, **kwargs)
        self._zk_loop = c._connection.zk_loop
        self._zk_loop_errors: list[BaseException] = []
        c._connection.zk_loop = (  # type: ignore[method-assign]
            self._zk_loop_func
        )
        return c

    def _zk_loop_func(self, *args: Any, **kwargs: Any) -> None:
        # patched zk_loop which will catch and collect all RuntimeError
        try:
            self._zk_loop(*args, **kwargs)
        except RuntimeError as e:
            self._zk_loop_errors.append(e)

    def test_xids_mismatch(self) -> None:
        from kazoo.protocol.states import KeeperState

        ev = threading.Event()
        error_stack = []

        def listen(state: KazooState) -> bool | None:
            if self.client.client_state == KeeperState.CLOSED:
                ev.set()
            return None

        self.client.add_listener(listen)

        def log_exception(*args: Any) -> None:
            error_stack.append((args, sys.exc_info()))

        self.connection.logger.exception = (  # type: ignore[method-assign]
            log_exception  # type: ignore[assignment]
        )

        ev.clear()
        with pytest.raises(RuntimeError):
            self.client.get_children("/")

        ev.wait()
        self.client.remove_listener(listen)
        assert self.client.connected is False
        assert self.client.state == "LOST"
        assert self.client.client_state == KeeperState.CLOSED

        args, exc_info = error_stack[-1]
        assert args == ("Unhandled exception in connection loop",)
        assert exc_info[0] == RuntimeError

        self.client.handler.sleep_func(0.2)
        assert self.connection_routine is not None
        assert not self.connection_routine.is_alive()
        assert len(self._zk_loop_errors) == 1
        assert self._zk_loop_errors[0] == exc_info[1]


class _naughty_deque(Deque[Tuple[Any, Any, int]]):
    def append(self, s: Tuple[Any, Any, int]) -> None:
        request, async_object, xid = s
        deque.append(self, (request, async_object, xid + 1))  # +1s
