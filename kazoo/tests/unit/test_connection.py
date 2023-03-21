from __future__ import annotations

import collections
from unittest.mock import Mock, patch

import pytest

from kazoo.exceptions import (
    SASLException,
    SessionClosedRequireSaslError,
)
from kazoo.protocol.connection import (
    ConnectionHandler,
    STOP_CONNECTING,
)
from kazoo.protocol.serialization import ReplyHeader
from kazoo.protocol.states import KeeperState
from kazoo.retry import KazooRetry


class TestConnectionAuthExceptions:
    def test_connect_attempt_sasl_exception(self) -> None:
        client = Mock()
        handler = Mock()
        handler.timeout_exception = TimeoutError
        client.handler = handler
        client._state = KeeperState.CONNECTING
        retry = KazooRetry()
        connection = ConnectionHandler(client, retry)

        with patch.object(
            connection, "_connect", side_effect=SASLException("library error")
        ):
            result = connection._connect_attempt(
                "127.0.0.1", "127.0.0.1", 2181, retry
            )
            assert result is STOP_CONNECTING
            client._session_callback.assert_called_with(
                KeeperState.AUTH_FAILED
            )

    def test_connect_attempt_session_closed_require_sasl(self) -> None:
        client = Mock()
        handler = Mock()
        handler.timeout_exception = TimeoutError
        client.handler = handler
        client._state = KeeperState.CONNECTING
        retry = KazooRetry()
        connection = ConnectionHandler(client, retry)

        with patch.object(
            connection,
            "_connect",
            side_effect=SessionClosedRequireSaslError(
                "session closed require sasl"
            ),
        ):
            result = connection._connect_attempt(
                "127.0.0.1", "127.0.0.1", 2181, retry
            )
            assert result is STOP_CONNECTING
            client._session_callback.assert_called_with(
                KeeperState.AUTH_FAILED
            )


class TestConnectionXidMismatch:
    def test_read_response_xid_mismatch_sets_exception_and_raises(
        self,
    ) -> None:
        client = Mock()
        async_obj = Mock()
        request = Mock()
        expected_xid = 1
        client._pending = collections.deque(
            [(request, async_obj, expected_xid)]
        )

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=2, zxid=100, err=0)

        with pytest.raises(RuntimeError) as exc_info:
            connection._read_response(header, b"", 0)

        assert exc_info.value.args == (
            "xids do not match, expected %r received %r",
            1,
            2,
        )
        assert client.last_zxid == 100
        async_obj.set_exception.assert_called_once_with(exc_info.value)

    def test_invoke_xid_mismatch_raises(self) -> None:
        client = Mock()
        connection = ConnectionHandler(client, KazooRetry())

        header = ReplyHeader(xid=99, zxid=50, err=0)
        with patch.object(connection, "_submit"):
            with patch.object(
                connection, "_read_header", return_value=(header, b"", 0)
            ):
                with pytest.raises(RuntimeError) as exc_info:
                    connection._invoke(timeout=1.0, request=Mock(), xid=42)

        assert exc_info.value.args == (
            "xids do not match, expected %r received %r",
            42,
            99,
        )

    def test_read_socket_dispatches_to_read_response_on_mismatch(
        self,
    ) -> None:
        client = Mock()
        async_obj = Mock()
        request = Mock()
        client._pending = collections.deque([(request, async_obj, 5)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=6, zxid=200, err=0)

        with patch.object(
            connection, "_read_header", return_value=(header, b"", 0)
        ):
            with pytest.raises(RuntimeError) as exc_info:
                connection._read_socket(read_timeout=1.0)

        assert exc_info.value.args == (
            "xids do not match, expected %r received %r",
            5,
            6,
        )
        async_obj.set_exception.assert_called_once_with(exc_info.value)


class TestPersistentWatchEvents:
    def test_find_persistent_recursive_watchers(self) -> None:
        client = Mock()
        w_root = Mock()
        w_a = Mock()
        client._persistent_recursive_watchers = {
            "/": [w_root],
            "/a": [w_a],
        }
        connection = ConnectionHandler(client, KazooRetry())

        assert connection._find_persistent_recursive_watchers("/") == [w_root]
        assert connection._find_persistent_recursive_watchers("/b") == [w_root]
        assert connection._find_persistent_recursive_watchers("/a") == [
            w_root,
            w_a,
        ]
        assert connection._find_persistent_recursive_watchers("/a/b") == [
            w_root,
            w_a,
        ]
        assert connection._find_persistent_recursive_watchers("/a/b/c") == [
            w_root,
            w_a,
        ]

    def test_child_event_triggers_persistent_watcher(self) -> None:
        from kazoo.protocol.serialization import int_int_struct, write_string

        client = Mock()
        client._stopped.is_set.return_value = False
        client._state = KeeperState.CONNECTED
        client.unchroot.side_effect = lambda p: p
        w_child = Mock()
        w_pers = Mock()
        client._child_watchers = {"/a": {w_child}}
        client._persistent_watchers = {"/a": {w_pers}}
        client._persistent_recursive_watchers = {}

        connection = ConnectionHandler(client, KazooRetry())
        # Watch(type=4 (CHILD_EVENT), state=3, path="/a")
        buf = int_int_struct.pack(4, 3) + write_string("/a")
        connection._read_watch_event(buf, 0)

        # Both child watch and persistent watch should be dispatched
        assert client.handler.dispatch_callback.call_count == 2
        callbacks = [
            call.args[0]
            for call in client.handler.dispatch_callback.call_args_list
        ]
        called_funcs = {cb.func for cb in callbacks}
        assert called_funcs == {w_child, w_pers}
        # One-time child watcher should be popped
        assert "/a" not in client._child_watchers
        # Persistent watcher should remain
        assert client._persistent_watchers["/a"] == {w_pers}

    def test_created_event_triggers_persistent_and_recursive_watchers(
        self,
    ) -> None:
        from kazoo.protocol.serialization import int_int_struct, write_string

        client = Mock()
        client._stopped.is_set.return_value = False
        client._state = KeeperState.CONNECTED
        client.unchroot.side_effect = lambda p: p
        w_data = Mock()
        w_pers = Mock()
        w_rec = Mock()
        client._data_watchers = {"/a/b": {w_data}}
        client._persistent_watchers = {"/a/b": {w_pers}}
        client._persistent_recursive_watchers = {"/a": [w_rec]}

        connection = ConnectionHandler(client, KazooRetry())
        # Watch(type=1 (CREATED_EVENT), state=3, path="/a/b")
        buf = int_int_struct.pack(1, 3) + write_string("/a/b")
        connection._read_watch_event(buf, 0)

        assert client.handler.dispatch_callback.call_count == 3
        callbacks = [
            call.args[0]
            for call in client.handler.dispatch_callback.call_args_list
        ]
        called_funcs = {cb.func for cb in callbacks}
        assert called_funcs == {w_data, w_pers, w_rec}
        # One-time data watcher should be popped
        assert "/a/b" not in client._data_watchers
        # Persistent and recursive watchers remain
        assert client._persistent_watchers["/a/b"] == {w_pers}
        assert client._persistent_recursive_watchers["/a"] == [w_rec]

    def test_changed_event_triggers_persistent_and_recursive_watchers(
        self,
    ) -> None:
        from kazoo.protocol.serialization import int_int_struct, write_string

        client = Mock()
        client._stopped.is_set.return_value = False
        client._state = KeeperState.CONNECTED
        client.unchroot.side_effect = lambda p: p
        w_data = Mock()
        w_pers = Mock()
        w_rec = Mock()
        client._data_watchers = {"/a/b": {w_data}}
        client._persistent_watchers = {"/a/b": {w_pers}}
        client._persistent_recursive_watchers = {"/a": [w_rec]}

        connection = ConnectionHandler(client, KazooRetry())
        # Watch(type=3 (CHANGED_EVENT), state=3, path="/a/b")
        buf = int_int_struct.pack(3, 3) + write_string("/a/b")
        connection._read_watch_event(buf, 0)

        assert client.handler.dispatch_callback.call_count == 3
        # One-time data watcher should be popped
        assert "/a/b" not in client._data_watchers
        # Persistent and recursive watchers remain
        assert client._persistent_watchers["/a/b"] == {w_pers}
        assert client._persistent_recursive_watchers["/a"] == [w_rec]

    def test_deleted_event_triggers_all_watchers(self) -> None:
        from kazoo.protocol.serialization import int_int_struct, write_string

        client = Mock()
        client._stopped.is_set.return_value = False
        client._state = KeeperState.CONNECTED
        client.unchroot.side_effect = lambda p: p
        w_data = Mock()
        w_child = Mock()
        w_pers = Mock()
        w_rec = Mock()
        client._data_watchers = {"/a": {w_data}}
        client._child_watchers = {"/a": {w_child}}
        client._persistent_watchers = {"/a": {w_pers}}
        client._persistent_recursive_watchers = {"/": [w_rec]}

        connection = ConnectionHandler(client, KazooRetry())
        # Watch(type=2 (DELETED_EVENT), state=3, path="/a")
        buf = int_int_struct.pack(2, 3) + write_string("/a")
        connection._read_watch_event(buf, 0)

        assert client.handler.dispatch_callback.call_count == 4
        # Data and child watchers should be popped
        assert "/a" not in client._data_watchers
        assert "/a" not in client._child_watchers
        # Persistent and recursive watchers remain
        assert client._persistent_watchers["/a"] == {w_pers}
        assert client._persistent_recursive_watchers["/"] == [w_rec]


class TestConnectionHandlerWatchRegistration:
    """Unit tests for ConnectionHandler registering and removing watchers in
    read_response.
    """

    def test_read_response_add_watch_persistent(self) -> None:
        from collections import defaultdict, deque
        from kazoo.protocol.serialization import AddWatch
        from kazoo.protocol.states import AddWatchMode

        client = Mock()
        client._stopped.is_set.return_value = False
        client._persistent_watchers = defaultdict(set)
        client._persistent_recursive_watchers = defaultdict(set)

        w = Mock()
        async_obj = Mock()
        request = AddWatch("/a", w, AddWatchMode.PERSISTENT)
        client._pending = deque([(request, async_obj, 1)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=1, zxid=10, err=0)
        connection._read_response(header, b"", 0)

        assert client._persistent_watchers["/a"] == {w}
        assert len(client._persistent_recursive_watchers) == 0
        async_obj.set.assert_called_once_with(None)

    def test_read_response_add_watch_persistent_recursive(self) -> None:
        from collections import defaultdict, deque
        from kazoo.protocol.serialization import AddWatch
        from kazoo.protocol.states import AddWatchMode

        client = Mock()
        client._stopped.is_set.return_value = False
        client._persistent_watchers = defaultdict(set)
        client._persistent_recursive_watchers = defaultdict(set)

        w = Mock()
        async_obj = Mock()
        request = AddWatch("/a", w, AddWatchMode.PERSISTENT_RECURSIVE)
        client._pending = deque([(request, async_obj, 1)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=1, zxid=10, err=0)
        connection._read_response(header, b"", 0)

        assert client._persistent_recursive_watchers["/a"] == {w}
        assert len(client._persistent_watchers) == 0

    def test_read_response_remove_watches_children(self) -> None:
        from collections import deque
        from kazoo.protocol.serialization import RemoveWatches
        from kazoo.protocol.states import WatcherType

        client = Mock()
        client._stopped.is_set.return_value = False
        w1, w2 = Mock(), Mock()
        client._child_watchers = {"/a": {w1}}
        client._data_watchers = {"/a": {w2}}

        async_obj = Mock()
        request = RemoveWatches("/a", WatcherType.CHILDREN)
        client._pending = deque([(request, async_obj, 1)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=1, zxid=10, err=0)
        connection._read_response(header, b"", 0)

        assert "/a" not in client._child_watchers
        assert client._data_watchers["/a"] == {w2}

    def test_read_response_remove_watches_data(self) -> None:
        from collections import deque
        from kazoo.protocol.serialization import RemoveWatches
        from kazoo.protocol.states import WatcherType

        client = Mock()
        client._stopped.is_set.return_value = False
        w1, w2 = Mock(), Mock()
        client._child_watchers = {"/a": {w1}}
        client._data_watchers = {"/a": {w2}}

        async_obj = Mock()
        request = RemoveWatches("/a", WatcherType.DATA)
        client._pending = deque([(request, async_obj, 1)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=1, zxid=10, err=0)
        connection._read_response(header, b"", 0)

        assert "/a" not in client._data_watchers
        assert client._child_watchers["/a"] == {w1}

    def test_read_response_remove_watches_any(self) -> None:
        from collections import deque
        from kazoo.protocol.serialization import RemoveWatches
        from kazoo.protocol.states import WatcherType

        client = Mock()
        client._stopped.is_set.return_value = False
        w1, w2, w3, w4 = Mock(), Mock(), Mock(), Mock()
        client._child_watchers = {"/a": {w1}}
        client._data_watchers = {"/a": {w2}}
        client._persistent_watchers = {"/a": {w3}}
        client._persistent_recursive_watchers = {"/a": {w4}}

        async_obj = Mock()
        request = RemoveWatches("/a", WatcherType.ANY)
        client._pending = deque([(request, async_obj, 1)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=1, zxid=10, err=0)
        connection._read_response(header, b"", 0)

        assert "/a" not in client._child_watchers
        assert "/a" not in client._data_watchers
        assert "/a" not in client._persistent_watchers
        assert "/a" not in client._persistent_recursive_watchers

    def test_read_response_add_watch_unexpected_mode(self) -> None:
        from collections import deque
        from kazoo.protocol.serialization import AddWatch

        client = Mock()
        client._stopped.is_set.return_value = False

        w = Mock()
        async_obj = Mock()
        request = AddWatch("/a", w, 999)  # invalid mode
        client._pending = deque([(request, async_obj, 1)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=1, zxid=10, err=0)
        with pytest.raises(ValueError, match="Unexpected AddWatchMode: 999"):
            connection._read_response(header, b"", 0)

    def test_read_response_remove_watches_unexpected_type(self) -> None:
        from collections import deque
        from kazoo.protocol.serialization import RemoveWatches

        client = Mock()
        client._stopped.is_set.return_value = False

        async_obj = Mock()
        request = RemoveWatches("/a", 999)  # invalid type
        client._pending = deque([(request, async_obj, 1)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=1, zxid=10, err=0)
        with pytest.raises(ValueError, match="Unexpected WatcherType: 999"):
            connection._read_response(header, b"", 0)
