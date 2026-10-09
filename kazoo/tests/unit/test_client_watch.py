from __future__ import annotations

import pytest

from kazoo.client import KazooClient
from kazoo.exceptions import NoWatcherError, UnimplementedError
from kazoo.protocol.serialization import AddWatch, RemoveWatches
from kazoo.protocol.states import AddWatchMode, WatcherType


class TestClientAddWatch:
    """Unit tests for KazooClient.add_watch and add_watch_async."""

    def test_add_watch_sync_success(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set(None)
            return True

        client._call = mock_call  # type: ignore[method-assign]
        result = client.add_watch(
            "/a", lambda ev: None, AddWatchMode.PERSISTENT
        )
        assert result is None

    def test_add_watch_async_persistent(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        called_ops = []

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            called_ops.append(op)
            async_result.set(None)
            return True

        client._call = mock_call  # type: ignore[method-assign]
        cb = lambda ev: None  # noqa: E731
        client.add_watch_async("/a", cb, AddWatchMode.PERSISTENT)

        assert len(called_ops) == 1
        assert isinstance(called_ops[0], AddWatch)
        assert called_ops[0].path == "/a"
        assert called_ops[0].watcher == cb
        assert called_ops[0].mode == AddWatchMode.PERSISTENT

    def test_add_watch_async_persistent_recursive(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        called_ops = []

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            called_ops.append(op)
            async_result.set(None)
            return True

        client._call = mock_call  # type: ignore[method-assign]
        cb = lambda ev: None  # noqa: E731
        client.add_watch_async("/a", cb, AddWatchMode.PERSISTENT_RECURSIVE)

        assert len(called_ops) == 1
        assert isinstance(called_ops[0], AddWatch)
        assert called_ops[0].path == "/a"
        assert called_ops[0].watcher == cb
        assert called_ops[0].mode == AddWatchMode.PERSISTENT_RECURSIVE

    def test_add_watch_async_chroot(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181/chroot_ns")
        called_ops = []

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            called_ops.append(op)
            async_result.set(None)
            return True

        client._call = mock_call  # type: ignore[method-assign]
        client.add_watch_async(
            "/sub", lambda ev: None, AddWatchMode.PERSISTENT
        )

        assert len(called_ops) == 1
        assert called_ops[0].path == "/chroot_ns/sub"

    def test_invalid_path_type(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        with pytest.raises(TypeError, match="Invalid type for 'path'"):
            client.add_watch(
                123,  # type: ignore[arg-type]
                lambda ev: None,
                AddWatchMode.PERSISTENT,
            )

    def test_invalid_watch_type(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        with pytest.raises(TypeError, match="Invalid type for 'watch'"):
            client.add_watch(
                "/a",
                "not_callable",  # type: ignore[arg-type]
                AddWatchMode.PERSISTENT,
            )

    def test_invalid_mode_type(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        with pytest.raises(TypeError, match="Invalid type for 'mode'"):
            client.add_watch(
                "/a",
                lambda ev: None,
                "persistent",  # type: ignore[arg-type]
            )

    def test_invalid_mode_value(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        with pytest.raises(ValueError, match="Invalid value for 'mode'"):
            client.add_watch("/a", lambda ev: None, 99)

    def test_add_watch_unimplemented_error(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set_exception(UnimplementedError("unimplemented"))
            return True

        client._call = mock_call  # type: ignore[method-assign]
        with pytest.raises(UnimplementedError):
            client.add_watch("/a", lambda ev: None, AddWatchMode.PERSISTENT)


class TestClientRemoveAllWatches:
    """Unit tests for KazooClient.remove_all_watches and
    remove_all_watches_async.
    """

    def test_remove_all_watches_sync_success(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set(None)
            return True

        client._call = mock_call  # type: ignore[method-assign]
        result = client.remove_all_watches("/a", WatcherType.ANY)
        assert result is None

    @pytest.mark.parametrize(
        "watcher_type",
        [WatcherType.CHILDREN, WatcherType.DATA, WatcherType.ANY],
    )
    def test_remove_all_watches_async_valid_types(
        self, watcher_type: WatcherType
    ) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        called_ops = []

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            called_ops.append(op)
            async_result.set(None)
            return True

        client._call = mock_call  # type: ignore[method-assign]
        client.remove_all_watches_async("/a", watcher_type)

        assert len(called_ops) == 1
        assert isinstance(called_ops[0], RemoveWatches)
        assert called_ops[0].path == "/a"
        assert called_ops[0].watcher_type == watcher_type

    def test_remove_all_watches_async_chroot(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181/chroot_ns")
        called_ops = []

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            called_ops.append(op)
            async_result.set(None)
            return True

        client._call = mock_call  # type: ignore[method-assign]
        client.remove_all_watches_async("/sub", WatcherType.ANY)

        assert len(called_ops) == 1
        assert called_ops[0].path == "/chroot_ns/sub"

    def test_invalid_path_type(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        with pytest.raises(TypeError, match="Invalid type for 'path'"):
            client.remove_all_watches(123, WatcherType.ANY)  # type: ignore[arg-type]

    def test_invalid_watcher_type_type(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        with pytest.raises(TypeError, match="Invalid type for 'watcher_type'"):
            client.remove_all_watches("/a", "any")  # type: ignore[arg-type]

    def test_invalid_watcher_type_value(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        with pytest.raises(
            ValueError, match="Invalid value for 'watcher_type'"
        ):
            client.remove_all_watches("/a", 99)

    def test_remove_all_watches_unimplemented_error(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set_exception(UnimplementedError("unimplemented"))
            return True

        client._call = mock_call  # type: ignore[method-assign]
        with pytest.raises(UnimplementedError):
            client.remove_all_watches("/a", WatcherType.ANY)

    def test_remove_all_watches_no_watcher_error(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set_exception(NoWatcherError("no watcher"))
            return True

        client._call = mock_call  # type: ignore[method-assign]
        with pytest.raises(NoWatcherError):
            client.remove_all_watches("/a", WatcherType.ANY)
