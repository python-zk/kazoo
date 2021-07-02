from __future__ import annotations

import time
import uuid
from typing import Any, List, Literal, TYPE_CHECKING

import pytest

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from kazoo.protocol.states import (
    EventType,
    WatchedEvent,
    ZnodeStat,
)
from kazoo.recipe.watchers import PatientChildrenWatch

if TYPE_CHECKING:
    pass


class TestDataWatcher:
    def test_data_watcher(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        data: list[bool | bytes | None] = [True]
        path = "/" + uuid.uuid4().hex

        @zkclient.DataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> bool | None:
            data.pop()
            data.append(d)
            update.set()
            return None

        update.wait(10)
        assert data == [None]
        update.clear()

        zkclient.create(path, b"fred")
        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

    def test_data_watcher_once(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        data: list[bool | bytes | None] = [True]
        path = "/" + uuid.uuid4().hex

        dwatcher = zkclient.DataWatch(path)

        @dwatcher
        def changed(d: bytes | None, stat: ZnodeStat | None) -> bool | None:
            data.pop()
            data.append(d)
            update.set()
            return None

        update.wait(10)
        assert data == [None]
        update.clear()

        with pytest.raises(KazooException):

            @dwatcher
            def func(d: bytes | None, stat: ZnodeStat | None) -> bool | None:
                data.pop()
                return None

    def test_data_watcher_with_event(self, zkclient: KazooClient) -> None:
        # Test that the data watcher gets passed the event, if it
        # accepts three arguments
        update = zkclient.handler.event_object()
        data: list[Literal[True] | WatchedEvent | None] = [True]
        path = "/" + uuid.uuid4().hex

        @zkclient.DataWatch(path)
        def changed(
            d: bytes | None, stat: ZnodeStat | None, event: WatchedEvent | None
        ) -> bool | None:
            data.pop()
            data.append(event)
            update.set()
            return None

        update.wait(10)
        assert data == [None]
        update.clear()

        zkclient.create(path, b"fred")
        update.wait(10)
        assert data[0] is not None and data[0] is not True
        assert data[0].type == EventType.CREATED
        update.clear()

    def test_func_style_data_watch(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        data: list[bytes | None | Literal[True]] = [True]
        path = "/" + uuid.uuid4().hex

        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        zkclient.DataWatch(path, changed)

        update.wait(10)
        assert data == [None]
        update.clear()

        zkclient.create(path, b"fred")
        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

    def test_datawatch_across_session_expire(
        self, zkclient: KazooClient
    ) -> None:
        update = zkclient.handler.event_object()
        data: list[bytes | None | Literal[True]] = [True]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        @zkclient.DataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        update.wait(10)
        assert data == [b""]
        update.clear()

        zkclient.harness_expire_session()
        zkclient.retry(zkclient.set, path, b"fred")
        update.wait(25)
        assert data[0] == b"fred"

    def test_func_stops(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        data: list[bytes | None | Literal[True]] = [True]
        path = "/" + uuid.uuid4().hex

        fail_through: list[bool] = []

        @zkclient.DataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> bool | None:
            data.pop()
            data.append(d)
            update.set()
            if fail_through:
                return False
            return None

        update.wait(10)
        assert data == [None]
        update.clear()

        fail_through.append(True)
        zkclient.create(path, b"fred")
        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

        zkclient.set(path, b"asdfasdf")
        update.wait(0.2)
        assert data[0] == b"fred"

        d, stat = zkclient.get(path)
        assert d == b"asdfasdf"

    def test_no_such_node(self, zkclient: KazooClient) -> None:
        args = []

        @zkclient.DataWatch("/some/path")
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            args.extend([d, stat])

        assert args == [None, None]

    def test_no_such_node_for_children_watch(
        self, zkclient: KazooClient
    ) -> None:
        args = []
        path = "/" + uuid.uuid4().hex
        update = zkclient.handler.event_object()

        def changed(children: list[str] | None) -> None:
            args.append(children)
            update.set()

        # watch a node which does not exist
        children_watch = zkclient.ChildrenWatch(path, changed)
        assert update.is_set() is False
        assert children_watch._stopped is True
        assert args == []

        # watch a node which exists
        zkclient.create(path, b"")
        children_watch = zkclient.ChildrenWatch(path, changed)
        update.wait(3)
        assert args == [[]]
        update.clear()

        # watch changes
        zkclient.create(path + "/fred", b"")
        update.wait(3)
        assert args == [[], ["fred"]]
        update.clear()

        # delete children
        zkclient.delete(path + "/fred")
        update.wait(3)
        assert args == [[], ["fred"], []]
        update.clear()

        # delete watching
        zkclient.delete(path)

        # a hack for waiting the watcher stop
        for retry in range(5):
            if children_watch._stopped:
                break
            children_watch._run_lock.acquire()
            children_watch._run_lock.release()
            time.sleep(retry / 10.0)

        assert update.is_set() is False
        assert children_watch._stopped is True

    def test_watcher_evaluating_to_false(self, zkclient: KazooClient) -> None:
        class WeirdWatcher(List[Any]):
            def __call__(self, *args: Any) -> None:
                self.called = True

        watcher = WeirdWatcher()
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)
        zkclient.DataWatch(path, watcher)
        zkclient.set(path, b"mwahaha")
        assert watcher.called is True

    def test_watcher_repeat_delete(self, zkclient: KazooClient) -> None:
        a = []
        ev = zkclient.handler.event_object()
        path = "/" + uuid.uuid4().hex

        @zkclient.DataWatch(path)
        def changed(val: bytes | None, stat: ZnodeStat | None) -> None:
            a.append(val)
            ev.set()

        assert a == [None]
        ev.wait(10)
        ev.clear()
        zkclient.create(path, b"blah")
        ev.wait(10)
        assert ev.is_set() is True
        ev.clear()
        assert a == [None, b"blah"]
        zkclient.delete(path)
        ev.wait(10)
        assert ev.is_set() is True
        ev.clear()
        assert a == [None, b"blah", None]
        zkclient.create(path, b"blah")
        ev.wait(10)
        assert ev.is_set() is True
        ev.clear()
        assert a == [None, b"blah", None, b"blah"]

    def test_watcher_with_closing(self, zkclient: KazooClient) -> None:
        a = []
        ev = zkclient.handler.event_object()
        path = "/" + uuid.uuid4().hex

        @zkclient.DataWatch(path)
        def changed(val: bytes | None, stat: ZnodeStat | None) -> None:
            a.append(val)
            ev.set()

        assert a == [None]

        b = False
        try:
            zkclient.stop()
        except:  # noqa
            b = True
        assert b is False


class TestExistingDataWatcher:
    def test_data_watcher_non_existent_path(
        self, zkclient: KazooClient
    ) -> None:
        update = zkclient.handler.event_object()
        data: list[bool | bytes | None] = [True]
        path = "/" + uuid.uuid4().hex

        @zkclient.ExistingDataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        update.wait(10)
        assert data == [None]
        update.clear()

        # We should not get an update
        zkclient.create(path, b"fred")
        update.wait(0.2)
        assert data == [None]
        update.clear()

    def test_data_watcher_existing_path(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        data: list[bool | bytes | None] = [True]
        path = "/" + uuid.uuid4().hex
        zkclient.create(path, b"fred")

        @zkclient.ExistingDataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

    def test_data_watcher_delete(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        data: list[bool | bytes | None] = [True]
        path = "/" + uuid.uuid4().hex
        zkclient.create(path, b"fred")

        @zkclient.ExistingDataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

        zkclient.delete(path)
        update.wait(10)
        assert data == [None]
        update.clear()

        zkclient.create(path, b"ginger")
        update.wait(0.2)
        assert data == [None]
        update.clear()

    def test_data_watcher_modify(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        data: list[bool | bytes | None] = [True]
        path = "/" + uuid.uuid4().hex
        zkclient.create(path, b"fred")

        @zkclient.ExistingDataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

        zkclient.set(path, b"wilma")
        update.wait(10)
        assert data[0] == b"wilma"
        update.clear()

        zkclient.set(path, b"betty")
        update.wait(10)
        assert data[0] == b"betty"
        update.clear()

    def test_data_watcher_with_event_and_delete(
        self, zkclient: KazooClient
    ) -> None:
        update = zkclient.handler.event_object()
        events: list[tuple[bytes | None, WatchedEvent | None]] = []
        path = "/" + uuid.uuid4().hex
        zkclient.create(path, b"initial")

        @zkclient.ExistingDataWatch(path)
        def changed(
            d: bytes | None,
            stat: ZnodeStat | None,
            event: WatchedEvent | None = None,
        ) -> None:
            events.append((d, event))
            update.set()

        update.wait(10)
        assert len(events) == 1
        assert events[0][0] == b"initial"
        assert events[0][1] is None
        update.clear()

        zkclient.set(path, b"updated")
        update.wait(10)
        assert len(events) == 2
        assert events[1][0] == b"updated"
        assert events[1][1] is not None
        assert events[1][1].type == EventType.CHANGED
        update.clear()

        zkclient.delete(path)
        update.wait(10)
        assert len(events) == 3
        assert events[2][0] is None
        assert events[2][1] is not None
        assert events[2][1].type == EventType.DELETED
        update.clear()

    @pytest.mark.zk_version(">=3.6")
    def test_data_watcher_no_server_watch_leak(
        self, zkclient: KazooClient
    ) -> None:
        update = zkclient.handler.event_object()
        data: list[bool | bytes | None] = [True]
        path = "/" + uuid.uuid4().hex
        zkclient.create(path, b"leak_test")

        @zkclient.ExistingDataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        update.wait(10)
        assert data[0] == b"leak_test"
        update.clear()

        zkclient.delete(path)
        update.wait(10)
        assert data == [None]

        # In standard DataWatch, an exists watch is set on the deleted node.
        # ExistingDataWatch must NOT leave any watch behind on the deleted
        # node.
        full_path = zkclient.chroot + path
        assert full_path not in zkclient._data_watchers

        # Recreating the node must not trigger any further callback
        zkclient.create(path, b"leak_test_recreated")
        time.sleep(0.5)
        assert data == [None]

    def test_data_watcher_return_false_unregisters(
        self, zkclient: KazooClient
    ) -> None:
        update = zkclient.handler.event_object()
        data: list[bytes | None] = []
        path = "/" + uuid.uuid4().hex
        zkclient.create(path, b"one")

        @zkclient.ExistingDataWatch(path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> bool | None:
            data.append(d)
            update.set()
            if d == b"two":
                return False
            return None

        update.wait(10)
        assert data == [b"one"]
        update.clear()

        zkclient.set(path, b"two")
        update.wait(10)
        assert data == [b"one", b"two"]
        update.clear()

        zkclient.set(path, b"three")
        update.wait(0.5)
        # Should not have received b"three" because False was returned
        assert data == [b"one", b"two"]


class TestChildrenWatcher:
    def test_child_watcher(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        all_children = ["fred"]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        @zkclient.ChildrenWatch(path)
        def changed(children: list[str] | None) -> None:
            assert children is not None
            while all_children:
                all_children.pop()
            all_children.extend(children)
            update.set()

        update.wait(10)
        assert all_children == []
        update.clear()

        zkclient.create(path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()

        zkclient.create(path + "/" + "george")
        update.wait(10)
        assert sorted(all_children) == ["george", "smith"]

    def test_child_watcher_once(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        all_children = ["fred"]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        cwatch = zkclient.ChildrenWatch(path)

        @cwatch
        def changed(children: list[str] | None) -> None:
            while all_children:
                all_children.pop()
            assert children is not None
            all_children.extend(children)
            update.set()

        update.wait(10)
        assert all_children == []
        update.clear()

        with pytest.raises(KazooException):

            @cwatch
            def changed_again(children: list[str] | None) -> None:
                update.set()

    def test_child_watcher_with_event(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        events: list[WatchedEvent | None | Literal[True]] = [True]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        @zkclient.ChildrenWatch(path, send_event=True)
        def changed(
            children: list[str] | None, event: WatchedEvent | None
        ) -> bool | None:
            events.pop()
            events.append(event)
            update.set()
            return None

        update.wait(10)
        assert events == [None]
        update.clear()

        zkclient.create(path + "/" + "smith")
        update.wait(10)
        assert events[0] is not None
        assert events[0] is not True
        assert events[0].type == EventType.CHILD
        update.clear()

    def test_func_style_child_watcher(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        all_children = ["fred"]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        def changed(children: list[str] | None) -> None:
            while all_children:
                all_children.pop()
            assert children is not None
            all_children.extend(children)
            update.set()

        zkclient.ChildrenWatch(path, changed)

        update.wait(10)
        assert all_children == []
        update.clear()

        zkclient.create(path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()

        zkclient.create(path + "/" + "george")
        update.wait(10)
        assert sorted(all_children) == ["george", "smith"]

    def test_func_stops(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        all_children = ["fred"]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        fail_through: list[bool] = []

        @zkclient.ChildrenWatch(path)
        def changed(children: list[str] | None) -> bool | None:
            assert children is not None
            while all_children:
                all_children.pop()
            all_children.extend(children)
            update.set()
            if fail_through:
                return False
            return None  # ? True?

        update.wait(10)
        assert all_children == []
        update.clear()

        fail_through.append(True)
        zkclient.create(path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()

        zkclient.create(path + "/" + "george")
        update.wait(0.5)
        assert all_children == ["smith"]

    def test_child_watcher_remove_session_watcher(
        self, zkclient: KazooClient
    ) -> None:
        update = zkclient.handler.event_object()
        all_children = ["fred"]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        fail_through: list[bool] = []

        def changed(children: list[str] | None) -> bool | None:
            while all_children:
                all_children.pop()
            assert children is not None
            all_children.extend(children)
            update.set()
            if fail_through:
                return False
            return None  # ?

        children_watch = zkclient.ChildrenWatch(path, changed)
        session_watcher = children_watch._session_watcher

        update.wait(10)
        assert session_watcher in zkclient.state_listeners
        assert all_children == []
        update.clear()

        fail_through.append(True)
        zkclient.create(path + "/" + "smith")
        update.wait(10)
        assert session_watcher not in zkclient.state_listeners
        assert all_children == ["smith"]
        update.clear()

        zkclient.create(path + "/" + "george")
        update.wait(10)
        assert session_watcher not in zkclient.state_listeners
        assert all_children == ["smith"]

    def test_child_watch_session_loss(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        all_children = ["fred"]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        @zkclient.ChildrenWatch(path)
        def changed(children: list[str] | None) -> None:
            while all_children:
                all_children.pop()
            assert children is not None
            all_children.extend(children)
            update.set()

        update.wait(10)
        assert all_children == []
        update.clear()

        zkclient.create(path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()
        zkclient.harness_expire_session()

        zkclient.retry(zkclient.create, path + "/" + "george")
        update.wait(20)
        assert sorted(all_children) == ["george", "smith"]

    def test_child_stop_on_session_loss(self, zkclient: KazooClient) -> None:
        update = zkclient.handler.event_object()
        all_children = ["fred"]
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)

        @zkclient.ChildrenWatch(path, allow_session_lost=False)
        def changed(children: list[str] | None) -> None:
            while all_children:
                all_children.pop()
            assert children is not None
            all_children.extend(children)
            update.set()

        update.wait(10)
        assert all_children == []
        update.clear()

        zkclient.create(path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()
        zkclient.harness_expire_session()

        zkclient.retry(zkclient.create, path + "/" + "george")
        update.wait(4)
        assert update.is_set() is False
        assert all_children == ["smith"]

        children = zkclient.get_children(path)
        assert sorted(children) == ["george", "smith"]


class TestPatientChildrenWatcher:
    def _makeOne(self, *args: Any, **kwargs: Any) -> PatientChildrenWatch:
        from kazoo.recipe.watchers import PatientChildrenWatch

        return PatientChildrenWatch(*args, **kwargs)

    def test_watch(self, zkclient: KazooClient) -> None:
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)
        watcher = self._makeOne(zkclient, path, 0.1)
        result = watcher.start()
        children, asy = result.get()
        assert len(children) == 0
        assert asy.ready() is False

        zkclient.create(path + "/" + "fred")
        asy.get(timeout=1)
        assert asy.ready() is True

    def test_exception(self, zkclient: KazooClient) -> None:
        from kazoo.exceptions import NoNodeError

        path = "/" + uuid.uuid4().hex
        watcher = self._makeOne(zkclient, path, 0.1)
        result = watcher.start()

        with pytest.raises(NoNodeError):
            result.get()

    def test_watch_iterations(self, zkclient: KazooClient) -> None:
        path = "/" + uuid.uuid4().hex
        zkclient.ensure_path(path)
        watcher = self._makeOne(zkclient, path, 0.5)
        result = watcher.start()
        assert result.ready() is False

        time.sleep(0.08)
        zkclient.create(path + "/" + uuid.uuid4().hex)
        assert result.ready() is False
        time.sleep(0.08)
        assert result.ready() is False
        zkclient.create(path + "/" + uuid.uuid4().hex)
        time.sleep(0.08)
        assert result.ready() is False

        children, asy = result.get()
        assert len(children) == 2
