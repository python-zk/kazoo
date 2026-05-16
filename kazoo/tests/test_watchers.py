from __future__ import annotations

import time
import threading
import uuid

from typing import Any, List, Literal

import pytest

from kazoo.exceptions import KazooException
from kazoo.protocol.states import EventType, WatchedEvent, ZnodeStat
from kazoo.recipe.watchers import PatientChildrenWatch

from kazoo.testing import KazooTestCase


class KazooDataWatcherTests(KazooTestCase):
    def setUp(self) -> None:
        super(KazooDataWatcherTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex
        self.client.ensure_path(self.path)

    def test_data_watcher(self) -> None:
        update = threading.Event()
        data: list[bool | bytes | None] = [True]

        # Make it a non-existent path
        self.path += "f"

        @self.client.DataWatch(self.path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> bool | None:
            data.pop()
            data.append(d)
            update.set()
            return None

        update.wait(10)
        assert data == [None]
        update.clear()

        self.client.create(self.path, b"fred")
        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

    def test_data_watcher_once(self) -> None:
        update = threading.Event()
        data: list[bool | bytes | None] = [True]

        # Make it a non-existent path
        self.path += "f"

        dwatcher = self.client.DataWatch(self.path)

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

    def test_data_watcher_with_event(self) -> None:
        # Test that the data watcher gets passed the event, if it
        # accepts three arguments
        update = threading.Event()
        data: list[Literal[True] | WatchedEvent | None] = [True]

        # Make it a non-existent path
        self.path += "f"

        @self.client.DataWatch(self.path)
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

        self.client.create(self.path, b"fred")
        update.wait(10)
        assert data[0] is not None and data[0] is not True
        assert data[0].type == EventType.CREATED
        update.clear()

    def test_func_style_data_watch(self) -> None:
        update = threading.Event()
        data: list[bytes | None | Literal[True]] = [True]

        # Make it a non-existent path
        path = self.path + "f"

        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        self.client.DataWatch(path, changed)

        update.wait(10)
        assert data == [None]
        update.clear()

        self.client.create(path, b"fred")
        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

    def test_datawatch_across_session_expire(self) -> None:
        update = threading.Event()
        data: list[bytes | None | Literal[True]] = [True]

        @self.client.DataWatch(self.path)
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            data.pop()
            data.append(d)
            update.set()

        update.wait(10)
        assert data == [b""]
        update.clear()

        self.expire_session(threading.Event)
        self.client.retry(self.client.set, self.path, b"fred")
        update.wait(25)
        assert data[0] == b"fred"

    def test_func_stops(self) -> None:
        update = threading.Event()
        data: list[bytes | None | Literal[True]] = [True]

        self.path += "f"

        fail_through: list[bool] = []

        @self.client.DataWatch(self.path)
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
        self.client.create(self.path, b"fred")
        update.wait(10)
        assert data[0] == b"fred"
        update.clear()

        self.client.set(self.path, b"asdfasdf")
        update.wait(0.2)
        assert data[0] == b"fred"

        d, stat = self.client.get(self.path)
        assert d == b"asdfasdf"

    def test_no_such_node(self) -> None:
        args = []

        @self.client.DataWatch("/some/path")
        def changed(d: bytes | None, stat: ZnodeStat | None) -> None:
            args.extend([d, stat])

        assert args == [None, None]

    def test_no_such_node_for_children_watch(self) -> None:
        args = []
        path = self.path + "/test_no_such_node_for_children_watch"
        update = threading.Event()

        def changed(children: list[str] | None) -> None:
            args.append(children)
            update.set()

        # watch a node which does not exist
        children_watch = self.client.ChildrenWatch(path, changed)
        assert update.is_set() is False
        assert children_watch._stopped is True
        assert args == []

        # watch a node which exists
        self.client.create(path, b"")
        children_watch = self.client.ChildrenWatch(path, changed)
        update.wait(3)
        assert args == [[]]
        update.clear()

        # watch changes
        self.client.create(path + "/fred", b"")
        update.wait(3)
        assert args == [[], ["fred"]]
        update.clear()

        # delete children
        self.client.delete(path + "/fred")
        update.wait(3)
        assert args == [[], ["fred"], []]
        update.clear()

        # delete watching
        self.client.delete(path)

        # a hack for waiting the watcher stop
        for retry in range(5):
            if children_watch._stopped:
                break
            children_watch._run_lock.acquire()
            children_watch._run_lock.release()
            time.sleep(retry / 10.0)

        assert update.is_set() is False
        assert children_watch._stopped is True

    def test_watcher_evaluating_to_false(self) -> None:
        class WeirdWatcher(List[Any]):
            def __call__(self, *args: Any) -> None:
                self.called = True

        watcher = WeirdWatcher()
        self.client.DataWatch(self.path, watcher)
        self.client.set(self.path, b"mwahaha")
        assert watcher.called is True

    def test_watcher_repeat_delete(self) -> None:
        a = []
        ev = threading.Event()

        self.client.delete(self.path)

        @self.client.DataWatch(self.path)
        def changed(val: bytes | None, stat: ZnodeStat | None) -> None:
            a.append(val)
            ev.set()

        assert a == [None]
        ev.wait(10)
        ev.clear()
        self.client.create(self.path, b"blah")
        ev.wait(10)
        assert ev.is_set() is True
        ev.clear()
        assert a == [None, b"blah"]
        self.client.delete(self.path)
        ev.wait(10)
        assert ev.is_set() is True
        ev.clear()
        assert a == [None, b"blah", None]
        self.client.create(self.path, b"blah")
        ev.wait(10)
        assert ev.is_set() is True
        ev.clear()
        assert a == [None, b"blah", None, b"blah"]

    def test_watcher_with_closing(self) -> None:
        a = []
        ev = threading.Event()

        self.client.delete(self.path)

        @self.client.DataWatch(self.path)
        def changed(val: bytes | None, stat: ZnodeStat | None) -> None:
            a.append(val)
            ev.set()

        assert a == [None]

        b = False
        try:
            self.client.stop()
        except:  # noqa
            b = True
        assert b is False


class KazooChildrenWatcherTests(KazooTestCase):
    def setUp(self) -> None:
        super(KazooChildrenWatcherTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex
        self.client.ensure_path(self.path)

    def test_child_watcher(self) -> None:
        update = threading.Event()
        all_children = ["fred"]

        @self.client.ChildrenWatch(self.path)
        def changed(children: list[str] | None) -> None:
            assert children is not None
            while all_children:
                all_children.pop()
            all_children.extend(children)
            update.set()

        update.wait(10)
        assert all_children == []
        update.clear()

        self.client.create(self.path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()

        self.client.create(self.path + "/" + "george")
        update.wait(10)
        assert sorted(all_children) == ["george", "smith"]

    def test_child_watcher_once(self) -> None:
        update = threading.Event()
        all_children = ["fred"]

        cwatch = self.client.ChildrenWatch(self.path)

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

    def test_child_watcher_with_event(self) -> None:
        update = threading.Event()
        events: list[WatchedEvent | None | Literal[True]] = [True]

        @self.client.ChildrenWatch(self.path, send_event=True)
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

        self.client.create(self.path + "/" + "smith")
        update.wait(10)
        assert events[0] is not None
        assert events[0] is not True
        assert events[0].type == EventType.CHILD
        update.clear()

    def test_func_style_child_watcher(self) -> None:
        update = threading.Event()
        all_children = ["fred"]

        def changed(children: list[str] | None) -> None:
            while all_children:
                all_children.pop()
            assert children is not None
            all_children.extend(children)
            update.set()

        self.client.ChildrenWatch(self.path, changed)

        update.wait(10)
        assert all_children == []
        update.clear()

        self.client.create(self.path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()

        self.client.create(self.path + "/" + "george")
        update.wait(10)
        assert sorted(all_children) == ["george", "smith"]

    def test_func_stops(self) -> None:
        update = threading.Event()
        all_children = ["fred"]

        fail_through: list[bool] = []

        @self.client.ChildrenWatch(self.path)
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
        self.client.create(self.path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()

        self.client.create(self.path + "/" + "george")
        update.wait(0.5)
        assert all_children == ["smith"]

    def test_child_watcher_remove_session_watcher(self) -> None:
        update = threading.Event()
        all_children = ["fred"]

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

        children_watch = self.client.ChildrenWatch(self.path, changed)
        session_watcher = children_watch._session_watcher

        update.wait(10)
        assert session_watcher in self.client.state_listeners
        assert all_children == []
        update.clear()

        fail_through.append(True)
        self.client.create(self.path + "/" + "smith")
        update.wait(10)
        assert session_watcher not in self.client.state_listeners
        assert all_children == ["smith"]
        update.clear()

        self.client.create(self.path + "/" + "george")
        update.wait(10)
        assert session_watcher not in self.client.state_listeners
        assert all_children == ["smith"]

    def test_child_watch_session_loss(self) -> None:
        update = threading.Event()
        all_children = ["fred"]

        @self.client.ChildrenWatch(self.path)
        def changed(children: list[str] | None) -> None:
            while all_children:
                all_children.pop()
            assert children is not None
            all_children.extend(children)
            update.set()

        update.wait(10)
        assert all_children == []
        update.clear()

        self.client.create(self.path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()
        self.expire_session(threading.Event)

        self.client.retry(self.client.create, self.path + "/" + "george")
        update.wait(20)
        assert sorted(all_children) == ["george", "smith"]

    def test_child_stop_on_session_loss(self) -> None:
        update = threading.Event()
        all_children = ["fred"]

        @self.client.ChildrenWatch(self.path, allow_session_lost=False)
        def changed(children: list[str] | None) -> None:
            while all_children:
                all_children.pop()
            assert children is not None
            all_children.extend(children)
            update.set()

        update.wait(10)
        assert all_children == []
        update.clear()

        self.client.create(self.path + "/" + "smith")
        update.wait(10)
        assert all_children == ["smith"]
        update.clear()
        self.expire_session(threading.Event)

        self.client.retry(self.client.create, self.path + "/" + "george")
        update.wait(4)
        assert update.is_set() is False
        assert all_children == ["smith"]

        children = self.client.get_children(self.path)
        assert sorted(children) == ["george", "smith"]


class KazooPatientChildrenWatcherTests(KazooTestCase):
    def setUp(self) -> None:
        super(KazooPatientChildrenWatcherTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def _makeOne(self, *args: Any, **kwargs: Any) -> PatientChildrenWatch:
        return PatientChildrenWatch(*args, **kwargs)

    def test_watch(self) -> None:
        self.client.ensure_path(self.path)
        watcher = self._makeOne(self.client, self.path, 0.1)
        result = watcher.start()
        children, asy = result.get()
        assert len(children) == 0
        assert asy.ready() is False

        self.client.create(self.path + "/" + "fred")
        asy.get(timeout=1)
        assert asy.ready() is True

    def test_exception(self) -> None:
        from kazoo.exceptions import NoNodeError

        watcher = self._makeOne(self.client, self.path, 0.1)
        result = watcher.start()

        with pytest.raises(NoNodeError):
            result.get()

    def test_watch_iterations(self) -> None:
        self.client.ensure_path(self.path)
        watcher = self._makeOne(self.client, self.path, 0.5)
        result = watcher.start()
        assert result.ready() is False

        time.sleep(0.08)
        self.client.create(self.path + "/" + uuid.uuid4().hex)
        assert result.ready() is False
        time.sleep(0.08)
        assert result.ready() is False
        self.client.create(self.path + "/" + uuid.uuid4().hex)
        time.sleep(0.08)
        assert result.ready() is False

        children, asy = result.get()
        assert len(children) == 2
