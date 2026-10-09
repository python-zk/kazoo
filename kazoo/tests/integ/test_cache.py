from __future__ import annotations

import gc
import uuid
from unittest.mock import Mock, call, patch
from typing import TYPE_CHECKING

import pytest
from objgraph import count as count_refs_by_type

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from kazoo.recipe.cache import TreeCache, TreeEvent, TreeNode

if TYPE_CHECKING:
    from queue import Queue


class FakeException(Exception):
    pass


class TestKazooTreeCache:
    cache: None | TreeCache
    _path: str
    _event_queue: Queue
    _error_queue: Queue

    @pytest.fixture(autouse=True)
    def _test_setup(self, zkclient):
        self._event_queue = zkclient.handler.queue_impl()
        self._error_queue = zkclient.handler.queue_impl()
        self._path = "/" + uuid.uuid4().hex
        self.cache = TreeCache(zkclient, self._path)
        self.cache.listen(lambda event: self._event_queue.put(event))
        self.cache.listen_fault(lambda error: self._error_queue.put(error))
        self.cache.start()

        yield

        if not self._error_queue.empty():
            try:
                raise self._error_queue.get()
            except FakeException:
                pass
        if self.cache is not None:
            self.cache.close()
            self.cache = None

    def _wait_cache(
        self, expect=None, since=None, timeout=10
    ) -> TreeEvent | None:
        started = since is None
        while True:
            event = self._event_queue.get(timeout=timeout)
            if started:
                if expect is not None:
                    assert event.event_type == expect
                return event
            if event.event_type == since:
                started = True
                if expect is None:
                    return

    def _spy_client(self, client: KazooClient, method_name):
        method = getattr(client, method_name)
        return patch.object(client, method_name, wraps=method)

    def _wait_gc(self, client: KazooClient):
        # trigger switching on some coroutine handlers
        client.handler.sleep_func(0.1)

        completion_queue = getattr(client.handler, "completion_queue", None)
        if completion_queue is not None:
            while not client.handler.completion_queue.empty():
                client.handler.sleep_func(0.1)

        for gen in range(3):
            gc.collect(gen)

    def _count_tree_node(self, client: KazooClient) -> int:
        # inspect GC and count tree nodes for checking memory leak
        for retry in range(10):
            result = set()
            for _ in range(5):
                self._wait_gc(client)
                result.add(count_refs_by_type("TreeNode"))
            if len(result) == 1:
                return list(result)[0]
        raise RuntimeError("could not count refs exactly")

    def test_start(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        stat = zkclient.exists(self._path)
        assert stat.version == 0

        assert self.cache is not None
        assert self.cache._state == TreeCache.STATE_STARTED
        assert self.cache._root._state == TreeNode.STATE_LIVE

    def test_start_started(self):
        with pytest.raises(KazooException):
            self.cache.start()

    def test_start_closed(self):
        self.cache.close()
        with pytest.raises(KazooException):
            self.cache.start()

    def test_close(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)
        assert self._count_tree_node(zkclient) == 1  # For the root node

        zkclient.create(self._path + "/foo/bar/baz", makepath=True)
        for _ in range(3):
            self._wait_cache(TreeEvent.NODE_ADDED)

        # setup stub watchers which are outside of tree cache
        stub_data_watcher = Mock(spec=lambda event: None)
        stub_child_watcher = Mock(spec=lambda event: None)
        zkclient.get(self._path + "/foo", stub_data_watcher)
        zkclient.get_children(self._path + "/foo", stub_child_watcher)

        # watchers inside tree cache should be here
        root_path = zkclient.chroot + self._path
        assert len(zkclient._data_watchers[root_path + "/foo"]) == 2
        assert len(zkclient._data_watchers[root_path + "/foo/bar"]) == 1
        assert len(zkclient._data_watchers[root_path + "/foo/bar/baz"]) == 1
        assert len(zkclient._child_watchers[root_path + "/foo"]) == 2
        assert len(zkclient._child_watchers[root_path + "/foo/bar"]) == 1
        assert len(zkclient._child_watchers[root_path + "/foo/bar/baz"]) == 1

        self.cache.close()

        # nothing should be published since tree closed
        assert self._event_queue.empty()

        # tree should be empty
        assert self.cache._root._children == {}
        assert self.cache._root._data is None
        assert self.cache._state == TreeCache.STATE_CLOSED

        # node state should not be changed
        assert self.cache._root._state != TreeNode.STATE_DEAD

        # watchers should be reset
        assert len(zkclient._data_watchers[root_path + "/foo"]) == 1
        assert len(zkclient._data_watchers[root_path + "/foo/bar"]) == 0
        assert len(zkclient._data_watchers[root_path + "/foo/bar/baz"]) == 0
        assert len(zkclient._child_watchers[root_path + "/foo"]) == 1
        assert len(zkclient._child_watchers[root_path + "/foo/bar"]) == 0
        assert len(zkclient._child_watchers[root_path + "/foo/bar/baz"]) == 0

        # outside watchers should not be deleted
        assert (
            list(zkclient._data_watchers[root_path + "/foo"])[0]
            == stub_data_watcher
        )
        assert (
            list(zkclient._child_watchers[root_path + "/foo"])[0]
            == stub_child_watcher
        )

        # should not be any leaked memory (tree node) here
        self.cache = None
        assert self._count_tree_node(zkclient) == 0

    def test_delete_operation(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        assert self._count_tree_node(zkclient) == 1

        zkclient.create(self._path + "/foo/bar/baz", makepath=True)
        for _ in range(3):
            self._wait_cache(TreeEvent.NODE_ADDED)

        zkclient.delete(self._path + "/foo", recursive=True)
        for _ in range(3):
            self._wait_cache(TreeEvent.NODE_REMOVED)

        # tree should be empty
        assert self.cache._root._children == {}

        # watchers should be reset
        root_path = zkclient.chroot + self._path
        assert zkclient._data_watchers[root_path + "/foo"] == set()
        assert zkclient._data_watchers[root_path + "/foo/bar"] == set()
        assert zkclient._data_watchers[root_path + "/foo/bar/baz"] == set()
        assert zkclient._child_watchers[root_path + "/foo"] == set()
        assert zkclient._child_watchers[root_path + "/foo/bar"] == set()
        assert zkclient._child_watchers[root_path + "/foo/bar/baz"] == set()

        # should not be any leaked memory (tree node) here
        assert self._count_tree_node(zkclient) == 1

    def test_children_operation(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        zkclient.create(self._path + "/test_children", b"test_children_1")
        event = self._wait_cache(TreeEvent.NODE_ADDED)
        assert event is not None
        assert event.event_type == TreeEvent.NODE_ADDED
        assert event.event_data.path == self._path + "/test_children"
        assert event.event_data.data == b"test_children_1"
        assert event.event_data.stat.version == 0

        zkclient.set(self._path + "/test_children", b"test_children_2")
        event = self._wait_cache(TreeEvent.NODE_UPDATED)
        assert event is not None
        assert event.event_type == TreeEvent.NODE_UPDATED
        assert event.event_data.path == self._path + "/test_children"
        assert event.event_data.data == b"test_children_2"
        assert event.event_data.stat.version == 1

        zkclient.delete(self._path + "/test_children")
        event = self._wait_cache(TreeEvent.NODE_REMOVED)
        assert event is not None
        assert event.event_type == TreeEvent.NODE_REMOVED
        assert event.event_data.path == self._path + "/test_children"
        assert event.event_data.data == b"test_children_2"
        assert event.event_data.stat.version == 1

    def test_subtree_operation(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        zkclient.create(self._path + "/foo/bar/baz", makepath=True)
        for relative_path in ("/foo", "/foo/bar", "/foo/bar/baz"):
            event = self._wait_cache(TreeEvent.NODE_ADDED)
            assert event is not None
            assert event.event_type == TreeEvent.NODE_ADDED
            assert event.event_data.path == self._path + relative_path
            assert event.event_data.data == b""
            assert event.event_data.stat.version == 0

        zkclient.delete(self._path + "/foo", recursive=True)
        for relative_path in ("/foo/bar/baz", "/foo/bar", "/foo"):
            event = self._wait_cache(TreeEvent.NODE_REMOVED)
            assert event is not None
            assert event.event_type == TreeEvent.NODE_REMOVED
            assert event.event_data.path == self._path + relative_path

    def test_get_data(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)
        zkclient.create(self._path + "/foo/bar/baz", b"@", makepath=True)
        self._wait_cache(TreeEvent.NODE_ADDED)
        self._wait_cache(TreeEvent.NODE_ADDED)
        self._wait_cache(TreeEvent.NODE_ADDED)

        cache = self.cache
        with patch.object(cache, "_client"):  # disable any remote operation
            assert cache.get_data(self._path).data == b""
            assert cache.get_data(self._path).stat.version == 0

            assert cache.get_data(self._path + "/foo").data == b""
            assert cache.get_data(self._path + "/foo").stat.version == 0

            assert cache.get_data(self._path + "/foo/bar").data == b""
            assert cache.get_data(self._path + "/foo/bar").stat.version == 0

            assert cache.get_data(self._path + "/foo/bar/baz").data == b"@"
            assert (
                cache.get_data(self._path + "/foo/bar/baz").stat.version == 0
            )

    def test_get_children(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)
        zkclient.create(self._path + "/foo/bar/baz", b"@", makepath=True)
        self._wait_cache(TreeEvent.NODE_ADDED)
        self._wait_cache(TreeEvent.NODE_ADDED)
        self._wait_cache(TreeEvent.NODE_ADDED)

        cache = self.cache
        with patch.object(cache, "_client"):  # disable any remote operation
            assert (
                cache.get_children(self._path + "/foo/bar/baz") == frozenset()
            )
            assert cache.get_children(self._path + "/foo/bar") == frozenset(
                ["baz"]
            )
            assert cache.get_children(self._path + "/foo") == frozenset(
                ["bar"]
            )
            assert cache.get_children(self._path) == frozenset(["foo"])

    def test_get_data_out_of_tree(self):
        self._wait_cache(since=TreeEvent.INITIALIZED)
        with pytest.raises(ValueError):
            self.cache.get_data("/out_of_tree")

    def test_get_children_out_of_tree(self):
        self._wait_cache(since=TreeEvent.INITIALIZED)
        with pytest.raises(ValueError):
            self.cache.get_children("/out_of_tree")

    def test_get_data_no_node(self):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        cache = self.cache
        with patch.object(cache, "_client"):  # disable any remote operation
            assert cache.get_data(self._path + "/non_exists") is None

    def test_get_children_no_node(self):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        with patch.object(
            self.cache, "_client"
        ):  # disable any remote operation
            assert self.cache.get_children(self._path + "/non_exists") is None

    def test_session_reconnected(self, zkclient, zkensemble):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        zkclient.create(self._path + "/foo")
        event = self._wait_cache(TreeEvent.NODE_ADDED)
        assert event is not None
        assert event.event_data.path == self._path + "/foo"

        with (
            self._spy_client(zkclient, "get_async") as get_data,
            self._spy_client(zkclient, "get_children_async") as get_children,
        ):
            # session suspended
            zkensemble.lose_connection(zkclient)
            self._wait_cache(TreeEvent.CONNECTION_SUSPENDED)

            # There are a serial refreshing operation here. But NODE_ADDED
            # events will not be raised because the zxid of nodes are the
            # same during reconnecting.

            # connection restore
            self._wait_cache(TreeEvent.CONNECTION_RECONNECTED)

            # wait for outstanding operations
            while self.cache._outstanding_ops > 0:
                zkclient.handler.sleep_func(0.1)

            # inspect in-memory nodes
            _node_root = self.cache._root
            _node_foo = self.cache._root._children["foo"]

            # make sure that all nodes are refreshed
            get_data.assert_has_calls(
                [
                    call(self._path, watch=_node_root._process_watch),
                    call(self._path + "/foo", watch=_node_foo._process_watch),
                ],
                any_order=True,
            )
            get_children.assert_has_calls(
                [
                    call(self._path, watch=_node_root._process_watch),
                    call(self._path + "/foo", watch=_node_foo._process_watch),
                ],
                any_order=True,
            )

    def test_root_recreated(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        # remove root node
        zkclient.delete(self._path)
        event = self._wait_cache(TreeEvent.NODE_REMOVED)
        assert event is not None
        assert event.event_type == TreeEvent.NODE_REMOVED
        assert event.event_data.data == b""
        assert event.event_data.path == self._path
        assert event.event_data.stat.version == 0

        # re-create root node
        zkclient.ensure_path(self._path)
        event = self._wait_cache(TreeEvent.NODE_ADDED)
        assert event is not None
        assert event.event_type == TreeEvent.NODE_ADDED
        assert event.event_data.data == b""
        assert event.event_data.path == self._path
        assert event.event_data.stat.version == 0

        assert self.cache._outstanding_ops >= 0, (
            "unexpected outstanding ops %r" % self.cache._outstanding_ops
        )

    def test_exception_handler(self):
        error_value = FakeException()
        error_handler = Mock()

        with patch.object(TreeNode, "on_deleted") as on_deleted:
            on_deleted.side_effect = [error_value]
            self.cache.listen_fault(error_handler)
            self.cache.close()
            error_handler.assert_called_once_with(error_value)

    def test_exception_suppressed(self, zkclient):
        self._wait_cache(since=TreeEvent.INITIALIZED)

        # stoke up ConnectionClosedError
        zkclient.stop()
        zkclient.close()
        zkclient.handler.start()  # keep the async completion
        self._wait_cache(since=TreeEvent.CONNECTION_LOST)

        with patch.object(TreeNode, "on_created") as on_created:
            self.cache._root._call_client("exists", "/")
            self.cache._root._call_client("get", "/")
            self.cache._root._call_client("get_children", "/")

            self._wait_cache(since=TreeEvent.INITIALIZED)
            on_created.assert_not_called()
            assert self.cache._outstanding_ops == 0
