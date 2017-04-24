import uuid

from mock import patch, call, Mock
from nose.tools import eq_, ok_, assert_not_equal, raises

from kazoo.testing import KazooTestCase
from kazoo.exceptions import KazooException
from kazoo.recipe.cache import TreeCache, TreeNode, TreeEvent


class KazooTreeCacheTests(KazooTestCase):

    def setUp(self):
        super(KazooTreeCacheTests, self).setUp()
        self._event_queue = self.client.handler.queue_impl()
        self._error_queue = self.client.handler.queue_impl()
        self.path = None
        self.cache = None

    def tearDown(self):
        super(KazooTreeCacheTests, self).tearDown()
        if not self._error_queue.empty():
            try:
                raise self._error_queue.get()
            except FakeException:
                pass

    def make_cache(self):
        if self.cache is None:
            self.path = '/' + uuid.uuid4().hex
            self.cache = TreeCache(self.client, self.path)
            self.cache.listen(lambda event: self._event_queue.put(event))
            self.cache.listen_fault(lambda error: self._error_queue.put(error))
            self.cache.start()
        return self.cache

    def wait_cache(self, expect=None, since=None, timeout=10):
        started = since is None
        while True:
            event = self._event_queue.get(timeout=timeout)
            if started:
                if expect is not None:
                    eq_(event.event_type, expect)
                return event
            if event.event_type == since:
                started = True
                if expect is None:
                    return

    def spy_client(self, method_name):
        method = getattr(self.client, method_name)
        return patch.object(self.client, method_name, wraps=method)

    def test_start(self):
        self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)

        stat = self.client.exists(self.path)
        eq_(stat.version, 0)

        eq_(self.cache._state, TreeCache.STATE_STARTED)
        eq_(self.cache._root._state, TreeNode.STATE_LIVE)

    @raises(KazooException)
    def test_start_started(self):
        self.make_cache()
        self.cache.start()

    @raises(KazooException)
    def test_start_closed(self):
        self.make_cache()
        self.cache.start()
        self.cache.close()
        self.cache.start()

    def test_close(self):
        self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)
        self.client.create(self.path + '/foo/bar/baz', makepath=True)
        for _ in range(3):
            self.wait_cache(TreeEvent.NODE_ADDED)

        self.cache.close()

        # nothing should be published since tree closed
        ok_(self._event_queue.empty())

        # tree should be empty
        eq_(self.cache._root._children, {})
        eq_(self.cache._root._data, None)
        eq_(self.cache._state, TreeCache.STATE_CLOSED)

        # node state should not be changed
        assert_not_equal(self.cache._root._state, TreeNode.STATE_DEAD)

    def test_children_operation(self):
        self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)

        self.client.create(self.path + '/test_children', b'test_children_1')
        event = self.wait_cache(TreeEvent.NODE_ADDED)
        eq_(event.event_type, TreeEvent.NODE_ADDED)
        eq_(event.event_data.path, self.path + '/test_children')
        eq_(event.event_data.data, b'test_children_1')
        eq_(event.event_data.stat.version, 0)

        self.client.set(self.path + '/test_children', b'test_children_2')
        event = self.wait_cache(TreeEvent.NODE_UPDATED)
        eq_(event.event_type, TreeEvent.NODE_UPDATED)
        eq_(event.event_data.path, self.path + '/test_children')
        eq_(event.event_data.data, b'test_children_2')
        eq_(event.event_data.stat.version, 1)

        self.client.delete(self.path + '/test_children')
        event = self.wait_cache(TreeEvent.NODE_REMOVED)
        eq_(event.event_type, TreeEvent.NODE_REMOVED)
        eq_(event.event_data.path, self.path + '/test_children')
        eq_(event.event_data.data, b'test_children_2')
        eq_(event.event_data.stat.version, 1)

    def test_subtree_operation(self):
        self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)

        self.client.create(self.path + '/foo/bar/baz', makepath=True)
        for relative_path in ('/foo', '/foo/bar', '/foo/bar/baz'):
            event = self.wait_cache(TreeEvent.NODE_ADDED)
            eq_(event.event_type, TreeEvent.NODE_ADDED)
            eq_(event.event_data.path, self.path + relative_path)
            eq_(event.event_data.data, b'')
            eq_(event.event_data.stat.version, 0)

        self.client.delete(self.path + '/foo', recursive=True)
        for relative_path in ('/foo/bar/baz', '/foo/bar', '/foo'):
            event = self.wait_cache(TreeEvent.NODE_REMOVED)
            eq_(event.event_type, TreeEvent.NODE_REMOVED)
            eq_(event.event_data.path, self.path + relative_path)

    def test_get_data(self):
        cache = self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)
        self.client.create(self.path + '/foo/bar/baz', b'@', makepath=True)
        self.wait_cache(TreeEvent.NODE_ADDED)
        self.wait_cache(TreeEvent.NODE_ADDED)
        self.wait_cache(TreeEvent.NODE_ADDED)

        with patch.object(cache, '_client'):  # disable any remote operation
            eq_(cache.get_data(self.path).data, b'')
            eq_(cache.get_data(self.path).stat.version, 0)

            eq_(cache.get_data(self.path + '/foo').data, b'')
            eq_(cache.get_data(self.path + '/foo').stat.version, 0)

            eq_(cache.get_data(self.path + '/foo/bar').data, b'')
            eq_(cache.get_data(self.path + '/foo/bar').stat.version, 0)

            eq_(cache.get_data(self.path + '/foo/bar/baz').data, b'@')
            eq_(cache.get_data(self.path + '/foo/bar/baz').stat.version, 0)

    def test_get_children(self):
        cache = self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)
        self.client.create(self.path + '/foo/bar/baz', b'@', makepath=True)
        self.wait_cache(TreeEvent.NODE_ADDED)
        self.wait_cache(TreeEvent.NODE_ADDED)
        self.wait_cache(TreeEvent.NODE_ADDED)

        with patch.object(cache, '_client'):  # disable any remote operation
            eq_(cache.get_children(self.path + '/foo/bar/baz'), frozenset())
            eq_(cache.get_children(self.path + '/foo/bar'), frozenset(['baz']))
            eq_(cache.get_children(self.path + '/foo'), frozenset(['bar']))
            eq_(cache.get_children(self.path), frozenset(['foo']))

    @raises(ValueError)
    def test_get_data_out_of_tree(self):
        self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)
        self.cache.get_data('/out_of_tree')

    @raises(ValueError)
    def test_get_children_out_of_tree(self):
        self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)
        self.cache.get_children('/out_of_tree')

    def test_get_data_no_node(self):
        cache = self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)

        with patch.object(cache, '_client'):  # disable any remote operation
            eq_(cache.get_data(self.path + '/non_exists'), None)

    def test_get_children_no_node(self):
        cache = self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)

        with patch.object(cache, '_client'):  # disable any remote operation
            eq_(cache.get_children(self.path + '/non_exists'), None)

    def test_session_reconnected(self):
        self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)

        self.client.create(self.path + '/foo')
        event = self.wait_cache(TreeEvent.NODE_ADDED)
        eq_(event.event_data.path, self.path + '/foo')

        with self.spy_client('get_async') as get_data:
            with self.spy_client('get_children_async') as get_children:
                # session suspended
                self.lose_connection(self.client.handler.event_object)
                self.wait_cache(TreeEvent.CONNECTION_SUSPENDED)

                # There are a serial refreshing operation here. But NODE_ADDED
                # events will not be raised because the zxid of nodes are the
                # same during reconnecting.

                # connection restore
                self.wait_cache(TreeEvent.CONNECTION_RECONNECTED)

                # wait for outstanding operations
                while self.cache._outstanding_ops > 0:
                    self.client.handler.sleep_func(0.1)

                # inspect in-memory nodes
                _node_root = self.cache._root
                _node_foo = self.cache._root._children['foo']

                # make sure that all nodes are refreshed
                get_data.assert_has_calls([
                    call(self.path, watch=_node_root._process_watch),
                    call(self.path + '/foo', watch=_node_foo._process_watch),
                ], any_order=True)
                get_children.assert_has_calls([
                    call(self.path, watch=_node_root._process_watch),
                    call(self.path + '/foo', watch=_node_foo._process_watch),
                ], any_order=True)

    def test_root_recreated(self):
        self.make_cache()
        self.wait_cache(since=TreeEvent.INITIALIZED)

        # remove root node
        self.client.delete(self.path)
        event = self.wait_cache(TreeEvent.NODE_REMOVED)
        eq_(event.event_type, TreeEvent.NODE_REMOVED)
        eq_(event.event_data.data, b'')
        eq_(event.event_data.path, self.path)
        eq_(event.event_data.stat.version, 0)

        # re-create root node
        self.client.ensure_path(self.path)
        event = self.wait_cache(TreeEvent.NODE_ADDED)
        eq_(event.event_type, TreeEvent.NODE_ADDED)
        eq_(event.event_data.data, b'')
        eq_(event.event_data.path, self.path)
        eq_(event.event_data.stat.version, 0)

        self.assertTrue(
            self.cache._outstanding_ops >= 0,
            'unexpected outstanding ops %r' % self.cache._outstanding_ops)

    def test_exception_handler(self):
        error_value = FakeException()
        error_handler = Mock()

        with patch.object(TreeNode, 'on_deleted') as on_deleted:
            on_deleted.side_effect = [error_value]

            self.make_cache()
            self.cache.listen_fault(error_handler)

            self.cache.close()
            error_handler.assert_called_once_with(error_value)


class FakeException(Exception):
    pass
