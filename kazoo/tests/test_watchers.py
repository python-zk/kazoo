import time
import threading
import uuid

from nose.tools import eq_
from nose.tools import raises

from kazoo.testing import KazooTestCase


class KazooDataWatcherTests(KazooTestCase):
    def setUp(self):
        super(KazooDataWatcherTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_child_watcher(self):
        self.client.ensure_path(self.path)
        update = threading.Event()
        data = [True]

        @self.client.DataWatch(self.path)
        def changed(d, stat):
            data.pop()
            data.append(d)
            update.set()

        update.wait()
        eq_(data, [""])
        update.clear()

        self.client.set(self.path, 'fred')
        update.wait()
        eq_(data[0], 'fred')
        update.clear()

    def test_func_stops(self):
        self.client.ensure_path(self.path)
        update = threading.Event()
        data = [True]

        fail_through = []

        @self.client.DataWatch(self.path)
        def changed(d, stat):
            data.pop()
            data.append(d)
            update.set()
            if fail_through:
                return False

        update.wait()
        eq_(data, [""])
        update.clear()

        fail_through.append(True)
        self.client.set(self.path, 'fred')
        update.wait()
        eq_(data[0], 'fred')
        update.clear()

        self.client.set(self.path, 'asdfasdf')
        update.wait(0.5)
        eq_(data[0], 'fred')

        d, stat = self.client.get(self.path)
        eq_(d, 'asdfasdf')

    def test_no_such_node(self):
        args = []

        @self.client.DataWatch("/some/path")
        def changed(d, stat):
            args.extend([d, stat])

        eq_(args, [None, None])


class KazooChildrenWatcherTests(KazooTestCase):
    def setUp(self):
        super(KazooChildrenWatcherTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_child_watcher(self):
        self.client.ensure_path(self.path)
        update = threading.Event()
        all_children = ['fred']

        @self.client.ChildrenWatch(self.path)
        def changed(children):
            while all_children:
                all_children.pop()
            all_children.extend(children)
            update.set()

        update.wait()
        eq_(all_children, [])
        update.clear()

        self.client.create(self.path + '/' + 'smith', '0')
        update.wait()
        eq_(all_children, ['smith'])
        update.clear()

        self.client.create(self.path + '/' + 'george', '0')
        update.wait()
        eq_(sorted(all_children), ['george', 'smith'])

    def test_func_stops(self):
        self.client.ensure_path(self.path)
        update = threading.Event()
        all_children = ['fred']

        fail_through = []

        @self.client.ChildrenWatch(self.path)
        def changed(children):
            while all_children:
                all_children.pop()
            all_children.extend(children)
            update.set()
            if fail_through:
                return False

        update.wait()
        eq_(all_children, [])
        update.clear()

        fail_through.append(True)
        self.client.create(self.path + '/' + 'smith', '0')
        update.wait()
        eq_(all_children, ['smith'])
        update.clear()

        self.client.create(self.path + '/' + 'george', '0')
        update.wait(0.5)
        eq_(all_children, ['smith'])

    def test_child_watch_session_loss(self):
        self.client.ensure_path(self.path)
        update = threading.Event()
        all_children = ['fred']

        @self.client.ChildrenWatch(self.path)
        def changed(children):
            while all_children:
                all_children.pop()
            all_children.extend(children)
            update.set()

        update.wait()
        eq_(all_children, [])
        update.clear()

        self.client.create(self.path + '/' + 'smith', '0')
        update.wait()
        eq_(all_children, ['smith'])
        update.clear()
        self.expire_session()

        eq_(update.is_set(), False)

        self.client.retry(self.client.create,
                          self.path + '/' + 'george', '0')
        update.wait()
        eq_(sorted(all_children), ['george', 'smith'])

    def test_child_stop_on_session_loss(self):
        self.client.ensure_path(self.path)
        update = threading.Event()
        all_children = ['fred']

        @self.client.ChildrenWatch(self.path, allow_session_lost=False)
        def changed(children):
            while all_children:
                all_children.pop()
            all_children.extend(children)
            update.set()

        update.wait()
        eq_(all_children, [])
        update.clear()

        self.client.create(self.path + '/' + 'smith', '0')
        update.wait()
        eq_(all_children, ['smith'])
        update.clear()
        self.expire_session()

        eq_(update.is_set(), False)

        self.client.retry(self.client.create,
                          self.path + '/' + 'george', '0')
        update.wait(3)
        eq_(update.is_set(), False)
        eq_(all_children, ['smith'])

        children = self.client.get_children(self.path)
        eq_(sorted(children), ['george', 'smith'])


class KazooPatientChildrenWatcherTests(KazooTestCase):
    def setUp(self):
        super(KazooPatientChildrenWatcherTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def _makeOne(self, *args, **kwargs):
        from kazoo.recipe.partitioner import PatientChildrenWatch
        return PatientChildrenWatch(*args, **kwargs)

    def test_watch(self):
        self.client.ensure_path(self.path)
        watcher = self._makeOne(self.client, self.path, 0.1)
        result = watcher.start()
        children, asy = result.get()
        eq_(len(children), 0)
        eq_(asy.ready(), False)

        self.client.create(self.path + '/' + 'fred', '0')
        asy.get(timeout=1)
        eq_(asy.ready(), True)

    def test_exception(self):
        from kazoo.exceptions import NoNodeException
        watcher = self._makeOne(self.client, self.path, 0.1)
        result = watcher.start()

        @raises(NoNodeException)
        def testit():
            result.get()
        testit()

    def test_watch_iterations(self):
        self.client.ensure_path(self.path)
        watcher = self._makeOne(self.client, self.path, 0.2)
        result = watcher.start()
        eq_(result.ready(), False)

        time.sleep(0.1)
        self.client.create(self.path + '/' + uuid.uuid4().hex, '0')
        eq_(result.ready(), False)
        time.sleep(0.1)
        eq_(result.ready(), False)
        self.client.create(self.path + '/' + uuid.uuid4().hex, '0')
        time.sleep(0.1)
        eq_(result.ready(), False)

        children, asy = result.get()
        eq_(len(children), 2)
