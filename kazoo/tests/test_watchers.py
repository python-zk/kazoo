import threading
import uuid

from nose.tools import eq_

from kazoo.testing import KazooTestCase


class KazooWatcherTests(KazooTestCase):
    def setUp(self):
        super(KazooWatcherTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_child_watcher(self):
        self.client.ensure_path(self.path)
        update = threading.Event()
        all_children = ['fred']

        @self.client.children(self.path)
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

        @self.client.children(self.path)
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

        @self.client.children(self.path)
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

        @self.client.children(self.path, allow_session_lost=False)
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
        update.wait(0.5)
        eq_(update.is_set(), False)
        eq_(all_children, ['smith'])

        children = self.client.get_children(self.path)
        eq_(sorted(children), ['george', 'smith'])
