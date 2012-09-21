import unittest

from nose import SkipTest
from nose.tools import eq_
from nose.tools import raises

from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import Callback
from kazoo.testing import KazooTestCase


class TestGeventHandler(unittest.TestCase):

    def setUp(self):
        try:
            import gevent
        except ImportError:
            raise SkipTest('gevent not available.')

    def _makeOne(self, *args):
        from kazoo.handlers.gevent import SequentialGeventHandler
        return SequentialGeventHandler(*args)

    def _getAsync(self, *args):
        from kazoo.handlers.gevent import AsyncResult
        return AsyncResult

    def _getEvent(self):
        from gevent.event import Event
        return Event

    def test_proper_threading(self):
        h = self._makeOne()
        h.start()
        assert isinstance(h.event_object(), self._getEvent())

    def test_matching_async(self):
        h = self._makeOne()
        h.start()
        async = self._getAsync()
        assert isinstance(h.async_result(), async)

    def test_exception_raising(self):
        h = self._makeOne()

        @raises(h.timeout_exception)
        def testit():
            raise h.timeout_exception("This is a timeout")
        testit()

    def test_exception_in_queue(self):
        h = self._makeOne()
        h.start()
        ev = self._getEvent()()

        def func():
            ev.set()
            raise ValueError('bang')

        call1 = Callback('completion', func, ())
        h.dispatch_callback(call1)
        ev.wait()

    def test_queue_empty_exception(self):
        from gevent.queue import Empty
        h = self._makeOne()
        h.start()
        ev = self._getEvent()()

        def func():
            ev.set()
            raise Empty()

        call1 = Callback('completion', func, ())
        h.dispatch_callback(call1)
        ev.wait()

    def test_peeking(self):
        from kazoo.handlers.gevent import _PeekableQueue
        from kazoo.handlers.gevent import Empty
        queue = _PeekableQueue()
        queue.put('fred')
        eq_(queue.peek(), 'fred')
        eq_(queue.get(), 'fred')

        @raises(Empty)
        def testit():
            queue.peek(block=False)
        testit()

    def test_peekable_queue_switching(self):
        import gevent
        h = self._makeOne()
        queue = h.peekable_queue()

        def getter():
            peeked = queue.peek(True, timeout=5)
            got = queue.get()
            eq_(peeked, got)
            return got

        # try to make sure getter is running before putting
        getter_thread = gevent.spawn(getter)
        gevent.spawn_later(0, queue.put, 'fred')

        eq_(getter_thread.get(), 'fred')

        # repeat the process to make sure the queue signalling mechanism
        # is reset correctly
        getter_thread = gevent.spawn(getter)
        gevent.spawn_later(0, queue.put, 'fred')

        eq_(getter_thread.get(), 'fred')


class TestGeventClient(KazooTestCase):

    def setUp(self):
        KazooTestCase.setUp(self)
        try:
            import gevent
        except ImportError:
            raise SkipTest('gevent not available.')

    def _makeOne(self, *args):
        from kazoo.handlers.gevent import SequentialGeventHandler
        return SequentialGeventHandler(*args)

    def _getEvent(self):
        from gevent.event import Event
        return Event

    def test_start(self):
        client = self._get_client(handler=self._makeOne())
        client.start()
        self.assertEqual(client.state, 'CONNECTED')
        client.stop()

    def test_start_stop_double(self):
        client = self._get_client(handler=self._makeOne())
        client.start()
        self.assertEqual(client.state, 'CONNECTED')
        client.handler.start()
        client.handler.stop()
        client.stop()

    def test_basic_commands(self):
        client = self._get_client(handler=self._makeOne())
        client.start()
        self.assertEqual(client.state, 'CONNECTED')
        client.create('/anode', 'fred')
        eq_(client.get('/anode')[0], 'fred')
        eq_(client.delete('/anode'), True)
        eq_(client.exists('/anode'), None)
        client.stop()

    def test_failures(self):
        client = self._get_client(handler=self._makeOne())
        client.start()
        self.assertRaises(NoNodeError, client.get, '/none')
        client.stop()

    def test_data_watcher(self):
        client = self._get_client(handler=self._makeOne())
        client.start()
        client.ensure_path('/some/node')
        ev = self._getEvent()()

        @client.DataWatch('/some/node')
        def changed(d, stat):
            ev.set()

        ev.wait()
        ev.clear()
        client.set('/some/node', 'newvalue')
        ev.wait()
        client.stop()
