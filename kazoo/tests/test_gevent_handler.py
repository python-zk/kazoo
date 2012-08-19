import unittest

from gevent.event import Event
from nose.tools import eq_
from nose.tools import raises

from kazoo.client import Callback
from kazoo.testing import KazooTestCase


class TestGeventHandler(unittest.TestCase):
    def _makeOne(self, *args):
        from kazoo.handlers.gevent import SequentialGeventHandler
        return SequentialGeventHandler(*args)

    def _getAsync(self, *args):
        from kazoo.handlers.gevent import AsyncResult
        return AsyncResult

    def test_completion_vs_session(self):
        h = self._makeOne()
        h.start()

        lst = []
        av = Event()
        bv = Event()
        cv = Event()
        dv = Event()

        def addToList():
            av.set()
            bv.wait()
            lst.append(True)
            dv.set()

        def anotherAdd():
            lst.append(True)
            cv.set()

        call1 = Callback('session', addToList, ())
        call2 = Callback('completion', anotherAdd, ())

        h.dispatch_callback(call1)
        av.wait()
        # Now we know the first is waiting, make sure
        # the second executes while the first has blocked
        # its thread
        h.dispatch_callback(call2)
        cv.wait()

        eq_(lst, [True])
        bv.set()
        dv.wait()
        eq_(lst, [True, True])

    def test_proper_threading(self):
        h = self._makeOne()
        h.start()
        assert isinstance(h.event_object(), Event)

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


class TestGeventClient(KazooTestCase):
    def _makeOne(self, *args):
        from kazoo.handlers.gevent import SequentialGeventHandler
        return SequentialGeventHandler(*args)

    def test_start(self):
        client = self._get_client(handler=self._makeOne())
        client.start()
        assert client.state == 'CONNECTED'
        client.stop()
