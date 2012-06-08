import threading
import unittest

import mock
from nose.tools import eq_

from kazoo.client import Callback


class TestThreadingHandler(unittest.TestCase):
    def _makeOne(self, *args):
        from kazoo.handlers.threading import SequentialThreadingHandler
        return SequentialThreadingHandler(*args)

    def _getAsync(self, *args):
        from kazoo.handlers.threading import AsyncResult
        return AsyncResult

    def test_completion_vs_session(self):
        h = self._makeOne()

        lst = []
        av = threading.Event()
        bv = threading.Event()
        cv = threading.Event()
        dv = threading.Event()

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
        assert isinstance(h.event_object(), threading._Event)

    def test_matching_async(self):
        h = self._makeOne()
        async = self._getAsync()
        assert isinstance(h.async_result(), async)


class TestThreadingAsync(unittest.TestCase):
    def _makeOne(self, *args):
        from kazoo.handlers.threading import AsyncResult
        return AsyncResult(*args)

    def test_ready(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        eq_(async.ready(), False)
        async.set('val')
        eq_(async.ready(), True)
        eq_(async.successful(), True)
        eq_(async.exception, None)

    def test_callback_queued(self):
        mock_handler = mock.Mock()
        mock_handler.completion_queue = mock.Mock()
        async = self._makeOne(mock_handler)

        async.rawlink(lambda a: a)
        async.set('val')

        assert mock_handler.completion_queue.put.called

    def test_set_exception(self):
        mock_handler = mock.Mock()
        mock_handler.completion_queue = mock.Mock()
        async = self._makeOne(mock_handler)
        async.rawlink(lambda a: a)
        async.set_exception(ImportError('Error occured'))

        assert isinstance(async.exception, ImportError)
        assert mock_handler.completion_queue.put.called
