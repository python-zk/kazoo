import threading
import unittest

import mock
from nose.tools import eq_
from nose.tools import raises

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
        h.start()

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
        print
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
        assert isinstance(h.event_object(), threading._Event)

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


class TestThreadingAsync(unittest.TestCase):
    def _makeOne(self, *args):
        from kazoo.handlers.threading import AsyncResult
        return AsyncResult(*args)

    def _makeHandler(self):
        from kazoo.handlers.threading import SequentialThreadingHandler
        return SequentialThreadingHandler()

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

    def test_get_wait_while_setting(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        bv = threading.Event()
        cv = threading.Event()

        def wait_for_val():
            bv.set()
            val = async.get()
            lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        bv.wait()

        async.set('fred')
        cv.wait()
        eq_(lst, ['fred'])

    def test_get_with_nowait(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)
        timeout = self._makeHandler().timeout_exception

        @raises(timeout)
        def test_it():
            async.get(block=False)
        test_it()

        @raises(timeout)
        def test_nowait():
            async.get_nowait()
        test_nowait()

    def test_get_with_exception(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        bv = threading.Event()
        cv = threading.Event()

        def wait_for_val():
            bv.set()
            try:
                val = async.get()
            except ImportError:
                lst.append('oops')
            else:
                lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        bv.wait()

        async.set_exception(ImportError)
        cv.wait()
        eq_(lst, ['oops'])

    def test_set_before_wait(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        cv = threading.Event()
        async.set('fred')

        def wait_for_val():
            val = async.get()
            lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        cv.wait()
        eq_(lst, ['fred'])

    def test_set_exc_before_wait(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        cv = threading.Event()
        async.set_exception(ImportError)

        def wait_for_val():
            try:
                val = async.get()
            except ImportError:
                lst.append('ooops')
            else:
                lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        cv.wait()
        eq_(lst, ['ooops'])

    def test_linkage(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)
        cv = threading.Event()

        lst = []

        def add_on():
            lst.append(True)

        def wait_for_val():
            async.get()
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.start()

        async.rawlink(add_on)
        async.set('fred')
        assert mock_handler.completion_queue.put.called
        async.unlink(add_on)
        cv.wait()
        eq_(async.value, 'fred')

    def test_linkage_not_ready(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []

        def add_on():
            lst.append(True)

        async.set('fred')
        assert not mock_handler.completion_queue.called
        async.rawlink(add_on)
        assert mock_handler.completion_queue.put.called

    def test_link_and_unlink(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []

        def add_on():
            lst.append(True)

        async.rawlink(add_on)
        assert not mock_handler.completion_queue.put.called
        async.unlink(add_on)
        async.set('fred')
        assert not mock_handler.completion_queue.put.called
