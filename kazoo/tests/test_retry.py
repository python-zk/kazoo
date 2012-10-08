import unittest

from nose.tools import eq_
from nose.tools import raises


class TestRetrySleeper(unittest.TestCase):

    def _makeOne(self, *args, **kwargs):
        from kazoo.retry import RetrySleeper
        return RetrySleeper(*args, **kwargs)

    def test_reset(self):
        retry = self._makeOne(delay=0)
        retry.increment()
        eq_(retry._attempts, 1)
        retry.reset()
        eq_(retry._attempts, 0)

    def test_too_many_tries(self):
        retry = self._makeOne(delay=0)
        retry.increment()

        @raises(Exception)
        def testit():
            retry.increment()
        testit()

    def test_maximum_delay(self):
        def sleep_func(time):
            pass

        retry = self._makeOne(delay=10, max_tries=100, sleep_func=sleep_func)
        for i in range(10):
            retry.increment()
        self.assertTrue(retry._cur_delay < 4000, retry._cur_delay)
        # gevent's sleep function is picky about the type
        eq_(type(retry._cur_delay), float)


class TestKazooRetry(unittest.TestCase):

    def _makeOne(self, **kw):
        from kazoo.retry import KazooRetry
        return KazooRetry(**kw)

    def test_connection_closed(self):
        from kazoo.exceptions import ConnectionClosedError
        retry = self._makeOne()

        def testit():
            raise ConnectionClosedError()
        self.assertRaises(ConnectionClosedError, retry, testit)

    def test_session_expired(self):
        from kazoo.exceptions import SessionExpiredError
        retry = self._makeOne(max_tries=1)

        def testit():
            raise SessionExpiredError()
        self.assertRaises(Exception, retry, testit)
