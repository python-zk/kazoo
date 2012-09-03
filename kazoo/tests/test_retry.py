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


class TestKazooRetry(unittest.TestCase):

    def _makeOne(self):
        from kazoo.retry import KazooRetry
        return KazooRetry()

    def test_connection_closed(self):
        from kazoo.exceptions import ConnectionClosedError
        retry = self._makeOne()

        def testit():
            raise ConnectionClosedError()
        self.assertRaises(ConnectionClosedError, retry, testit)
