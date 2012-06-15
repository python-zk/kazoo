import unittest

from nose.tools import eq_
import zookeeper


class TestExceptions(unittest.TestCase):
    def _makeOne(self, *args, **kwargs):
        from kazoo.exceptions import err_to_exception
        return err_to_exception(*args, **kwargs)

    def test_error_translate(self):
        exc = self._makeOne(zookeeper.SYSTEMERROR)
        assert isinstance(exc, zookeeper.SystemErrorException)

    def test_error_with_message(self):
        exc = self._makeOne(zookeeper.NONODE, msg="oops")
        assert isinstance(exc, zookeeper.NoNodeException)
        eq_(str(exc), "no node: oops")

    def test_generic_error_code(self):
        exc = self._makeOne(-200)
        assert isinstance(exc, Exception)

    def test_zookeeper_ok(self):
        exc = self._makeOne(zookeeper.OK)
        eq_(exc, None)

    def test_not_error_code(self):
        exc = self._makeOne("this needs to be an int")
        assert isinstance(exc, Exception)
