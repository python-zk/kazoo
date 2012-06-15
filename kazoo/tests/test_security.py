import unittest

from nose.tools import eq_
import zookeeper


class TestACL(unittest.TestCase):
    def _makeOne(self, *args, **kwargs):
        from kazoo.security import make_acl
        return make_acl(*args, **kwargs)

    def test_read_acl(self):
        acl = self._makeOne("digest", ":", read=True)
        eq_(acl['perms'] & zookeeper.PERM_READ, zookeeper.PERM_READ)

    def test_all_perms(self):
        acl = self._makeOne("digest", ":", write=True, create=True,
                            delete=True, admin=True)
        for perm in [zookeeper.PERM_WRITE, zookeeper.PERM_CREATE,
                     zookeeper.PERM_DELETE, zookeeper.PERM_ADMIN]:
            eq_(acl['perms'] & perm, perm)
