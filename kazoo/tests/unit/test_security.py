from __future__ import annotations

import unittest

from typing import Any

from kazoo.security import ACL, Permissions


class TestACL(unittest.TestCase):
    def _makeOne(self, *args: Any, **kwargs: Any) -> ACL:
        from kazoo.security import make_acl

        return make_acl(*args, **kwargs)

    def test_read_acl(self) -> None:
        acl = self._makeOne("digest", ":", read=True)
        assert (acl.perms & Permissions.READ) == Permissions.READ

    def test_all_perms(self) -> None:
        acl = self._makeOne(
            "digest",
            ":",
            read=True,
            write=True,
            create=True,
            delete=True,
            admin=True,
        )
        for perm in [
            Permissions.READ,
            Permissions.CREATE,
            Permissions.WRITE,
            Permissions.DELETE,
            Permissions.ADMIN,
        ]:
            assert (acl.perms & perm) == perm

    def test_perm_listing(self) -> None:
        # FIXME ACL(n, Id) isn't an API, so why do we do this?

        from kazoo.security import Id

        f = ACL(15, Id("fred", "bill"))
        assert "READ" in f.acl_list
        assert "WRITE" in f.acl_list
        assert "CREATE" in f.acl_list
        assert "DELETE" in f.acl_list

        f = ACL(16, Id("fred", "bill"))
        assert "ADMIN" in f.acl_list

        f = ACL(31, Id("fred", "bill"))
        assert "ALL" in f.acl_list

    def test_perm_repr(self) -> None:
        # FIXME See above
        from kazoo.security import Id

        f = ACL(16, Id("fred", "bill"))
        assert "ACL(perms=16, acl_list=['ADMIN']" in repr(f)
