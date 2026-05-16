"""Kazoo Security"""

from __future__ import annotations

from base64 import b64encode
import hashlib
from typing import NamedTuple


# Represents a Zookeeper ID and ACL object
class Id(NamedTuple):
    scheme: str
    id: str


class ACL(NamedTuple):
    """An ACL for a Zookeeper Node

    An ACL object is created by using an :class:`Id` object along with
    a :class:`Permissions` setting. For convenience,
    :meth:`make_digest_acl` should be used to create an ACL object with
    the desired scheme, id, and permissions.
    """

    perms: int
    id: Id

    @property
    def acl_list(self) -> list[str]:
        perms = []
        if self.perms & Permissions.ALL == Permissions.ALL:
            perms.append("ALL")
            return perms
        if self.perms & Permissions.READ == Permissions.READ:
            perms.append("READ")
        if self.perms & Permissions.WRITE == Permissions.WRITE:
            perms.append("WRITE")
        if self.perms & Permissions.CREATE == Permissions.CREATE:
            perms.append("CREATE")
        if self.perms & Permissions.DELETE == Permissions.DELETE:
            perms.append("DELETE")
        if self.perms & Permissions.ADMIN == Permissions.ADMIN:
            perms.append("ADMIN")
        return perms

    def __repr__(self) -> str:
        return "ACL(perms=%r, acl_list=%s, id=%r)" % (
            self.perms,
            self.acl_list,
            self.id,
        )


class Permissions(object):
    READ = 1
    WRITE = 2
    CREATE = 4
    DELETE = 8
    ADMIN = 16
    ALL = 31


# Shortcuts for common Ids
ANYONE_ID_UNSAFE = Id("world", "anyone")
AUTH_IDS = Id("auth", "")

# Shortcuts for common ACLs
OPEN_ACL_UNSAFE = [ACL(Permissions.ALL, ANYONE_ID_UNSAFE)]
CREATOR_ALL_ACL = [ACL(Permissions.ALL, AUTH_IDS)]
READ_ACL_UNSAFE = [ACL(Permissions.READ, ANYONE_ID_UNSAFE)]


def make_digest_acl_credential(username: str, password: str) -> str:
    """Create a SHA1 digest credential.

    .. note::

        This function uses UTF-8 to encode non-ASCII codepoints,
        whereas ZooKeeper uses the "default locale" for decoding.  It
        may be a good idea to start the JVM with `-Dfile.encoding=UTF-8`
        in non-UTF-8 locales.
        See: https://github.com/python-zk/kazoo/pull/584

    """
    credential = username.encode("utf-8") + b":" + password.encode("utf-8")
    cred_hash = b64encode(hashlib.sha1(credential).digest()).strip()
    return username + ":" + cred_hash.decode("utf-8")


def make_acl(
    scheme: str,
    credential: str,
    read: bool = False,
    write: bool = False,
    create: bool = False,
    delete: bool = False,
    admin: bool = False,
    all: bool = False,
) -> ACL:
    """Given a scheme and credential, return an :class:`ACL` object
    appropriate for use with Kazoo.

    :param scheme: The scheme to use. I.e. `digest`.
    :param credential:
        A colon separated username, password. The password should be
        hashed with the `scheme` specified. The
        :meth:`make_digest_acl_credential` method will create and
        return a credential appropriate for use with the `digest`
        scheme.
    :param write: Write permission.
    :type write: bool
    :param create: Create permission.
    :type create: bool
    :param delete: Delete permission.
    :type delete: bool
    :param admin: Admin permission.
    :type admin: bool
    :param all: All permissions.
    :type all: bool

    :rtype: :class:`ACL`

    """
    if all:
        permissions = Permissions.ALL
    else:
        permissions = 0
        if read:
            permissions |= Permissions.READ
        if write:
            permissions |= Permissions.WRITE
        if create:
            permissions |= Permissions.CREATE
        if delete:
            permissions |= Permissions.DELETE
        if admin:
            permissions |= Permissions.ADMIN
    return ACL(permissions, Id(scheme, credential))


def make_digest_acl(
    username: str,
    password: str,
    read: bool = False,
    write: bool = False,
    create: bool = False,
    delete: bool = False,
    admin: bool = False,
    all: bool = False,
) -> ACL:
    """Create a digest ACL for Zookeeper with the given permissions

    This method combines :meth:`make_digest_acl_credential` and
    :meth:`make_acl` to create an :class:`ACL` object appropriate for
    use with Kazoo's ACL methods.

    :param username: Username to use for the ACL.
    :param password: A plain-text password to hash.
    :param write: Write permission.
    :type write: bool
    :param create: Create permission.
    :type create: bool
    :param delete: Delete permission.
    :type delete: bool
    :param admin: Admin permission.
    :type admin: bool
    :param all: All permissions.
    :type all: bool

    :rtype: :class:`ACL`

    """
    cred = make_digest_acl_credential(username, password)
    return make_acl(
        "digest",
        cred,
        read=read,
        write=write,
        create=create,
        delete=delete,
        admin=admin,
        all=all,
    )
