"""Kazoo Security

"""
from collections import namedtuple
import hashlib


# Represents a Zookeeper ID and ACL object
Id = namedtuple('Id', 'scheme id')


class ACL(namedtuple('ACL', 'perms id')):
    @property
    def acl_list(self):
        perms = []
        if self.perms & Permissions.ALL == Permissions.ALL:
            perms.append('ALL')
            return perms
        if self.perms & Permissions.READ == Permissions.READ:
            perms.append('READ')
        if self.perms & Permissions.WRITE == Permissions.WRITE:
            perms.append('WRITE')
        if self.perms & Permissions.CREATE == Permissions.CREATE:
            perms.append('CREATE')
        if self.perms & Permissions.DELETE == Permissions.DELETE:
            perms.append('DELETE')
        if self.perms & Permissions.ADMIN == Permissions.ADMIN:
            perms.append('ADMIN')
        return perms

    def __repr__(self):
        return 'ACL(perms=%r, acl_list=%s, id=%r)' % (
            self.perms, self.acl_list, self.id)


class Permissions(object):
    READ = 1
    WRITE = 2
    CREATE = 4
    DELETE = 8
    ADMIN = 16
    ALL = 31


# Shortcuts for common Ids
ANYONE_ID_UNSAFE = Id('world', 'anyone')
AUTH_IDS = Id('world', 'anyone')

# Shortcuts for common ACLs
OPEN_ACL_UNSAFE = [ACL(Permissions.ALL, ANYONE_ID_UNSAFE)]
CREATOR_ALL_ACL = [ACL(Permissions.ALL, AUTH_IDS)]
READ_ACL_UNSAFE = [ACL(Permissions.READ, ANYONE_ID_UNSAFE)]


def make_digest_acl_credential(username, password):
    """Create a SHA1 digest credential"""
    credential = "%s:%s" % (username, password)
    cred_hash = hashlib.sha1(credential).digest().encode('base64').strip()
    return "%s:%s" % (username, cred_hash)


def make_acl(scheme, credential, read=False, write=False,
             create=False, delete=False, admin=False, all=False):
    """Given a scheme and credential, return an ACL dict appropriate for
    Zookeeper"""
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


def make_digest_acl(username, password, read=False, write=False,
                    create=False, delete=False, admin=False, all=False):
    """Create a digest ACL for Zookeeper with the given permissions"""
    cred = make_digest_acl_credential(username, password)
    return make_acl("digest", cred, read=read, write=write, create=create,
        delete=delete, admin=admin, all=all)
