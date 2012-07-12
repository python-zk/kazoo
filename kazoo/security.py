"""Kazoo Security

This modules includes helper functions to create digest ACL's appropriate for
use with Zookeeper.

"""
import hashlib

import zookeeper


__all__ = ("make_digest_acl", )


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
        permissions = ACLPermission.ALL
    else:
        permissions = 0
        if read:
            permissions |= ACLPermission.READ
        if write:
            permissions |= ACLPermission.WRITE
        if create:
            permissions |= ACLPermission.CREATE
        if delete:
            permissions |= ACLPermission.DELETE
        if admin:
            permissions |= ACLPermission.ADMIN

    return dict(scheme=scheme, id=credential, perms=permissions)


def make_digest_acl(username, password, read=False, write=False,
                    create=False, delete=False, admin=False, all=False):
    """Create a digest ACL for Zookeeper with the given permissions"""
    cred = make_digest_acl_credential(username, password)
    return make_acl("digest", cred, read=read, write=write, create=create,
        delete=delete, admin=admin, all=all)


class ACLPermission(object):
    """ACL Permission object"""
    READ = zookeeper.PERM_READ
    WRITE = zookeeper.PERM_WRITE
    CREATE = zookeeper.PERM_CREATE
    DELETE = zookeeper.PERM_DELETE
    ADMIN = zookeeper.PERM_ADMIN
    ALL = zookeeper.PERM_ALL
