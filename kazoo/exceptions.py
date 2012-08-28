"""Kaoo Exceptions"""
from collections import defaultdict


class KazooException(Exception):
    """Base Kazoo exception that all other kazoo library exceptions inherit
    from"""


class CancelledError(KazooException):
    """Raised when a process is cancelled by another thread"""


class ConfigurationError(KazooException):
    """Raised if the configuration arguments to an object are invalid"""


class ZookeeperStoppedError(KazooException):
    """Raised when the kazoo client stopped (and thus not connected)"""


class ConnectionDropped(KazooException):
    """ Internal error for jumping out of loops """


class AuthFailedError(KazooException):
    """Authentication failed when connected"""


def _invalid_error_code():
    raise RuntimeError('Invalid error code')


EXCEPTIONS = defaultdict(_invalid_error_code)
