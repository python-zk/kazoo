"""Handler utilities for getting non-monkey patched standard library stuff.

Allows one to get an unpatched thread module, with a thread
decorator that uses the unpatching OS thread.

"""
from __future__ import absolute_import

try:
    from gevent._threading import start_new_thread
except ImportError:
    from thread import start_new_thread


def thread(func):
    """Thread decorator

    Takes a function and spawns it as a daemon thread using the
    real OS thread regardless of monkey patching.

    """
    start_new_thread(func, ())
