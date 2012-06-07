"""Handler utilities for getting non-monkey patched std lib stuff

Allows one to get an unpatched thread module, with a thread
decorator that uses the unpatching OS thread.

"""
try:
    from gevent import monkey
    [start_new_thread] = monkey.get_original('thread', ['start_new_thread'])
except ImportError:
    from thread import start_new_thread


def thread(func):
    """Thread decorator

    Takes a function and spawns it as a daemon thread using the
    real OS thread regardless of monkey patching.

    """
    start_new_thread(func, ())
