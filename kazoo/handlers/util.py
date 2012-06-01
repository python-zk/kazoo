"""Handler utilities for getting non-monkey patched std lib stuff

Allows one to get an unpatched thread module, with a thread
decorator that using the unpatching OS thread.

"""
_realthread = None


def get_realthread():
    """Get the real Python thread module, regardless of any monkeypatching"""
    global _realthread
    if _realthread:
        return _realthread

    import imp
    fp, pathname, description = imp.find_module('thread')
    try:
        return imp.load_module('realthread', fp, pathname, description)
    finally:
        if fp:
            fp.close()


def thread(func):
    """Thread decorator

    Takes a function and spawns it as a daemon thread using the
    real OS thread regardless of monkey patching.

    """
    get_realthread().start_new_thread(func, ())
