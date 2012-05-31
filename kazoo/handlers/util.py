"""Handler utilities for getting non-monkey patched std lib stuff

Allows one to get an unpatched threading and thread module.

"""

_realthread = None
_realthreading = None


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


def get_realthreading():
    """Get the real Python thread module, regardless of any monkeypatching"""
    global _realthreading
    if _realthreading:
        return _realthreading

    import imp
    fp, pathname, description = imp.find_module('threading')
    try:
        threading = imp.load_module('realthreading', fp, pathname, description)
        thread = get_realthread()
        if threading._start_new_thread != thread.start_new_thread:
            # All monkey patched all over, straighten our copy out
            threading._start_new_thread = thread.start_new_thread
            threading._allocate_lock = thread.allocate_lock
            threading.Lock = thread.allocate_lock
            threading._get_ident = thread.get_ident
        return threading
    finally:
        if fp:
            fp.close()
