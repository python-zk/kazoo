"""Same as atexit except it supports unregister"""
__all__ = ["register"]

import sys

_exithandlers = []
def _run_exitfuncs():
    """run any registered exit functions

    _exithandlers is traversed in reverse order so functions are executed
    last in, first out.
    """

    exc_info = None
    while _exithandlers:
        func, targs, kargs = _exithandlers.pop()
        try:
            func(*targs, **kargs)
        except SystemExit:
            exc_info = sys.exc_info()
        except:
            import traceback
            print >> sys.stderr, "Error in atexit._run_exitfuncs:"
            traceback.print_exc()
            exc_info = sys.exc_info()

    if exc_info is not None:
        raise exc_info[0], exc_info[1], exc_info[2]


def register(func, *targs, **kargs):
    """register a function to be executed upon normal program termination

    func - function to be called at exit
    targs - optional arguments to pass to func
    kargs - optional keyword arguments to pass to func

    func is returned to facilitate usage as a decorator.
    """
    _exithandlers.append((func, targs, kargs))
    return func

def unregister(func):
    """remove func from the list of functions that are registered
    doesn't do anything if func is not found

    func = function to be unregistered
    """
    handler_entries = [e for e in _exithandlers if e[0] == func]
    for e in handler_entries:
        _exithandlers.remove(e)

if hasattr(sys, "exitfunc"):
    # Assume it's another registered exit function - append it to our list
    register(sys.exitfunc)
sys.exitfunc = _run_exitfuncs
