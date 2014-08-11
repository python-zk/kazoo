"""Kazoo handler helpers"""

HAS_FNCTL = True
try:
    import fcntl
except ImportError:  # pragma: nocover
    HAS_FNCTL = False
import functools
import os


def _set_fd_cloexec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


def _set_default_tcpsock_options(module, sock):
    sock.setsockopt(module.IPPROTO_TCP, module.TCP_NODELAY, 1)
    if HAS_FNCTL:
        _set_fd_cloexec(sock)
    return sock


def create_pipe():
    """Create a non-blocking read/write pipe.
    """
    r, w = os.pipe()
    if HAS_FNCTL:
        fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(w, fcntl.F_SETFL, os.O_NONBLOCK)
        _set_fd_cloexec(r)
        _set_fd_cloexec(w)
    return r, w

def create_socketpair(handler, module):
    """Create socketpair with O_NONBLOCK
    """
    if hasattr(module, "socketpair"):
        r, w = module.socketpair()
    else:
        listener = module.socket(module.AF_INET, module.SOCK_STREAM)
        listener.setsockopt(module.SOL_SOCKET, module.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1",0))
        listener.listen(1)
        ar = handler.async_result()
        handler.spawn(lambda:ar.set(listener.accept()[0]))
        w = module.socket(module.AF_INET, module.SOCK_STREAM)
        w.connect(listener.getsockname())
        listener.close()
        r = ar.get()
    r.setblocking(0)
    w.setblocking(0)
    return r, w

def create_tcp_socket(module):
    """Create a TCP socket with the CLOEXEC flag set.
    """
    type_ = module.SOCK_STREAM
    if hasattr(module, 'SOCK_CLOEXEC'):  # pragma: nocover
        # if available, set cloexec flag during socket creation
        type_ |= module.SOCK_CLOEXEC
    sock = module.socket(module.AF_INET, type_)
    _set_default_tcpsock_options(module, sock)
    return sock


def create_tcp_connection(module, address, timeout=None):
    if timeout is None:
        # thanks to create_connection() developers for
        # this ugliness...
        timeout = module._GLOBAL_DEFAULT_TIMEOUT

    sock = module.create_connection(address, timeout)
    _set_default_tcpsock_options(module, sock)
    return sock


def capture_exceptions(async_result):
    """Return a new decorated function that propagates the exceptions of the
    wrapped function to an async_result.

    :param async_result: An async result implementing :class:`IAsyncResult`

    """
    def capture(function):
        @functools.wraps(function)
        def captured_function(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except Exception as exc:
                async_result.set_exception(exc)
        return captured_function
    return capture


def wrap(async_result):
    """Return a new decorated function that propagates the return value or
    exception of wrapped function to an async_result.  NOTE: Only propagates a
    non-None return value.

    :param async_result: An async result implementing :class:`IAsyncResult`

    """
    def capture(function):
        @capture_exceptions(async_result)
        def captured_function(*args, **kwargs):
            value = function(*args, **kwargs)
            if value is not None:
                async_result.set(value)
            return value
        return captured_function
    return capture
