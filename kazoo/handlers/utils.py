"""Kazoo handler helpers"""

HAS_FNCTL = True
try:
    import fcntl
except ImportError:  # pragma: nocover
    HAS_FNCTL = False
import functools
import os

# sentinel objects
_NONE = object()


class AsyncResult(object):
    """A one-time event that stores a value or an exception"""
    def __init__(self, handler, condition_cls):
        self._handler = handler
        self.value = None
        self._exception = _NONE
        self._condition = condition_cls()
        self._callbacks = []

    def ready(self):
        """Return true if and only if it holds a value or an
        exception"""
        return self._exception is not _NONE

    def successful(self):
        """Return true if and only if it is ready and holds a value"""
        return self._exception is None

    @property
    def exception(self):
        if self._exception is not _NONE:
            return self._exception

    def set(self, value=None):
        """Store the value. Wake up the waiters."""
        with self._condition:
            self.value = value
            self._exception = None

            for callback in self._callbacks:
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
            self._condition.notify_all()

    def set_exception(self, exception):
        """Store the exception. Wake up the waiters."""
        with self._condition:
            self._exception = exception

            for callback in self._callbacks:
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
            self._condition.notify_all()

    def get(self, block=True, timeout=None):
        """Return the stored value or raise the exception.

        If there is no value raises TimeoutError.

        """
        with self._condition:
            if self._exception is not _NONE:
                if self._exception is None:
                    return self.value
                raise self._exception
            elif block:
                self._condition.wait(timeout)
                if self._exception is not _NONE:
                    if self._exception is None:
                        return self.value
                    raise self._exception

            # if we get to this point we timeout
            raise self._handler.timeout_exception()

    def get_nowait(self):
        """Return the value or raise the exception without blocking.

        If nothing is available, raises TimeoutError

        """
        return self.get(block=False)

    def wait(self, timeout=None):
        """Block until the instance is ready."""
        with self._condition:
            self._condition.wait(timeout)
        return self._exception is not _NONE

    def rawlink(self, callback):
        """Register a callback to call when a value or an exception is
        set"""
        with self._condition:
            # Are we already set? Dispatch it now
            if self.ready():
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
                return

            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def unlink(self, callback):
        """Remove the callback set by :meth:`rawlink`"""
        with self._condition:
            if self.ready():
                # Already triggered, ignore
                return

            if callback in self._callbacks:
                self._callbacks.remove(callback)


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
