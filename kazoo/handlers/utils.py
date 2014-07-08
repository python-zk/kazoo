"""Kazoo handler helpers"""

HAS_FNCTL = True
try:
    import fcntl
except ImportError:  # pragma: nocover
    HAS_FNCTL = False
import functools
import os
import socket
import random
import sys


def _set_fd_cloexec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


def _set_default_tcpsock_options(module, sock):
    sock.setsockopt(module.IPPROTO_TCP, module.TCP_NODELAY, 1)
    if HAS_FNCTL:
        _set_fd_cloexec(sock)
    return sock


def set_non_block(r, w):
    if HAS_FNCTL:
        fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(w, fcntl.F_SETFL, os.O_NONBLOCK)
        _set_fd_cloexec(r)
        _set_fd_cloexec(w)


def create_pipe():
    """Create a non-blocking read/write pipe.
    """
    if sys.platform == 'win32':
        return SocketHandle()
    return PipeHandle()


class PipeHandle(object):
    def __init__(self):
        self._read_pipe = self._write_pipe = None

    def open(self):
        self._read_pipe, self._write_pipe = os.pipe()
        set_non_block(self._read_pipe, self._write_pipe)

    def close(self):
        wp, rp = self._write_pipe, self._read_pipe
        self._write_pipe = self._read_pipe = None
        if wp is not None:
            os.close(wp)
        if rp is not None:
            os.close(rp)

    def write(self, data):
        return os.write(self._write_pipe, data)

    def read(self, size):
        return os.read(self._read_pipe, size)


class SocketHandle(object):
    BIND_INTERFACE = '127.0.0.1'
    BIND_RANGE_BEGIN = 30000
    BIND_RANGE_END = 40000
    SEED_LEN = 7

    def __init__(self):
        self._read_pipe = self._write_pipe = None
        self._port = 0

    def open(self):
        self._read_pipe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._read_pipe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        random.seed(os.urandom(self.SEED_LEN))
        while True:
            port = random.randrange(self.BIND_RANGE_BEGIN, self.BIND_RANGE_END)
            try:
                self._read_pipe.bind((self.BIND_INTERFACE, port))
                self._port = port
                break
            except socket.error:
                pass

        self._write_pipe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        set_non_block(self._read_pipe, self._write_pipe)

    def close(self):
        wp, rp = self._write_pipe, self._read_pipe
        self._write_pipe = self._read_pipe = None
        if wp is not None:
            wp.close()
        if rp is not None:
            rp.close()

    def write(self, data):
        return self._write_pipe.sendto(data, (self.BIND_INTERFACE, self._port))

    def read(self, size):
        return self._read_pipe.recv(size)


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
