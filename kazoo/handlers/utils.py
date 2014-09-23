"""Kazoo handler helpers"""

HAS_FNCTL = True
try:
    import fcntl
except ImportError:  # pragma: nocover
    HAS_FNCTL = False
import functools
import os
import sys
import socket
import errno

def _set_fd_cloexec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


def _set_default_tcpsock_options(module, sock):
    sock.setsockopt(module.IPPROTO_TCP, module.TCP_NODELAY, 1)
    if HAS_FNCTL:
        _set_fd_cloexec(sock)
    return sock

def create_socket_pair(port=0):
    """Create socket pair.

    If socket.socketpair isn't available, we emulate it.
    """
    # See if socketpair() is available.
    have_socketpair = hasattr(socket, 'socketpair')
    if have_socketpair:
        client_sock, srv_sock = socket.socketpair()
        return client_sock, srv_sock

    # Create a non-blocking temporary server socket
    temp_srv_sock = socket.socket()
    temp_srv_sock.setblocking(False)
    temp_srv_sock.bind(('', port))
    port = temp_srv_sock.getsockname()[1]
    temp_srv_sock.listen(1)

    # Create non-blocking client socket
    client_sock = socket.socket()
    client_sock.setblocking(False)
    try:
        client_sock.connect(('localhost', port))
    except socket.error as err:
        # EWOULDBLOCK is not an error, as the socket is non-blocking
        if err.errno != errno.EWOULDBLOCK:
            raise

    # Use select to wait for connect() to succeed.
    import select
    timeout = 1
    readable = select.select([temp_srv_sock], [], [], timeout)[0]
    if temp_srv_sock not in readable:
        raise Exception('Client socket not connected in {} second(s)'.format(timeout))
    srv_sock, _ = temp_srv_sock.accept()

    return client_sock, srv_sock

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
