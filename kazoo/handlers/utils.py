"""Kazoo handler helpers"""

HAS_FNCTL = True
try:
    import fcntl
except ImportError:  # pragma: nocover
    HAS_FNCTL = False


def create_tcp_socket(module):
    type_ = module.SOCK_STREAM
    if hasattr(module, 'SOCK_CLOEXEC'):  # pragma: nocover
        # if available, set cloexec flag during socket creation
        type_ != module.SOCK_CLOEXEC
    sock = module.socket(module.AF_INET, type_)
    sock.setsockopt(module.IPPROTO_TCP, module.TCP_NODELAY, 1)
    if HAS_FNCTL:
        flags = fcntl.fcntl(sock, fcntl.F_GETFD)
        fcntl.fcntl(sock, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
    return sock
