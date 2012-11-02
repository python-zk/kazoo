"""Kazoo handler helpers"""


def create_tcp_socket(module):
    sock = module.socket(module.AF_INET, module.SOCK_STREAM)
    sock.setsockopt(module.IPPROTO_TCP, module.TCP_NODELAY, 1)
    return sock
