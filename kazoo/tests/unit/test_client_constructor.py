from __future__ import annotations

import pytest

from kazoo.client import KazooClient
from kazoo.exceptions import ConfigurationError
from kazoo.retry import KazooRetry


def test_invalid_handler() -> None:
    from kazoo.handlers.threading import (
        SequentialThreadingHandler,
    )

    with pytest.raises(ConfigurationError):
        KazooClient(handler=SequentialThreadingHandler)


def test_chroot() -> None:
    assert KazooClient(hosts="127.0.0.1:2181/").chroot == ""
    assert KazooClient(hosts="127.0.0.1:2181/a").chroot == "/a"
    assert KazooClient(hosts="127.0.0.1/a").chroot == "/a"
    assert KazooClient(hosts="127.0.0.1/a/b").chroot == "/a/b"
    assert (
        KazooClient(hosts="127.0.0.1:2181,127.0.0.1:2182/a/b").chroot == "/a/b"
    )


def test_connection_timeout() -> None:
    from kazoo.handlers.threading import (
        KazooTimeoutError,
    )

    client = KazooClient(hosts="127.0.0.1:9")
    assert client.handler.timeout_exception is KazooTimeoutError

    with pytest.raises(client.handler.timeout_exception):
        client.start(0.1)


def test_ordered_host_selection() -> None:
    client = KazooClient(
        hosts="127.0.0.1:9,127.0.0.2:9/a", randomize_hosts=False
    )
    hosts = [h for h in client.hosts]
    assert hosts == [("127.0.0.1", 9), ("127.0.0.2", 9)]


def test_invalid_hostname() -> None:
    client = KazooClient(hosts="nosuchhost/a")
    timeout = client.handler.timeout_exception
    with pytest.raises(timeout):
        client.start(0.1)


def test_another_invalid_hostname() -> None:
    with pytest.raises(ValueError):
        KazooClient(hosts="/nosuchhost/a")


def test_retry_options_dict() -> None:
    client = KazooClient(
        command_retry=dict(max_tries=99), connection_retry=dict(delay=99)
    )
    assert isinstance(client._conn_retry, KazooRetry)
    assert isinstance(client._retry, KazooRetry)
    assert client._retry.max_tries == 99
    assert client._conn_retry.delay == 99
