from __future__ import annotations

import socket
from collections.abc import Iterator
from unittest.mock import patch

import pytest

from kazoo.client import KazooClient
from kazoo.protocol.connection import STOP_CONNECTING
from kazoo.retry import ForceRetryError, KazooRetry


@pytest.fixture
def client() -> Iterator[KazooClient]:
    client = KazooClient(hosts="zk.invalid:2181", randomize_hosts=False)
    client._stopped.clear()
    yield client
    client._stopped.set()
    client.close()


def addresses(*ips: str) -> list[tuple[int, int, int, str, tuple[str, int]]]:
    return [
        (
            socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
            "",
            (ip, 2181),
        )
        for ip in ips
    ]


def test_refreshes_dns_between_failed_attempts(client: KazooClient) -> None:
    connection = client._connection
    answers = [addresses("192.0.2.1", "192.0.2.2")]
    attempted = []

    def attempt(host: str, ip: str, port: int, retry: KazooRetry) -> object:
        attempted.append((host, ip, port))
        answers[0] = addresses("192.0.2.3")
        return STOP_CONNECTING if len(attempted) == 2 else None

    with (
        patch(
            "kazoo.protocol.connection.socket.getaddrinfo",
            side_effect=lambda *args: answers[0],
        ),
        patch.object(connection, "_connect_attempt", side_effect=attempt),
    ):
        assert connection._connect_loop(KazooRetry()) is STOP_CONNECTING
    assert attempted == [
        ("zk.invalid", "192.0.2.1", 2181),
        ("zk.invalid", "192.0.2.3", 2181),
    ]


def test_stable_dns_preserves_expanded_order(client: KazooClient) -> None:
    connection = client._connection
    endpoints = [
        ("b", "192.0.2.2", 2181),
        ("a", "192.0.2.1", 2181),
        ("b", "192.0.2.3", 2181),
    ]
    with (
        patch.object(
            connection, "_expand_client_hosts", return_value=endpoints
        ),
        patch.object(
            connection,
            "_resolve_host",
            side_effect=lambda host, port: [
                item for item in endpoints if item[0] == host
            ],
        ),
        patch.object(
            connection, "_connect_attempt", return_value=None
        ) as attempt,
        pytest.raises(ForceRetryError),
    ):
        connection._connect_loop(KazooRetry())
    assert [call.args[:3] for call in attempt.call_args_list] == endpoints


def test_churn_does_not_grow_pass_or_starve_other_host(
    client: KazooClient,
) -> None:
    connection = client._connection
    endpoints = [("a", "192.0.2.1", 2181), ("b", "192.0.2.2", 2181)]
    counter = iter(range(10, 100))
    with (
        patch.object(
            connection, "_expand_client_hosts", return_value=endpoints
        ),
        patch.object(
            connection,
            "_resolve_host",
            side_effect=lambda host, port: [
                (host, f"192.0.2.{next(counter)}", port)
            ],
        ),
        patch.object(
            connection, "_connect_attempt", return_value=None
        ) as attempt,
        pytest.raises(ForceRetryError),
    ):
        connection._connect_loop(KazooRetry())
    assert [call.args[:3] for call in attempt.call_args_list] == [
        ("a", "192.0.2.10", 2181),
        ("b", "192.0.2.11", 2181),
    ]


def test_disappeared_host_allows_later_host(client: KazooClient) -> None:
    connection = client._connection
    endpoints = [("a", "192.0.2.1", 2181), ("b", "192.0.2.2", 2181)]
    with (
        patch.object(
            connection, "_expand_client_hosts", return_value=endpoints
        ),
        patch.object(
            connection,
            "_resolve_host",
            side_effect=lambda host, port: (
                [] if host == "a" else [endpoints[1]]
            ),
        ),
        patch.object(
            connection, "_connect_attempt", return_value=STOP_CONNECTING
        ) as attempt,
    ):
        assert connection._connect_loop(KazooRetry()) is STOP_CONNECTING
    assert [call.args[:3] for call in attempt.call_args_list] == [endpoints[1]]


def test_duplicate_addresses_are_attempted_once(client: KazooClient) -> None:
    connection = client._connection
    with (
        patch(
            "kazoo.protocol.connection.socket.getaddrinfo",
            return_value=addresses("192.0.2.1", "192.0.2.1"),
        ),
        patch.object(
            connection, "_connect_attempt", return_value=None
        ) as attempt,
        pytest.raises(ForceRetryError),
    ):
        connection._connect_loop(KazooRetry())
    assert attempt.call_count == 1


def test_resolution_failure_skips_host(client: KazooClient) -> None:
    with patch(
        "kazoo.protocol.connection.socket.getaddrinfo",
        side_effect=socket.gaierror("not found"),
    ):
        with pytest.raises(ForceRetryError, match="No host resolved"):
            client._connection._connect_loop(KazooRetry())


def test_stop_prevents_attempt(client: KazooClient) -> None:
    client._stopped.set()
    with (
        patch(
            "kazoo.protocol.connection.socket.getaddrinfo",
            return_value=addresses("192.0.2.1"),
        ),
        patch.object(client._connection, "_connect_attempt") as attempt,
    ):
        assert (
            client._connection._connect_loop(KazooRetry()) is STOP_CONNECTING
        )
    attempt.assert_not_called()


def test_replacement_does_not_consume_later_slot(client: KazooClient) -> None:
    connection = client._connection
    with (
        patch.object(
            connection,
            "_expand_client_hosts",
            return_value=[("zk", "old", 2181), ("zk", "retained", 2181)],
        ),
        patch.object(
            connection,
            "_resolve_host",
            return_value=[("zk", "retained", 2181), ("zk", "new", 2181)],
        ),
        patch.object(
            connection, "_connect_attempt", return_value=None
        ) as attempt,
        pytest.raises(ForceRetryError),
    ):
        connection._connect_loop(KazooRetry())
    assert [call.args[1] for call in attempt.call_args_list] == [
        "retained",
        "new",
    ]


def test_stop_during_resolution_prevents_attempt(client: KazooClient) -> None:
    connection = client._connection

    def resolve(host: str, port: int) -> list[tuple[str, str, int]]:
        client._stopped.set()
        return [(host, "192.0.2.1", port)]

    with (
        patch.object(
            connection,
            "_expand_client_hosts",
            return_value=[("zk", "192.0.2.1", 2181)],
        ),
        patch.object(connection, "_resolve_host", side_effect=resolve),
        patch.object(connection, "_connect_attempt") as attempt,
    ):
        assert connection._connect_loop(KazooRetry()) is STOP_CONNECTING
    attempt.assert_not_called()
