"""Unit tests for KazooClient.command()."""

from __future__ import annotations

from unittest.mock import Mock, patch

from kazoo.client import KazooClient


class TestClientCommand:
    """command() uses the peer host (not the port) as the TLS hostname."""

    def test_passes_peer_host_as_hostname(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        client._live.set()
        client._connection = Mock()
        client._connection._socket = Mock()
        client._connection._socket.getpeername.return_value = (
            "127.0.0.1",
            2181,
        )
        sock = Mock()
        with patch.object(
            client.handler, "create_connection", return_value=sock
        ) as mock_create_connection:
            client.command(b"ruok")

            kwargs = mock_create_connection.call_args.kwargs
            assert kwargs["hostname"] == "127.0.0.1"
