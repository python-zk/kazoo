"""Unit tests for KazooClient.command()."""

from __future__ import annotations

import unittest
from unittest.mock import Mock

from kazoo.client import KazooClient


class ClientCommandTestCase(unittest.TestCase):
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
        client.handler.create_connection = Mock(return_value=sock)

        client.command(b"ruok")

        kwargs = client.handler.create_connection.call_args.kwargs
        self.assertEqual(kwargs["hostname"], "127.0.0.1")


if __name__ == "__main__":
    unittest.main()
