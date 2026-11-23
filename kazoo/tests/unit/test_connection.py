from __future__ import annotations

import collections
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest

from kazoo.exceptions import (
    ConnectionDropped,
    SASLException,
    SessionClosedRequireSaslError,
)
from kazoo.loggingsupport import BLATHER
from kazoo.protocol.connection import (
    CLOSE_RESPONSE,
    STOP_CONNECTING,
    ConnectionHandler,
    RWPinger,
    RWServerAvailable,
)
from kazoo.protocol.serialization import ReplyHeader
from kazoo.protocol.states import KeeperState
from kazoo.retry import KazooRetry


class TestConnectionAuthExceptions:
    def test_connect_attempt_sasl_exception(self) -> None:
        client = Mock()
        handler = Mock()
        handler.timeout_exception = TimeoutError
        client.handler = handler
        client._state = KeeperState.CONNECTING
        retry = KazooRetry()
        connection = ConnectionHandler(client, retry)

        with patch.object(
            connection, "_connect", side_effect=SASLException("library error")
        ):
            result = connection._connect_attempt(
                "127.0.0.1", "127.0.0.1", 2181, retry
            )
            assert result is STOP_CONNECTING
            client._session_callback.assert_called_with(
                KeeperState.AUTH_FAILED
            )

    def test_connect_attempt_session_closed_require_sasl(self) -> None:
        client = Mock()
        handler = Mock()
        handler.timeout_exception = TimeoutError
        client.handler = handler
        client._state = KeeperState.CONNECTING
        retry = KazooRetry()
        connection = ConnectionHandler(client, retry)

        with patch.object(
            connection,
            "_connect",
            side_effect=SessionClosedRequireSaslError(
                "session closed require sasl"
            ),
        ):
            result = connection._connect_attempt(
                "127.0.0.1", "127.0.0.1", 2181, retry
            )
            assert result is STOP_CONNECTING
            client._session_callback.assert_called_with(
                KeeperState.AUTH_FAILED
            )


class TestConnectionXidMismatch:
    def test_read_response_xid_mismatch_sets_exception_and_raises(
        self,
    ) -> None:
        client = Mock()
        async_obj = Mock()
        request = Mock()
        expected_xid = 1
        client._pending = collections.deque(
            [(request, async_obj, expected_xid)]
        )

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=2, zxid=100, err=0)

        with pytest.raises(RuntimeError) as exc_info:
            connection._read_response(header, b"", 0)

        assert exc_info.value.args == (
            "xids do not match, expected %r received %r",
            1,
            2,
        )
        assert client.last_zxid == 100
        async_obj.set_exception.assert_called_once_with(exc_info.value)

    def test_invoke_xid_mismatch_raises(self) -> None:
        client = Mock()
        connection = ConnectionHandler(client, KazooRetry())

        header = ReplyHeader(xid=99, zxid=50, err=0)
        with patch.object(connection, "_submit"):
            with patch.object(
                connection, "_read_header", return_value=(header, b"", 0)
            ):
                with pytest.raises(RuntimeError) as exc_info:
                    connection._invoke(timeout=1.0, request=Mock(), xid=42)

        assert exc_info.value.args == (
            "xids do not match, expected %r received %r",
            42,
            99,
        )

    def test_read_socket_dispatches_to_read_response_on_mismatch(
        self,
    ) -> None:
        client = Mock()
        async_obj = Mock()
        request = Mock()
        client._pending = collections.deque([(request, async_obj, 5)])

        connection = ConnectionHandler(client, KazooRetry())
        header = ReplyHeader(xid=6, zxid=200, err=0)

        with patch.object(
            connection, "_read_header", return_value=(header, b"", 0)
        ):
            with pytest.raises(RuntimeError) as exc_info:
                connection._read_socket(read_timeout=1.0)

        assert exc_info.value.args == (
            "xids do not match, expected %r received %r",
            5,
            6,
        )
        async_obj.set_exception.assert_called_once_with(exc_info.value)


class TestRWServerSelection:
    def test_rw_pinger_skips_down_and_ro_hosts_and_returns_hostip(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        send_fail_sock = Mock()
        send_fail_sock.sendall.side_effect = OSError("broken pipe")
        ro_sock = Mock()
        ro_sock.recv.return_value = b"ro"
        rw_sock = Mock()
        rw_sock.recv.return_value = b"rw"

        conns = {
            "10.0.0.1": send_fail_sock,
            "10.0.0.2": ro_sock,
            "10.0.0.3": rw_sock,
        }
        dialed: list[tuple[str, str, int, float | None]] = []

        def fake_connect(
            host: str, hostip: str, port: int, timeout: float | None
        ) -> Mock:
            dialed.append((host, hostip, port, timeout))
            if hostip == "10.0.0.0":
                raise OSError("connection refused")
            return conns[hostip]

        @contextmanager
        def socket_handling() -> Iterator[None]:
            try:
                yield
            except OSError as exc:
                raise ConnectionDropped(str(exc)) from exc

        logger = logging.getLogger("kazoo.test.rwpinger")
        pinger = RWPinger(
            [
                ("refused.example", "10.0.0.0", 2180),
                ("sendfail.example", "10.0.0.1", 2181),
                ("ro.example", "10.0.0.2", 2182),
                ("rw.example", "10.0.0.3", 2183),
            ],
            fake_connect,
            socket_handling,
            timeout=0.75,
            logger=logger,
        )

        with caplog.at_level(logging.DEBUG, logger="kazoo.test.rwpinger"):
            it = iter(pinger)
            # Each next() probes at most one endpoint
            assert next(it) is False
            assert len(dialed) == 1
            assert next(it) is False
            assert len(dialed) == 2
            assert next(it) is False
            assert len(dialed) == 3
            result = next(it)

        assert result == ("rw.example", "10.0.0.3", 2183)
        assert dialed == [
            ("refused.example", "10.0.0.0", 2180, 0.75),
            ("sendfail.example", "10.0.0.1", 2181, 0.75),
            ("ro.example", "10.0.0.2", 2182, 0.75),
            ("rw.example", "10.0.0.3", 2183, 0.75),
        ]
        send_fail_sock.settimeout.assert_called_once_with(0.75)
        ro_sock.settimeout.assert_called_once_with(0.75)
        rw_sock.settimeout.assert_called_once_with(0.75)
        send_fail_sock.close.assert_called_once()
        ro_sock.close.assert_called_once()
        rw_sock.close.assert_called_once()
        assert (
            "Pinging server for r/w: rw.example(10.0.0.3):2183" in caplog.text
        )

    def test_rw_pinger_exponential_backoff(self) -> None:
        ro_sock = Mock()
        ro_sock.recv.return_value = b"ro"
        dialed: list[tuple[str, str, int, float | None]] = []

        def fake_connect(
            host: str, hostip: str, port: int, timeout: float | None
        ) -> Mock:
            dialed.append((host, hostip, port, timeout))
            return ro_sock

        @contextmanager
        def no_error() -> Iterator[None]:
            yield

        now = 100.0
        pinger = RWPinger(
            [
                ("ro1.example", "10.0.0.1", 2181),
                ("ro2.example", "10.0.0.2", 2182),
            ],
            fake_connect,
            no_error,
        )

        with (
            patch(
                "kazoo.protocol.connection.time.monotonic",
                side_effect=lambda: now,
            ),
            patch("kazoo.protocol.connection.random.randint", return_value=0),
        ):
            it = iter(pinger)
            # First run-through steps one host per next() before back-off
            assert next(it) is False
            assert len(dialed) == 1
            assert next(it) is False
            assert len(dialed) == 2

            # Within initial 0.5s backoff -> skips without dialing
            now = 100.4
            assert next(it) is False
            assert len(dialed) == 2

            # Past 0.5s -> second pass starts, next backoff doubles to 1.0s
            now = 100.5
            assert next(it) is False
            assert len(dialed) == 3
            assert next(it) is False
            assert len(dialed) == 4

            now = 101.4
            assert next(it) is False
            assert len(dialed) == 4

            # Past 1.0s -> third pass starts
            now = 101.5
            assert next(it) is False
            assert len(dialed) == 5

    def test_server_pinger_reuses_host_ports_and_shares_open_socket(
        self,
    ) -> None:
        client = Mock()
        client._session_timeout = 10000
        client.use_ssl = True
        client.keyfile = "k"
        client.certfile = "c"
        client.ca = "ca"
        client.keyfile_password = "pw"
        client.verify_certs = True
        client.check_hostname = True
        sock = Mock()
        sock.recv.return_value = b"rw"
        handler = Mock()
        handler.create_connection.return_value = sock
        client.handler = handler
        connection = ConnectionHandler(client, KazooRetry())
        connection._host_ports = [("rw.example", "10.0.0.2", 2182)]

        pinger = connection._server_pinger()
        result = next(iter(pinger))

        assert result == ("rw.example", "10.0.0.2", 2182)
        handler.create_connection.assert_called_once_with(
            address=("10.0.0.2", 2182),
            hostname="rw.example",
            timeout=1.0,
            use_ssl=True,
            keyfile="k",
            certfile="c",
            ca="ca",
            keyfile_password="pw",
            verify_certs=True,
            check_hostname=True,
        )
        sock.settimeout.assert_called_once_with(1.0)
        sock.close.assert_called_once()

    def test_connect_attempt_rw_server_overrides_host_hostip_port_and_logs(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = Mock()
        handler = Mock()
        handler.timeout_exception = TimeoutError
        sock = Mock()
        handler.select.return_value = ([sock], [], [])
        client.handler = handler
        client._state = KeeperState.CONNECTING
        retry = KazooRetry()
        logger = logging.getLogger("kazoo.test.rw_attempt")
        connection = ConnectionHandler(client, retry, logger=logger)
        connection._rw_server = ("rw.example", "10.0.0.2", 2182)

        def fake_connect(
            host: str, hostip: str, port: int
        ) -> tuple[float, float]:
            connection._socket = sock
            return (10.0, 10.0)

        with (
            caplog.at_level(BLATHER, logger="kazoo.test.rw_attempt"),
            patch.object(
                connection, "_connect", side_effect=fake_connect
            ) as mock_connect,
            patch.object(
                connection, "_read_socket", return_value=CLOSE_RESPONSE
            ),
        ):
            result = connection._connect_attempt(
                "ro.example", "10.0.0.1", 2181, retry
            )

        assert result is STOP_CONNECTING
        assert connection._rw_server is None
        mock_connect.assert_called_once_with("rw.example", "10.0.0.2", 2182)
        assert (
            "Found r/w server to use, rw.example(10.0.0.2):2182" in caplog.text
        )
        assert "ro.example" not in caplog.text
        assert "Closing connection to rw.example(10.0.0.2):2182" in caplog.text

    def test_connect_loop_failover_uses_rw_server_hostip(self) -> None:
        client = Mock()
        client._stopped.is_set.return_value = False
        handler = Mock()
        handler.timeout_exception = TimeoutError
        client.handler = handler
        client._state = KeeperState.CONNECTING
        client.hosts = [
            ("ro.example", 2181),
            ("other.example", 2183),
        ]
        retry = KazooRetry()
        connection = ConnectionHandler(client, retry)

        expanded = [
            ("ro.example", "10.0.0.1", 2181),
            ("other.example", "10.0.0.3", 2183),
        ]
        sock1 = Mock()
        sock2 = Mock()
        handler.select.return_value = ([sock2], [], [])
        connect_calls: list[tuple[str, str, int]] = []

        def fake_connect(
            host: str, hostip: str, port: int
        ) -> tuple[float, float]:
            connect_calls.append((host, hostip, port))
            if len(connect_calls) == 1:
                connection._socket = sock1
                connection._rw_server = ("rw.example", "10.0.0.2", 2182)
                raise RWServerAvailable()
            connection._socket = sock2
            return (10.0, 10.0)

        with (
            patch.object(
                connection, "_expand_client_hosts", return_value=expanded
            ),
            patch.object(connection, "_connect", side_effect=fake_connect),
            patch.object(
                connection, "_read_socket", return_value=CLOSE_RESPONSE
            ),
        ):
            result = connection._connect_loop(retry)

        assert result is STOP_CONNECTING
        assert connect_calls == [
            ("ro.example", "10.0.0.1", 2181),
            ("rw.example", "10.0.0.2", 2182),
        ]
        assert connection._rw_server is None
