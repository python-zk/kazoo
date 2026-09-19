from __future__ import annotations

import collections
from unittest.mock import Mock, patch

import pytest

from kazoo.exceptions import (
    SASLException,
    SessionClosedRequireSaslError,
)
from kazoo.protocol.connection import (
    ConnectionHandler,
    STOP_CONNECTING,
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
