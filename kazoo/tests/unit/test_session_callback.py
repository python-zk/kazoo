from __future__ import annotations

import time
import pytest

from kazoo.client import KazooClient
from kazoo.exceptions import AuthFailedError
from kazoo.protocol.states import KazooState, KeeperState


def test_session_callback_states() -> None:
    client = KazooClient()
    client._live.set()

    client._session_callback(KeeperState.CONNECTED)

    # Now with stopped
    client._stopped.set()
    client._session_callback(KeeperState.CONNECTED)

    # Test several state transitions
    client._stopped.clear()
    client.start_async = (  # type: ignore[method-assign]
        lambda: client.handler.async_result()
    )
    client._session_callback(KeeperState.CONNECTED)
    assert client.state == KazooState.CONNECTED

    client._session_callback(KeeperState.AUTH_FAILED)
    # FIXME mypy seems to be under the impression that the state can't
    # change as a result of the above call, even though it can.
    assert client.state == KazooState.LOST  # type: ignore[comparison-overlap]

    client._session_callback(-250)  # type: ignore[unreachable]
    assert client.state == KazooState.SUSPENDED


def test_start_fails_immediately_on_auth_failed() -> None:
    client = KazooClient()

    def fake_conn_start() -> None:
        client._session_callback(KeeperState.AUTH_FAILED)

    client._connection.start = (  # type: ignore[method-assign]
        fake_conn_start
    )
    start_time = time.monotonic()
    with pytest.raises(AuthFailedError):
        client.start(timeout=10.0)
    elapsed = time.monotonic() - start_time
    assert elapsed < 1.0


def test_start_fails_immediately_with_underlying_auth_error() -> None:
    client = KazooClient()
    expected_err = AuthFailedError("custom auth error")

    def fake_conn_start() -> None:
        client._auth_error = expected_err
        client._session_callback(KeeperState.AUTH_FAILED)

    client._connection.start = (  # type: ignore[method-assign]
        fake_conn_start
    )
    with pytest.raises(AuthFailedError) as exc_info:
        client.start(timeout=10.0)
    assert exc_info.value is expected_err


def test_start_async_triggers_on_auth_failed() -> None:
    client = KazooClient()
    expected_err = AuthFailedError("custom auth error")

    def fake_conn_start() -> None:
        client._auth_error = expected_err
        client._session_callback(KeeperState.AUTH_FAILED)

    client._connection.start = (  # type: ignore[method-assign]
        fake_conn_start
    )
    res = client.start_async()
    assert res.ready()
    assert not res.successful()
    assert res.exception is expected_err
    assert res.wait(timeout=5.0)
    with pytest.raises(AuthFailedError) as exc_info:
        res.get()
    assert exc_info.value is expected_err


def test_start_async_success() -> None:
    client = KazooClient()

    def fake_conn_start() -> None:
        client._session_callback(KeeperState.CONNECTED)

    client._connection.start = (  # type: ignore[method-assign]
        fake_conn_start
    )
    res = client.start_async()
    assert res.ready()
    assert res.successful()
    assert res.get() is True
    assert client.connected

    # Calling start_async when already connected returns completed result
    res2 = client.start_async()
    assert res2.ready()
    assert res2.successful()
    assert res2.get() is True


def test_start_async_recovers_from_expired_session() -> None:
    client = KazooClient()

    def fake_conn_start() -> None:
        # First attempt receives EXPIRED_SESSION (not terminal)
        client._session_callback(KeeperState.EXPIRED_SESSION)
        # Second attempt receives CONNECTED
        client._session_callback(KeeperState.CONNECTED)

    client._connection.start = (  # type: ignore[method-assign]
        fake_conn_start
    )
    res = client.start_async()
    assert res.ready()
    assert res.successful()
    assert res.get() is True
    assert client.connected
