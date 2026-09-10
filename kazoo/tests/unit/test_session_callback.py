from __future__ import annotations

import threading

from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState, KeeperState


def test_session_callback_states() -> None:
    client = KazooClient()
    client._handle = 1  # type: ignore[assignment]
    client._live.set()

    result = client._session_callback(KeeperState.CONNECTED)
    assert result is None

    # Now with stopped
    client._stopped.set()
    result = client._session_callback(KeeperState.CONNECTED)
    assert result is None

    # Test several state transitions
    client._stopped.clear()
    client.start_async = (  # type: ignore[method-assign]
        lambda: threading.Event()  # type: ignore[return-value]
    )
    client._session_callback(KeeperState.CONNECTED)
    assert client.state == KazooState.CONNECTED

    client._session_callback(KeeperState.AUTH_FAILED)
    assert client.state == KazooState.LOST  # type: ignore[comparison-overlap]

    client._handle = 1  # type: ignore[assignment]
    client._session_callback(-250)  # type: ignore[unreachable]
    assert client.state == KazooState.SUSPENDED
