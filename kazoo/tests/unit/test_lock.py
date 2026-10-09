from __future__ import annotations

from unittest import mock

import pytest

from kazoo.exceptions import (
    ConnectionLoss,
    KazooException,
    NoNodeError,
    SessionExpiredError,
)
from kazoo.recipe.lock import Lock, Semaphore
from kazoo.retry import KazooRetry


def _dummy_retry() -> KazooRetry:
    return KazooRetry(
        max_tries=3,
        delay=0,
        backoff=1,
        sleep_func=lambda _: None,
    )


class TestLockCleanup:
    """Unit tests for Lock cleanup logic and retry handling."""

    def test_cleanup_success(self) -> None:
        """Test that _cleanup deletes the lock node successfully."""
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        lock = Lock(client, "/testlock")
        lock.node = "node_0000000001"

        lock._cleanup()

        client.delete.assert_called_once_with("/testlock/node_0000000001")

    def test_cleanup_retries_on_connection_loss(self) -> None:
        """Test that _cleanup retries deletion when ConnectionLoss occurs."""
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        lock = Lock(client, "/testlock")
        lock.node = "node_0000000001"

        client.delete.side_effect = [ConnectionLoss(), None]

        lock._cleanup()

        assert client.delete.call_count == 2
        client.delete.assert_called_with("/testlock/node_0000000001")

    def test_cleanup_swallows_no_node_error(self) -> None:
        """Test that _cleanup ignores NoNodeError if the node was already
        deleted.
        """
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        lock = Lock(client, "/testlock")
        lock.node = "node_0000000001"

        client.delete.side_effect = NoNodeError()

        lock._cleanup()

        client.delete.assert_called_once_with("/testlock/node_0000000001")

    def test_cleanup_swallows_session_expired(self) -> None:
        """Test that _cleanup handles SessionExpiredError gracefully."""
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        lock = Lock(client, "/testlock")
        lock.node = "node_0000000001"

        client.delete.side_effect = SessionExpiredError()

        lock._cleanup()

        client.delete.assert_called_once_with("/testlock/node_0000000001")

    def test_cleanup_finds_node_if_none(self) -> None:
        """Test that _cleanup calls _find_node if lock.node is not set."""
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        lock = Lock(client, "/testlock")
        lock.node = None
        lock._find_node = mock.MagicMock(return_value="found_node")

        lock._cleanup()

        lock._find_node.assert_called_once()
        client.delete.assert_called_once_with("/testlock/found_node")

    def test_acquire_cleans_up_on_failure(self) -> None:
        """Test that acquire calls _cleanup when non-blocking acquisition
        fails.
        """
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        lock = Lock(client, "/testlock")
        lock._inner_acquire = mock.MagicMock(return_value=False)
        lock._cleanup = mock.MagicMock()

        gotten = lock.acquire(blocking=False)

        assert gotten is False
        lock._cleanup.assert_called_once()

    def test_acquire_cleans_up_on_exception(self) -> None:
        """Test that acquire calls _cleanup when an exception is raised."""
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        lock = Lock(client, "/testlock")
        lock._inner_acquire = mock.MagicMock(
            side_effect=KazooException("Fail!")
        )
        lock._cleanup = mock.MagicMock()

        with pytest.raises(KazooException):
            lock.acquire()

        lock._cleanup.assert_called_once()


class TestSemaphoreCleanup:
    """Unit tests for Semaphore cleanup logic and retry handling."""

    def test_cleanup_success(self) -> None:
        """Test that Semaphore._cleanup deletes create_path successfully."""
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        sem = Semaphore(client, "/testsem", "client_1")
        sem.create_path = "/testsem/lease_1"

        sem._cleanup()

        client.delete.assert_called_once_with("/testsem/lease_1")

    def test_cleanup_retries_on_connection_loss(self) -> None:
        """Test that Semaphore._cleanup retries on ConnectionLoss."""
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        sem = Semaphore(client, "/testsem", "client_1")
        sem.create_path = "/testsem/lease_1"

        client.delete.side_effect = [ConnectionLoss(), None]

        sem._cleanup()

        assert client.delete.call_count == 2
        client.delete.assert_called_with("/testsem/lease_1")

    def test_cleanup_swallows_no_node_error(self) -> None:
        """Test that Semaphore._cleanup ignores NoNodeError."""
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        sem = Semaphore(client, "/testsem", "client_1")
        sem.create_path = "/testsem/lease_1"

        client.delete.side_effect = NoNodeError()

        sem._cleanup()

        client.delete.assert_called_once_with("/testsem/lease_1")

    def test_cleanup_swallows_session_expired(self) -> None:
        """Test that Semaphore._cleanup handles SessionExpiredError
        gracefully.
        """
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        sem = Semaphore(client, "/testsem", "client_1")
        sem.create_path = "/testsem/lease_1"

        client.delete.side_effect = SessionExpiredError()

        sem._cleanup()

        client.delete.assert_called_once_with("/testsem/lease_1")

    def test_acquire_cleans_up_on_exception(self) -> None:
        """Test that Semaphore.acquire calls _cleanup when an exception is
        raised.
        """
        client = mock.MagicMock()
        client.retry = _dummy_retry()
        sem = Semaphore(client, "/testsem", "client_1")
        sem._inner_acquire = mock.MagicMock(
            side_effect=KazooException("Sem Fail!")
        )
        sem._cleanup = mock.MagicMock()

        with pytest.raises(KazooException):
            sem.acquire()

        sem._cleanup.assert_called_once()
