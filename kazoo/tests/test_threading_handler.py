from __future__ import annotations

import socket
import threading
import unittest

from typing import Any, Callable, Type

import pytest

from kazoo.handlers.threading import AsyncResult, SequentialThreadingHandler
from kazoo.handlers.utils import create_tcp_socket
from kazoo.testing.async_result_common import (
    AsyncResultCommon,
    ThreadlikeContext,
)


class ThreadContext(ThreadlikeContext):
    def __init__(self, target: Callable[[], None]):
        super().__init__(target)
        self._thread = threading.Thread(target=self._target)

    def __enter__(self) -> None:
        self._thread.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        self._thread.join()


class TestThreadingAsyncResult(AsyncResultCommon, unittest.TestCase):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(
            SequentialThreadingHandler,
            lambda handler: AsyncResult(handler),
            thread_context_factory=ThreadContext,
            *args,
            **kwargs,
        )


class TestThreadingHandler(unittest.TestCase):
    def _makeOne(self, *args: Any) -> SequentialThreadingHandler:
        return SequentialThreadingHandler(*args)

    def _getAsync(self) -> Type[AsyncResult]:
        return AsyncResult

    def test_proper_threading(self) -> None:
        h = self._makeOne()
        h.start()
        assert isinstance(h.event_object(), threading.Event)

    def test_matching_async(self) -> None:
        h = self._makeOne()
        h.start()
        async_result = self._getAsync()
        assert isinstance(h.async_result(), async_result)

    def test_exception_raising(self) -> None:
        h = self._makeOne()

        with pytest.raises(h.timeout_exception):
            raise h.timeout_exception("This is a timeout")

    def test_double_start_stop(self) -> None:
        h = self._makeOne()
        h.start()
        assert h._running is True
        h.start()
        h.stop()
        h.stop()
        assert h._running is False

    def test_huge_file_descriptor(self) -> None:
        try:
            import resource
        except ImportError:
            self.skipTest("resource module unavailable on this platform")
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (4096, 4096))
        except (ValueError, resource.error):
            self.skipTest("couldn't raise fd limit high enough")
        fd = 0
        socks = []
        while fd < 4000:
            sock = create_tcp_socket(socket)
            fd = sock.fileno()
            socks.append(sock)
        h = self._makeOne()
        h.start()
        h.select(socks, [], [], 0)
        h.stop()
        for sock in socks:
            sock.close()
