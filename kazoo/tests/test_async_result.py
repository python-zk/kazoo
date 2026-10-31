from __future__ import annotations

import threading
import unittest

from typing import Any, Callable
from unittest.mock import Mock

# Using AsyncResult from kazoo.handlers.utils isn't ideal, but it allows us to
# smoke test the AsyncResult interface without having to depend on a specific
# handler implementation or writing a mock.
# Actually this whole thing is probably now useless, since well call this from
# the other handler tests, but leaving it here for now.
from kazoo.handlers.utils import AsyncResult
from kazoo.interfaces import IHandler, SpawnedFunc
from kazoo.testing.async_result_common import (
    AsyncResultCommon,
    ThreadlikeContext,
)


class Mock_Condition:
    def __init__(self) -> None:
        self._event = threading.Event()

    def __enter__(self) -> Mock_Condition:
        return self

    def __exit__(self, *exc_info: object) -> None:
        pass

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)

    def notify_all(self) -> None:
        self._event.set()


class Mock_Timeout(BaseException):
    pass


class Mock_Handler(Mock):
    _running: bool

    @property
    def running(self) -> bool:
        return self._running


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


class TestAsyncResult(AsyncResultCommon, unittest.TestCase):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        def condition_factory() -> Mock_Condition:
            return Mock_Condition()

        def timeout_factory() -> Mock_Timeout:
            return Mock_Timeout()

        def handler_factory() -> Mock_Handler:
            handler = Mock_Handler(spec=IHandler)
            handler._running = False

            def start() -> None:
                handler._running = True

            def stop() -> None:
                handler._running = False

            def add_to_completion_queue(callback: SpawnedFunc) -> None:
                callback()

            handler.start.side_effect = start
            handler.stop.side_effect = stop
            handler.add_to_completion_queue.side_effect = (
                add_to_completion_queue
            )
            handler.timeout_exception = Mock_Timeout
            handler.event_object = lambda: threading.Event()
            return handler

        super().__init__(
            handler_factory,
            lambda handler: AsyncResult(
                handler, condition_factory, timeout_factory
            ),
            thread_context_factory=ThreadContext,
            *args,
            **kwargs,
        )
