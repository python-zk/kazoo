from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Callable, Generator
from unittest.mock import Mock

import pytest

from kazoo.interfaces import IAsyncResult, IHandler
from kazoo.handlers.utils import capture_exceptions, wrap


class ThreadlikeContext(ABC):
    def __init__(self, target: Callable[[], None]):
        self._target = target

    @abstractmethod
    def __enter__(self) -> None:
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass


# FIXME It's not clear whether this needs all the tests to be run with and
# without a running handler to test all the paths. The original mocks always
# returned a truth value for `running`.
class AsyncResultCommon:
    def __init__(
        self,
        handler_factory: Callable[[], IHandler],
        async_result_factory: Callable[[IHandler], IAsyncResult],
        *args: Any,
        thread_context_factory: Callable[
            [Callable[[], None]], ThreadlikeContext
        ],
        **kwargs: Any,
    ) -> None:
        self._handler_factory = handler_factory
        self._async_result_factory = async_result_factory
        self._thread_context_factory = thread_context_factory
        super().__init__(*args, **kwargs)

    @contextlib.contextmanager
    def _start_and_stop_handler(self) -> Generator[IHandler, None, None]:
        self._handler.start()
        try:
            yield self._handler
        finally:
            self._handler.stop()

    def setUp(self) -> None:
        self._handler = self._handler_factory()
        self._async_result = self._async_result_factory(self._handler)

    def test_ready(self) -> None:
        assert self._async_result.ready() is False
        self._async_result.set(None)
        assert self._async_result.ready() is True
        assert self._async_result.successful() is True
        assert self._async_result.exception is None
        assert self._async_result.get() is None

    def test_ready_value(self) -> None:
        assert self._async_result.ready() is False
        self._async_result.set("value")
        assert self._async_result.ready() is True
        assert self._async_result.successful() is True
        assert self._async_result.exception is None
        assert self._async_result.get() == "value"

    def test_callback_queued(self) -> None:
        callback_called = self._handler.event_object()

        def add_on(async_result: IAsyncResult) -> None:
            callback_called.set()

        self._async_result.rawlink(add_on)
        self._async_result.set("val")

        assert callback_called.wait(10)

    def test_set_exception(self) -> None:
        callback_called = self._handler.event_object()

        def add_on(async_result: IAsyncResult) -> None:
            callback_called.set()

        self._async_result.rawlink(add_on)
        self._async_result.set_exception(ImportError("Error occurred"))

        assert self._async_result.ready() is True
        assert self._async_result.successful() is False
        assert isinstance(self._async_result.exception, ImportError)

        assert callback_called.wait(10)

    def test_get_wait_while_setting(self) -> None:
        lst = []
        bv = self._handler.event_object()
        cv = self._handler.event_object()

        def wait_for_val() -> None:
            bv.set()
            val = self._async_result.get()
            lst.append(val)
            cv.set()

        with self._thread_context_factory(wait_for_val):
            bv.wait()
            self._async_result.set("fred")
            cv.wait()
        assert lst == ["fred"]

    def test_get_with_nowait(self) -> None:
        timeout = self._handler.timeout_exception

        with pytest.raises(timeout):
            self._async_result.get(block=False)

        with pytest.raises(timeout):
            self._async_result.get_nowait()

    def test_get_with_exception(self) -> None:
        lst = []
        bv = self._handler.event_object()
        cv = self._handler.event_object()

        def wait_for_val() -> None:
            bv.set()
            try:
                val = self._async_result.get()
            except ImportError:
                lst.append("oops")
            else:
                lst.append(val)
            cv.set()

        with self._thread_context_factory(wait_for_val):
            bv.wait()
            self._async_result.set_exception(ImportError())
            cv.wait()
        assert lst == ["oops"]

    def test_wait(self, *, is_green: bool = False) -> None:
        lst: list[bool | str] = []
        bv = self._handler.event_object()
        cv = self._handler.event_object()

        expected_list = ["fred" if is_green else True]

        def wait_for_val() -> None:
            bv.set()
            try:
                val = self._async_result.wait(15)
            except self._handler.timeout_exception:
                lst.append("oops")
            else:
                lst.append(val)
            cv.set()

        with self._thread_context_factory(wait_for_val):
            bv.wait(10)
            self._async_result.set("fred")
            cv.wait(15)

        assert lst == expected_list

    def test_wait_race(self) -> None:
        """Test that there is no race condition in `IAsyncResult.wait()`.

        Guards against the reappearance of:
            https://github.com/python-zk/kazoo/issues/485
        """
        self._async_result.set("immediate")

        cv = self._handler.event_object()

        def wait_for_val() -> None:
            # NB: should not sleep
            self._async_result.wait(20)
            cv.set()

        with self._thread_context_factory(wait_for_val):
            self._async_result.set("immediate")
            cv.wait(10)

        # if the wait() didn't sleep (correctly), cv will be set quickly
        # if it did sleep, the cv will not be set yet and this will timeout
        cv.wait(10)
        assert cv.is_set()

    def test_set_before_wait(self) -> None:
        lst = []
        cv = self._handler.event_object()
        self._async_result.set("fred")

        def wait_for_val() -> None:
            val = self._async_result.get()
            lst.append(val)
            cv.set()

        with self._thread_context_factory(wait_for_val):
            self._async_result.set("fred")
            cv.wait()
        assert lst == ["fred"]

    def test_set_exc_before_wait(self) -> None:
        lst = []
        cv = self._handler.event_object()
        self._async_result.set_exception(ImportError())

        def wait_for_val() -> None:
            try:
                val = self._async_result.get()
            except ImportError:
                lst.append("ooops")
            else:
                lst.append(val)
            cv.set()

        with self._thread_context_factory(wait_for_val):
            self._async_result.set_exception(ImportError())
            cv.wait()

        assert lst == ["ooops"]

    def test_linkage(self) -> None:
        cv = self._handler.event_object()
        callback_called = self._handler.event_object()

        def add_on(async_result: IAsyncResult) -> None:
            callback_called.set()

        def wait_for_val() -> None:
            self._async_result.get()
            cv.set()

        with self._thread_context_factory(wait_for_val):
            self._async_result.rawlink(add_on)
            self._async_result.set(b"fred")
            cv.wait()
            self._async_result.unlink(add_on)

        assert callback_called.wait(10)
        assert self._async_result.get() == b"fred"

    def test_linkage_not_ready(self) -> None:
        callback_called = self._handler.event_object()
        lst = []

        def add_on(async_result: IAsyncResult) -> None:
            lst.append(True)
            callback_called.set()

        self._async_result.set("fred")
        self._async_result.rawlink(add_on)
        assert callback_called.wait(10)
        assert lst == [True]

    def test_link_and_unlink(self) -> None:
        lst = []

        def add_on(async_result: IAsyncResult) -> None:
            lst.append(True)

        self._async_result.rawlink(add_on)
        self._async_result.unlink(add_on)
        self._async_result.set("fred")
        assert lst == []

    def test_captured_exception(self) -> None:
        @capture_exceptions(self._async_result)
        def exceptional_function() -> float:
            return 1 / 0

        exceptional_function()

        with pytest.raises(ZeroDivisionError):
            self._async_result.get()

    def test_no_capture_exceptions(self) -> None:
        lst = []

        def add_on(async_result: IAsyncResult) -> None:
            lst.append(True)

        self._async_result.rawlink(add_on)

        @capture_exceptions(self._async_result)
        def regular_function() -> bool:
            return True

        regular_function()

        assert lst == []

    def test_wraps(self) -> None:
        callback_called = self._handler.event_object()
        lst = []

        def add_on(result: IAsyncResult) -> None:
            lst.append(result.get())
            callback_called.set()

        self._async_result.rawlink(add_on)

        @wrap(self._async_result)
        def regular_function() -> str:
            return "hello"

        assert regular_function() == "hello"
        assert callback_called.wait(10)
        assert self._async_result.get() == "hello"

    def test_multiple_callbacks(self) -> None:
        mockback1 = Mock(name="mockback1")
        mockback2 = Mock(name="mockback2")

        self._async_result.rawlink(mockback1)
        self._async_result.rawlink(mockback2)
        self._async_result.set("howdy")
        self._async_result.wait()

        mockback2.assert_called_once_with(self._async_result)
        mockback1.assert_called_once_with(self._async_result)
