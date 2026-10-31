from __future__ import annotations

import threading
import unittest

from unittest.mock import Mock

import pytest

from kazoo.handlers.utils import capture_exceptions, wrap, AsyncResult
from kazoo.interfaces import IHandler, SpawnedFunc


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


class TestAsyncResult(unittest.TestCase):
    # FIXME There seems no need to pass a timeout_factory to AsyncResult,
    # because it comes from the IHandler, which is already passed to
    # AsyncResult.
    # The timeout_factory is only used in the get() method of AsyncResult, and
    # it is not used anywhere else. So we can remove the timeout_factory from
    # the AsyncResult constructor and just use the IHandler's timeout_factory
    # in the get() method.
    def _makeOne(self, handler: IHandler) -> AsyncResult:
        def condition_factory() -> Mock_Condition:
            return Mock_Condition()

        def timeout_factory() -> Mock_Timeout:
            return Mock_Timeout()

        return AsyncResult(handler, condition_factory, timeout_factory)

    def _makeHandler(self) -> Mock_Handler:
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
        handler.add_to_completion_queue.side_effect = add_to_completion_queue
        return handler

    def test_ready(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        assert async_result.ready() is False
        async_result.set(None)
        assert async_result.ready() is True
        assert async_result.successful() is True
        assert async_result.exception is None
        assert async_result.get() is None

    def test_ready_value(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        assert async_result.ready() is False
        async_result.set("value")
        assert async_result.ready() is True
        assert async_result.successful() is True
        assert async_result.exception is None
        assert async_result.get() == "value"

    def test_callback_queued(self) -> None:
        mock_handler = self._makeHandler()
        mock_handler.start()
        async_result = self._makeOne(mock_handler)

        def add_on(async_result: AsyncResult) -> None:
            pass

        async_result.rawlink(add_on)
        async_result.set("val")

        mock_handler.add_to_completion_queue.assert_called_once()

    def test_set_exception(self) -> None:
        mock_handler = self._makeHandler()
        mock_handler.start()
        async_result = self._makeOne(mock_handler)

        def add_on(async_result: AsyncResult) -> None:
            pass

        async_result.rawlink(add_on)
        async_result.set_exception(ImportError("Error occurred"))

        assert async_result.ready() is True
        assert async_result.successful() is False
        assert isinstance(async_result.exception, ImportError)
        mock_handler.add_to_completion_queue.assert_called_once()

    def test_get_wait_while_setting(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        lst = []
        bv = threading.Event()
        cv = threading.Event()

        def wait_for_val() -> None:
            bv.set()
            val = async_result.get()
            lst.append(val)
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.start()
        bv.wait()

        async_result.set("fred")
        cv.wait()
        assert lst == ["fred"]
        th.join()

    def test_get_with_nowait(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)
        timeout = Mock_Timeout

        with pytest.raises(timeout):
            async_result.get(block=False)

        with pytest.raises(timeout):
            async_result.get_nowait()

    def test_get_with_exception(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        lst = []
        bv = threading.Event()
        cv = threading.Event()

        def wait_for_val() -> None:
            bv.set()
            try:
                val = async_result.get()
            except ImportError:
                lst.append("oops")
            else:
                lst.append(val)
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.start()
        bv.wait()

        async_result.set_exception(ImportError())
        cv.wait()
        assert lst == ["oops"]
        th.join()

    def test_wait(self) -> None:
        mock_handler = Mock()
        async_result = self._makeOne(mock_handler)

        lst: list[bool | str] = []
        bv = threading.Event()
        cv = threading.Event()

        def wait_for_val() -> None:
            bv.set()
            try:
                val = async_result.wait(10)
            except ImportError:
                lst.append("oops")
            else:
                lst.append(val)
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.start()
        bv.wait(10)

        async_result.set("fred")
        cv.wait(15)
        assert lst == [True]
        th.join()

    def test_wait_race(self) -> None:
        """Test that there is no race condition in `IAsyncResult.wait()`.

        Guards against the reappearance of:
            https://github.com/python-zk/kazoo/issues/485
        """
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        async_result.set("immediate")

        cv = threading.Event()

        def wait_for_val() -> None:
            # NB: should not sleep
            async_result.wait(20)
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.daemon = True
        th.start()

        # if the wait() didn't sleep (correctly), cv will be set quickly
        # if it did sleep, the cv will not be set yet and this will timeout
        cv.wait(10)
        assert cv.is_set() is True
        th.join()

    def test_set_before_wait(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        lst = []
        cv = threading.Event()
        async_result.set("fred")

        def wait_for_val() -> None:
            val = async_result.get()
            lst.append(val)
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.start()
        cv.wait()
        assert lst == ["fred"]
        th.join()

    def test_set_exc_before_wait(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        lst = []
        cv = threading.Event()
        async_result.set_exception(ImportError())

        def wait_for_val() -> None:
            try:
                val = async_result.get()
            except ImportError:
                lst.append("ooops")
            else:
                lst.append(val)
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.start()
        cv.wait()
        assert lst == ["ooops"]
        th.join()

    def test_linkage(self) -> None:
        mock_handler = self._makeHandler()
        mock_handler.start()
        async_result = self._makeOne(mock_handler)
        cv = threading.Event()

        lst = []

        def add_on(async_result: AsyncResult) -> None:
            lst.append(True)

        def wait_for_val() -> None:
            async_result.get()
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.start()

        async_result.rawlink(add_on)
        async_result.set(b"fred")
        mock_handler.add_to_completion_queue.assert_called_once()
        async_result.unlink(add_on)
        cv.wait()
        assert async_result.value == b"fred"
        th.join()

    def test_linkage_not_ready(self) -> None:
        mock_handler = self._makeHandler()
        mock_handler.start()
        async_result = self._makeOne(mock_handler)

        lst = []

        def add_on(async_result: AsyncResult) -> None:
            lst.append(True)

        async_result.set("fred")
        mock_handler.add_to_completion_queue.assert_not_called()
        async_result.rawlink(add_on)
        mock_handler.add_to_completion_queue.assert_called_once()

    def test_link_and_unlink(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        lst = []

        def add_on(async_result: AsyncResult) -> None:
            lst.append(True)

        async_result.rawlink(add_on)
        mock_handler.add_to_completion_queue.assert_not_called()
        async_result.unlink(add_on)
        async_result.set("fred")
        mock_handler.add_to_completion_queue.assert_not_called()

    def test_captured_exception(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        @capture_exceptions(async_result)
        def exceptional_function() -> float:
            return 1 / 0

        exceptional_function()

        with pytest.raises(ZeroDivisionError):
            async_result.get()

    def test_no_capture_exceptions(self) -> None:
        mock_handler = self._makeHandler()
        async_result = self._makeOne(mock_handler)

        lst = []

        def add_on(async_result: AsyncResult) -> None:
            lst.append(True)

        async_result.rawlink(add_on)

        @capture_exceptions(async_result)
        def regular_function() -> bool:
            return True

        regular_function()

        mock_handler.add_to_completion_queue.assert_not_called()

    def test_wraps(self) -> None:
        mock_handler = Mock()
        async_result = self._makeOne(mock_handler)

        lst = []

        def add_on(result: AsyncResult) -> None:
            lst.append(result.get())

        async_result.rawlink(add_on)

        @wrap(async_result)
        def regular_function() -> str:
            return "hello"

        assert regular_function() == "hello"
        mock_handler.add_to_completion_queue.assert_called_once()
        assert async_result.get() == "hello"

    def test_multiple_callbacks(self) -> None:
        mockback1 = Mock(name="mockback1")
        mockback2 = Mock(name="mockback2")
        handler = self._makeHandler()
        handler.start()

        async_result = self._makeOne(handler)
        async_result.rawlink(mockback1)
        async_result.rawlink(mockback2)
        print("set")
        async_result.set("howdy")
        print("wait")
        async_result.wait()
        print("stop")
        handler.stop()
        print("done")

        mockback2.assert_called_once_with(async_result)
        mockback1.assert_called_once_with(async_result)
