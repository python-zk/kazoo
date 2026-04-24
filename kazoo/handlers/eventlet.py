"""A eventlet based handler."""

from __future__ import annotations
from __future__ import absolute_import

import atexit
import contextlib
import logging

from typing import Any, Generator, TYPE_CHECKING

import eventlet
from eventlet.green import socket as green_socket
from eventlet.green import time as green_time
from eventlet.green import threading as green_threading
from eventlet.green import selectors as green_selectors
from eventlet import queue as green_queue

from kazoo.handlers import utils
from kazoo.handlers.utils import selector_select

if TYPE_CHECKING:
    import socket as py_socket


LOG = logging.getLogger(__name__)

# sentinel objects
_STOP = object()


@contextlib.contextmanager
def _yield_before_after() -> Generator[None, None, None]:
    # Yield to any other co-routines...
    #
    # See: http://eventlet.net/doc/modules/greenthread.html
    # for how this zero sleep is really a cooperative yield to other potential
    # co-routines...
    eventlet.sleep(0)
    try:
        yield
    finally:
        eventlet.sleep(0)


class TimeoutError(Exception):
    pass


class AsyncResult(utils.AsyncResult):
    """A one-time event that stores a value or an exception"""

    def __init__(self, handler: Any):
        super(AsyncResult, self).__init__(
            handler, green_threading.Condition, TimeoutError
        )


class SequentialEventletHandler(object):
    """Eventlet handler for sequentially executing callbacks.

    This handler executes callbacks in a sequential manner. A queue is
    created for each of the callback events, so that each type of event
    has its callback type run sequentially. These are split into two
    queues, one for watch events and one for async result completion
    callbacks.

    Each queue type has a greenthread worker that pulls the callback event
    off the queue and runs it in the order the client sees it.

    This split helps ensure that watch callbacks won't block session
    re-establishment should the connection be lost during a Zookeeper
    client call.

    Watch and completion callbacks should avoid blocking behavior as
    the next callback of that type won't be run until it completes. If
    you need to block, spawn a new greenthread and return immediately so
    callbacks can proceed.

    .. note::

        Completion callbacks can block to wait on Zookeeper calls, but
        no other completion callbacks will execute until the callback
        returns.

    """

    name = "sequential_eventlet_handler"
    queue_impl = green_queue.LightQueue
    queue_empty = green_queue.Empty

    def __init__(self) -> None:
        """Create a :class:`SequentialEventletHandler` instance"""
        self.callback_queue = self.queue_impl()
        self.completion_queue = self.queue_impl()
        self._workers: list[
            tuple[eventlet.GreenThread, green_queue.LightQueue]
        ] = []
        self._started = False

    @staticmethod
    def sleep_func(wait: float) -> None:
        green_time.sleep(wait)

    @property
    def running(self) -> bool:
        return self._started

    timeout_exception = TimeoutError

    def _process_completion_queue(self) -> None:
        while True:
            cb = self.completion_queue.get()
            if cb is _STOP:
                break
            try:
                with _yield_before_after():
                    cb()
            except Exception:
                LOG.warning(
                    "Exception in worker completion queue greenlet",
                    exc_info=True,
                )
            finally:
                del cb  # release before possible idle

    def _process_callback_queue(self) -> None:
        while True:
            cb = self.callback_queue.get()
            if cb is _STOP:
                break
            try:
                with _yield_before_after():
                    cb()
            except Exception:
                LOG.warning(
                    "Exception in worker callback queue greenlet",
                    exc_info=True,
                )
            finally:
                del cb  # release before possible idle

    def start(self) -> None:
        if not self._started:
            # Spawn our worker threads, we have
            # - A callback worker for watch events to be called
            # - A completion worker for completion events to be called
            w = eventlet.spawn(self._process_completion_queue)
            self._workers.append((w, self.completion_queue))
            w = eventlet.spawn(self._process_callback_queue)
            self._workers.append((w, self.callback_queue))
            self._started = True
            atexit.register(self.stop)

    def stop(self) -> None:
        while self._workers:
            w, q = self._workers.pop()
            q.put(_STOP)
            w.wait()
        self._started = False
        atexit.unregister(self.stop)

    def socket(self, *args: Any, **kwargs: Any) -> py_socket.socket:
        return utils.create_tcp_socket(green_socket)

    def create_socket_pair(self) -> tuple:
        return utils.create_socket_pair(green_socket)

    def event_object(self) -> green_threading.Event:
        return green_threading.Event()

    def lock_object(self) -> green_threading.Lock:
        return green_threading.Lock()

    def rlock_object(self) -> green_threading.RLock:
        return green_threading.RLock()

    def create_connection(self, *args: Any, **kwargs: Any) -> py_socket.socket:
        return utils.create_tcp_connection(green_socket, *args, **kwargs)

    def select(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[int], list[int], list[int]]:
        with _yield_before_after():
            # Following appears to be a bug in mypy (see
            # https://github.com/python/mypy/issues/6799)
            return selector_select(
                *args,
                selectors_module=green_selectors,  # type: ignore[misc]
                **kwargs,
            )

    def async_result(self) -> AsyncResult:
        return AsyncResult(self)

    def spawn(
        self, func: Any, *args: Any, **kwargs: Any
    ) -> green_threading.Thread:
        t = green_threading.Thread(target=func, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t

    def dispatch_callback(self, callback: Any) -> None:
        self.callback_queue.put(lambda: callback.func(*callback.args))
