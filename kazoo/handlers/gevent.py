"""A gevent based handler."""

from __future__ import annotations

import atexit
import logging

from typing import Any, Callable, Iterable, TYPE_CHECKING, cast
from typing_extensions import TypeAlias


from kazoo.handlers.utils import selector_select
from kazoo.handlers import utils
from kazoo.interfaces import IAsyncResult, IHandler, SpawnedFunc

import gevent
from gevent import socket
import gevent.event
import gevent.queue
import gevent.thread
import gevent.selectors
from gevent.lock import Semaphore, RLock

if TYPE_CHECKING:
    from kazoo.interfaces import (
        Event,
        FdLike,
        Lockable,
        ReentrantLock,
        Socket,
        Threadlike,
    )
    from kazoo.protocol.states import Callback

    from gevent import Greenlet

log = logging.getLogger(__name__)


_STOP = object()

AsyncResult = gevent.event.AsyncResult

Worker: TypeAlias = "gevent.Greenlet[..., Any]"
CallbackQueue: TypeAlias = "gevent.queue.Queue[Callable[..., None]]"


class SequentialGeventHandler(IHandler):
    """Gevent handler for sequentially executing callbacks.

    This handler executes callbacks in a sequential manner. A queue is
    created for each of the callback events, so that each type of event
    has its callback type run sequentially.

    Each queue type has a greenlet worker that pulls the callback event
    off the queue and runs it in the order the client sees it.

    This split helps ensure that watch callbacks won't block session
    re-establishment should the connection be lost during a Zookeeper
    client call.

    Watch callbacks should avoid blocking behavior as the next callback
    of that type won't be run until it completes. If you need to block,
    spawn a new greenlet and return immediately so callbacks can
    proceed.

    """

    name = "sequential_gevent_handler"
    queue_impl = gevent.queue.Queue  # type: ignore[assignment]
    queue_empty = gevent.queue.Empty
    sleep_func = staticmethod(gevent.sleep)  # type: ignore[assignment]

    def __init__(self) -> None:
        """Create a :class:`SequentialGeventHandler` instance"""
        self._callback_queue: CallbackQueue = self.queue_impl()

        def FakeQueue() -> CallbackQueue:
            """Raise NotImplementedError() on put().

            This is here because generally AyncResult is created with an
            IHandler, but we don't actually return a kazoo AsyncResult, we
            return a gevent one which behaves a little differently underneath.
            """

            class _FakeQueue:
                def put(self, item: Any) -> None:
                    raise NotImplementedError(
                        "FakeQueue does not support put()"
                    )

            return cast(CallbackQueue, _FakeQueue())

        self._completion_queue = FakeQueue()

        self._running = False
        self._async = None
        self._state_change = Semaphore()
        self._workers: list[Worker] = []

    @property
    def running(self) -> bool:
        return self._running

    class timeout_exception(gevent.Timeout):
        """Exception raised when a timeout occurs.

        Passing seconds=None to the constructor will create a timeout that
        never expires.
        """

        def __init__(self, msg: Any):
            super().__init__(exception=msg)

    def _create_greenlet_worker(self, queue: CallbackQueue) -> Worker:
        def greenlet_worker() -> None:
            while True:
                try:
                    func = queue.get()
                    try:
                        if func is _STOP:
                            break
                        func()
                    except Exception as exc:
                        log.warning("Exception in worker greenlet")
                        log.exception(exc)
                    finally:
                        del func  # release before possible idle
                except self.queue_empty:
                    continue

        return gevent.spawn(greenlet_worker)

    def start(self) -> None:
        """Start the greenlet workers."""
        with self._state_change:
            if self._running:
                return

            self._running = True

            # Spawn our worker greenlets, we have
            # - A callback worker for watch events to be called
            # FIXME Why the loop?
            for queue in (self._callback_queue,):
                w = self._create_greenlet_worker(queue)
                self._workers.append(w)
            atexit.register(self.stop)

    def stop(self) -> None:
        """Stop the greenlet workers and empty all queues."""
        with self._state_change:
            if not self._running:
                return

            self._running = False

            for queue in (self._callback_queue,):
                queue.put(cast("Callable[..., None]", _STOP))

            while self._workers:
                worker = self._workers.pop()
                worker.join()

            # Clear the queues
            self._callback_queue = self.queue_impl()  # pragma: nocover

            atexit.unregister(self.stop)

    def select(
        self, *args: Any, **kwargs: Any
    ) -> tuple[Iterable[FdLike], Iterable[FdLike], Iterable[FdLike]]:
        # FIXME use the correct arguments, not *args, *kwargs
        return selector_select(
            # Likely a bug in mypy (see
            # https://github.com/python/mypy/issues/6799)
            *args,
            selectors_module=gevent.selectors,
            **kwargs,  # type: ignore[misc]
        )

    def socket(self) -> Socket:
        return utils.create_tcp_socket(socket)

    def create_connection(self, *args: Any, **kwargs: Any) -> Socket:
        return utils.create_tcp_connection(socket, *args, **kwargs)

    def create_socket_pair(self) -> tuple[Socket, Socket]:
        return utils.create_socket_pair(socket)

    def event_object(self) -> Event:
        """Create an appropriate Event object"""
        return gevent.event.Event()

    def lock_object(self) -> Lockable:
        """Create an appropriate Lock object"""
        return cast(
            "Lockable",
            gevent.thread.allocate_lock(),  # type: ignore[no-untyped-call]
        )

    def rlock_object(self) -> ReentrantLock:
        """Create an appropriate RLock object"""
        return RLock()

    def async_result(self) -> IAsyncResult:
        """Create a :class:`AsyncResult` instance

        The :class:`AsyncResult` instance will have its completion
        callbacks executed in the thread the
        :class:`SequentialGeventHandler` is created in (which should be
        the gevent/main thread).

        """
        return AsyncResult()

    def spawn(self, func: Any, *args: Any, **kwargs: Any) -> Threadlike:
        """Spawn a function to run asynchronously"""
        return gevent.spawn(func, *args, **kwargs)

    def dispatch_callback(self, callback: Callback) -> None:
        """Dispatch to the callback object

        The callback is put on separate queues to run depending on the
        type as documented for the :class:`SequentialGeventHandler`.

        """
        self._callback_queue.put(lambda: callback.func(*callback.args))

    def add_to_completion_queue(self, callback: SpawnedFunc) -> None:
        """Add a method to callback on completion.

        :param callback: A :class:`~kazoo.protocol.states.Callback`
                         object to be called.

        The supplied method will be called whn an event completes.

        Note: This seems an odd place to store these.
        """
        raise NotImplementedError()
