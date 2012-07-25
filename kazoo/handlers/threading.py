"""Threading Handler

The :class:`SequentialThreadingHandler` is intended for regular Python
environments that use threads.

.. warning::

    Do not use :class:`SequentialThreadingHandler` with applications using
    asynchronous event loops (like gevent). Use the
    :class:`~kazoo.handlers.gevent.SequentialGeventHandler` instead.

"""
from __future__ import absolute_import

import Queue
import time
import threading

from zope.interface import implementer

from kazoo.handlers.util import thread
from kazoo.interfaces import IAsyncResult
from kazoo.interfaces import IHandler

# sentinal object
_NONE = object()


class TimeoutError(Exception):
    pass


@implementer(IAsyncResult)
class AsyncResult(object):
    """A one-time event that stores a value or an exception"""
    def __init__(self, handler):
        self._handler = handler
        self.value = None
        self._exception = _NONE
        self._condition = threading.Condition()
        self._callbacks = []

    def ready(self):
        """Return true if and only if it holds a value or an exception"""
        return self._exception is not _NONE

    def successful(self):
        """Return true if and only if it is ready and holds a value"""
        return self._exception is None

    @property
    def exception(self):
        if self._exception is not _NONE:
            return self._exception

    def set(self, value=None):
        """Store the value. Wake up the waiters."""
        with self._condition:
            self.value = value
            self._exception = None

            for callback in self._callbacks:
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
            self._condition.notify_all()

    def set_exception(self, exception):
        """Store the exception. Wake up the waiters."""
        with self._condition:
            self._exception = exception

            for callback in self._callbacks:
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
            self._condition.notify_all()

    def get(self, block=True, timeout=None):
        """Return the stored value or raise the exception.

        If there is no value raises TimeoutError.

        """
        with self._condition:
            if self._exception is not _NONE:
                if self._exception is None:
                    return self.value
                raise self._exception
            elif block:
                self._condition.wait(timeout)
                if self._exception is not _NONE:
                    if self._exception is None:
                        return self.value
                    raise self._exception

            # if we get to this point we timeout
            raise TimeoutError()

    def get_nowait(self):
        """Return the value or raise the exception without blocking.

        If nothing is available, raises TimeoutError

        """
        return self.get(block=False)

    def rawlink(self, callback):
        """Register a callback to call when a value or an exception is set"""
        with self._condition:
            # Are we already set? Dispatch it now
            if self.ready():
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
                return

            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def unlink(self, callback):
        """Remove the callback set by :meth:`rawlink`"""
        with self._condition:
            if self.ready():
                # Already triggered, ignore
                return

            if callback in self._callbacks:
                self._callbacks.remove(callback)


@implementer(IHandler)
class SequentialThreadingHandler(object):
    """threading Handler for sequentially executing callbacks

    This handler executes callbacks in a sequential manner from the Zookeeper
    thread. A queue is created for each of the callback events, so that each
    type of event has its callback type run sequentially. These are split into
    three queues, therefore it's possible that a session event arriving after a
    watch event may have its callback executed at the same time or slightly
    before the watch event callback.

    Each queue type has a thread worker that pulls the callback event off the
    queue and runs it in the order Zookeeper sent it.

    This split helps ensure that watch callbacks won't block session
    re-establishment should the connection be lost during a Zookeeper client
    call.

    Watch, session, and completion callbacks should avoid blocking behavior as
    the next callback of that type won't be run until it completes. If you need
    to block, spawn a new thread and return immediately so callbacks can
    proceed.

    .. note::

        Completion callbacks can block to wait on Zookeeper calls, but no
        other completion callbacks will execute until the callback returns.

    """
    name = "sequential_threading_handler"
    timeout_exception = TimeoutError
    sleep_func = time.sleep

    def __init__(self):
        """Create a :class:`SequentialThreadingHandler` instance"""
        self.callback_queue = Queue.Queue()
        self.session_queue = Queue.Queue()
        self.completion_queue = Queue.Queue()
        self._running = True

        # Spawn our worker threads, we have
        # - A callback worker for watch events to be called
        # - A session worker for session events to be called
        # - A completion worker for completion events to be called
        self._create_thread_worker(self.callback_queue)
        self._create_thread_worker(self.session_queue)
        self._create_thread_worker(self.completion_queue)

    def _create_thread_worker(self, queue):
        @thread
        def thread_worker():  # pragma: nocover
            while self._running:
                try:
                    func = queue.get(timeout=1)
                    func()
                except Queue.Empty:
                    continue

    def event_object(self):
        """Create an appropriate Event object"""
        return threading.Event()

    def lock_object(self):
        """Create a lock object"""
        return threading.Lock()

    def async_result(self):
        """Create a :class:`AsyncResult` instance"""
        return AsyncResult(self)

    def spawn(self, func, *args, **kwargs):
        t = threading.Thread(target=func, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()

    def dispatch_callback(self, callback):
        """Dispatch to the callback object

        The callback is put on separate queues to run depending on the type
        as documented for the :class:`SequentialThreadingHandler`.

        """
        if callback.type == 'session':
            self.session_queue.put(lambda: callback.func(*callback.args))
        else:
            self.callback_queue.put(lambda: callback.func(*callback.args))
