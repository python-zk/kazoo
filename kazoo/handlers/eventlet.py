"""A eventlet based handler."""
from __future__ import absolute_import

import atexit
import logging

import eventlet
from eventlet.green import select as green_select
from eventlet.green import socket as green_socket
from eventlet.green import threading as green_threading

from kazoo.handlers import utils

LOG = logging.getLogger(__name__)

# sentinel objects
_STOP = object()


class TimeoutError(Exception):
    pass


class AsyncResult(utils.BaseAsyncResult):
    """A one-time event that stores a value or an exception"""
    def __init__(self, handler):
        super(AsyncResult, self).__init__(handler,
                                          green_threading.Condition,
                                          TimeoutError)


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
    sleep_func = staticmethod(eventlet.sleep)

    def __init__(self):
        """Create a :class:`SequentialEventletHandler` instance"""
        self.callback_queue = eventlet.Queue()
        self.completion_queue = eventlet.Queue()
        self._lock = green_threading.Lock()
        self._workers = []
        self._started = False

    @property
    def running(self):
        with self._lock:
            return self._started

    timeout_exception = TimeoutError

    def start(self):
        def _process_queue(queue):
            while True:
                func = queue.get()
                if func is _STOP:
                    break
                try:
                    func()
                except Exception:
                    LOG.warning("Exception in worker greenlet", exc_info=True)

        with self._lock:
            if not self._started:
                # Spawn our worker threads, we have
                # - A callback worker for watch events to be called
                # - A completion worker for completion events to be called
                for q in (self.callback_queue, self.completion_queue):
                    w = eventlet.spawn(_process_queue, q)
                    self._workers.append((w, q))
                self._started = True
                atexit.register(self.stop)

    def stop(self):
        with self._lock:
            if self._started:
                while self._workers:
                    w, q = self._workers.pop()
                    q.put(_STOP)
                    w.wait()
                self.callback_queue = eventlet.Queue()
                self.completion_queue = eventlet.Queue()
                self._started = False
                if hasattr(atexit, "unregister"):
                    atexit.unregister(self.stop)

    def socket(self, *args, **kwargs):
        return utils.create_tcp_socket(green_socket)

    def event_object(self):
        return green_threading.Event()

    def lock_object(self):
        return green_threading.Lock()

    def rlock_object(self):
        return green_threading.RLock()

    def create_connection(self, *args, **kwargs):
        return utils.create_tcp_connection(green_socket, *args, **kwargs)

    def select(self, *args, **kwargs):
        return green_select.select(*args, **kwargs)

    def async_result(self):
        return AsyncResult(self)

    def spawn(self, func, *args, **kwargs):
        return eventlet.spawn(func, *args, **kwargs)

    def dispatch_callback(self, callback):
        self.callback_queue.put(lambda: callback.func(*callback.args))
