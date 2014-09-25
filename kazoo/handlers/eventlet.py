"""A eventlet based handler."""
from __future__ import absolute_import

import atexit
import logging

import eventlet
from eventlet.green import select as green_select
from eventlet.green import socket as green_socket
from eventlet.green import time as green_time
from eventlet.green import threading as green_threading
from eventlet import queue as green_queue

from kazoo.handlers import utils

LOG = logging.getLogger(__name__)

# sentinel objects
_STOP = object()


class TimeoutError(Exception):
    pass


class AsyncResult(utils.AsyncResult):
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

    def __init__(self):
        """Create a :class:`SequentialEventletHandler` instance"""
        self.callback_queue = green_queue.LightQueue()
        self.completion_queue = green_queue.LightQueue()
        self._workers = []
        self._started = False

    @staticmethod
    def sleep_func(wait):
        green_time.sleep(wait)

    @property
    def running(self):
        return self._started

    timeout_exception = TimeoutError

    def _process_completion_queue(self):
        while True:
            cb = self.completion_queue.get()
            if cb is _STOP:
                break
            LOG.debug("Completion queue calling %s", cb.func)
            try:
                cb.func(*cb.args)
            except Exception:
                LOG.warning("Exception in worker greenlet calling %s",
                            cb.func, exc_info=True)

    def _process_callback_queue(self):
        while True:
            cb = self.callback_queue.get()
            if cb is _STOP:
                break
            LOG.debug("Callback queue calling %s", cb.func)
            try:
                cb.func(*cb.args)
            except Exception:
                LOG.warning("Exception in worker greenlet calling %s",
                            cb.func, exc_info=True)

    def start(self):
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

    def stop(self):
        while self._workers:
            w, q = self._workers.pop()
            q.put(_STOP)
            w.wait()
        self._started = False
        if hasattr(atexit, "unregister"):
            atexit.unregister(self.stop)

    def socket(self, *args, **kwargs):
        return utils.create_tcp_socket(green_socket)

    def create_socket_pair(self):
        return green_socket.socketpair()

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
        eventlet.spawn_n(func, *args, **kwargs)

    def dispatch_callback(self, callback):
        self.callback_queue.put(callback)
