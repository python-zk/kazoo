"""A gevent based handler."""
from __future__ import absolute_import

import atexit
import logging

import gevent
import gevent.queue
import gevent.coros
import gevent.event
import gevent.thread
import gevent.select

from gevent.queue import Empty
from gevent.queue import Queue
from gevent import socket
from zope.interface import implementer

from kazoo.interfaces import IAsyncResult
from kazoo.interfaces import IHandler


if gevent.__version__.startswith('1.'):
    _using_libevent = False
else:
    _using_libevent = True


log = logging.getLogger(__name__)

_STOP = object()


if _using_libevent:
    from gevent.timeout import Timeout
    from gevent.hub import Waiter, getcurrent

    # No peek method on queue in 0.13, so add one from gevent 1.0
    class _PeekableQueue(gevent.queue.Queue):
        def _peek(self):
            return self.queue[0]

        def peek(self, block=True, timeout=None):
            if self.qsize():
                return self._peek()
            elif self.hub is getcurrent():
                # special case to make peek(False) runnable in the mainloop
                # greenlet there are no items in the queue; try to fix the
                # situation by unlocking putters
                while self.putters:
                    self.putters.pop().put_and_switch()
                    if self.qsize():
                        return self._peek()
                raise Empty
            elif block:
                waiter = Waiter()
                timeout = Timeout.start_new(timeout, Empty)
                try:
                    self.getters.add(waiter)
                    if self.putters:
                        self._schedule_unlock()
                    result = waiter.get()
                    assert result is waiter, 'Invalid switch into Queue.peek: %r' % (result, )
                    return self._peek()
                finally:
                    self.getters.discard(waiter)
                    timeout.cancel()
            else:
                raise Empty

        def peek_nowait(self):
            return self.peek(False)
else:
    _PeekableQueue = gevent.queue.Queue


AsyncResult = implementer(IAsyncResult)(gevent.event.AsyncResult)


@implementer(IHandler)
class SequentialGeventHandler(object):
    """Gevent handler for sequentially executing callbacks.

    This handler executes callbacks in a sequential manner.
    A queue is created for each of the callback events, so that each
    type of event has its callback type run sequentially. These are split into
    three queues, therefore it's possible that a session event arriving after a
    watch event may have its callback executed at the same time or slightly
    before the watch event callback.

    Each queue type has a greenlet worker that pulls the callback event off the
    queue and runs it in the order Zookeeper sent it.

    This split helps ensure that watch callbacks won't block session
    re-establishment should the connection be lost during a Zookeeper client
    call.

    Watch callbacks and session callbacks should avoid blocking behavior as the
    next callback of that type won't be run until it completes. If you need
    to block, spawn a new greenlet and return immediately so callbacks can
    proceed.

    """
    name = "sequential_gevent_handler"
    sleep_func = staticmethod(gevent.sleep)
    empty = staticmethod(gevent.queue.Empty)

    def __init__(self):
        """Create a :class:`SequentialGeventHandler` instance"""
        self.completion_queue = Queue()
        self.callback_queue = Queue()
        self.session_queue = Queue()
        self._running = False
        self._async = None
        self._state_change = gevent.coros.Semaphore()
        self._workers = []
        atexit.register(self.stop)

    class timeout_exception(gevent.event.Timeout):
        def __init__(self, msg):
            gevent.event.Timeout.__init__(self, exception=msg)

    def _create_greenlet_worker(self, queue):
        def greenlet_worker():
            while True:
                try:
                    func = queue.get()
                    if func is _STOP:
                        break
                    func()
                except Empty:
                    continue
                except Exception as exc:
                    log.warning("Exception in worker greenlet")
                    log.exception(exc)
        return gevent.spawn(greenlet_worker)

    def start(self):
        """Start the greenlet workers."""
        with self._state_change:
            if self._running:
                return

            self._running = True

            # Spawn our worker greenlets, we have
            # - A completion worker for when values come back to be set on
            #   the AsyncResult object
            # - A callback worker for watch events to be called
            # - A session worker for session events to be called
            for queue in (self.completion_queue, self.callback_queue,
                          self.session_queue):
                w = self._create_greenlet_worker(queue)
                self._workers.append(w)

    def stop(self):
        """Stop the greenlet workers and empty all queues."""
        with self._state_change:
            if not self._running:
                return

            self._running = False

            for queue in (self.completion_queue, self.callback_queue,
                          self.session_queue):
                queue.put(_STOP)

            while self._workers:
                worker = self._workers.pop()
                worker.join()

            # Clear the queues
            self.completion_queue = Queue()
            self.callback_queue = Queue()
            self.session_queue = Queue()

    def select(self, *args, **kwargs):
        return gevent.select.select(*args, **kwargs)

    def socket(self, *args, **kwargs):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock

    def peekable_queue(self, *args, **kwargs):
        return _PeekableQueue(*args, **kwargs)

    def event_object(self):
        """Create an appropriate Event object"""
        return gevent.event.Event()

    def lock_object(self):
        """Create an appropriate Lock object"""
        return gevent.thread.allocate_lock()

    def rlock_object(self):
        """Create an appropriate RLock object"""
        return gevent.coros.RLock()

    def async_result(self):
        """Create a :class:`AsyncResult` instance

        The :class:`AsyncResult` instance will have its completion
        callbacks executed in the thread the :class:`SequentialGeventHandler`
        is created in (which should be the gevent/main thread).

        """
        return AsyncResult()

    def spawn(self, func, *args, **kwargs):
        """Spawn a function to run asynchronously"""
        gevent.spawn(func, *args, **kwargs)

    def dispatch_callback(self, callback):
        """Dispatch to the callback object

        The callback is put on separate queues to run depending on the type
        as documented for the :class:`SequentialGeventHandler`.

        """
        if callback.type == 'session':
            self.session_queue.put(lambda: callback.func(*callback.args))
        else:
            self.callback_queue.put(lambda: callback.func(*callback.args))
