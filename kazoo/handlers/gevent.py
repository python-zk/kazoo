"""A gevent based handler."""
from __future__ import absolute_import

import logging

import gevent
from gevent import socket
import gevent.event
import gevent.queue
import gevent.select
import gevent.thread
try:
    from gevent.lock import Semaphore, RLock
except ImportError:
    from gevent.coros import Semaphore, RLock

from kazoo.handlers import utils
from kazoo import python2atexit

import six
from collections import defaultdict
from itertools import chain

_using_libevent = gevent.__version__.startswith('0.')

log = logging.getLogger(__name__)

_STOP = object()

AsyncResult = gevent.event.AsyncResult


def _to_fileno(obj):
    if isinstance(obj, six.integer_types):
        fd = int(obj)
    elif hasattr(obj, "fileno"):
        fd = obj.fileno()
        if not isinstance(fd, six.integer_types):
            raise TypeError("fileno() returned a non-integer")
        fd = int(fd)
    else:
        raise TypeError("argument must be an int, or have a fileno() method.")

    if fd < 0:
        raise ValueError(
            "file descriptor cannot be a negative integer (%d)" % (fd,)
        )

    return fd

class SequentialGeventHandler(object):
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
    queue_impl = gevent.queue.Queue
    queue_empty = gevent.queue.Empty
    sleep_func = staticmethod(gevent.sleep)

    def __init__(self):
        """Create a :class:`SequentialGeventHandler` instance"""
        self.callback_queue = self.queue_impl()
        self._running = False
        self._async = None
        self._state_change = Semaphore()
        self._workers = []

    @property
    def running(self):
        return self._running

    class timeout_exception(gevent.Timeout):
        def __init__(self, msg):
            gevent.Timeout.__init__(self, exception=msg)

    def _create_greenlet_worker(self, queue):
        def greenlet_worker():
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

    def start(self):
        """Start the greenlet workers."""
        with self._state_change:
            if self._running:
                return

            self._running = True

            # Spawn our worker greenlets, we have
            # - A callback worker for watch events to be called
            for queue in (self.callback_queue,):
                w = self._create_greenlet_worker(queue)
                self._workers.append(w)
            python2atexit.register(self.stop)

    def stop(self):
        """Stop the greenlet workers and empty all queues."""
        with self._state_change:
            if not self._running:
                return

            self._running = False

            for queue in (self.callback_queue,):
                queue.put(_STOP)

            while self._workers:
                worker = self._workers.pop()
                worker.join()

            # Clear the queues
            self.callback_queue = self.queue_impl()  # pragma: nocover

            python2atexit.unregister(self.stop)

    def select(self, *args, **kwargs):
        # if the highest fd we've seen is > 1023, use a poll-based "select".
        if max(map(_to_fileno, chain(*args[:3]))) > 1023:
            return self._poll_select(*args, **kwargs)
        return self._select(*args, **kwargs)

    def _select(self, *args, **kwargs):
        return gevent.select.select(*args, **kwargs)

    def _poll_select(self, rlist, wlist, xlist, timeout=None):
        """poll-based drop-in replacement for select to overcome select
        limitation on a maximum filehandle value
        """
        if timeout is not None and timeout > 0:
            timeout *= 1000
        eventmasks = defaultdict(int)
        rfd2obj = defaultdict(list)
        wfd2obj = defaultdict(list)
        xfd2obj = defaultdict(list)

        def store_evmasks(obj_list, evmask, fd2obj):
            for obj in obj_list:
                fileno = _to_fileno(obj)
                eventmasks[fileno] |= evmask
                fd2obj[fileno].append(obj)

        store_evmasks(rlist, gevent.select.POLLIN, rfd2obj)
        store_evmasks(wlist, gevent.select.POLLOUT, wfd2obj)
        store_evmasks(xlist, gevent.select.POLLNVAL, xfd2obj)

        poller = gevent.select.poll()

        for fileno in eventmasks:
            poller.register(fileno, eventmasks[fileno])

        try:
            events = poller.poll(timeout)
            revents = []
            wevents = []
            xevents = []
            for fileno, event in events:
                if event & gevent.select.POLLIN:
                    revents += rfd2obj.get(fileno, [])
                if event & gevent.select.POLLOUT:
                    wevents += wfd2obj.get(fileno, [])
                if event & gevent.select.POLLNVAL:
                    xevents += xfd2obj.get(fileno, [])
        finally:
            for fileno in eventmasks:
                poller.unregister(fileno)

        return revents, wevents, xevents

    def socket(self, *args, **kwargs):
        return utils.create_tcp_socket(socket)

    def create_connection(self, *args, **kwargs):
        return utils.create_tcp_connection(socket, *args, **kwargs)

    def create_socket_pair(self):
        return utils.create_socket_pair(socket)

    def event_object(self):
        """Create an appropriate Event object"""
        return gevent.event.Event()

    def lock_object(self):
        """Create an appropriate Lock object"""
        return gevent.thread.allocate_lock()

    def rlock_object(self):
        """Create an appropriate RLock object"""
        return RLock()

    def async_result(self):
        """Create a :class:`AsyncResult` instance

        The :class:`AsyncResult` instance will have its completion
        callbacks executed in the thread the
        :class:`SequentialGeventHandler` is created in (which should be
        the gevent/main thread).

        """
        return AsyncResult()

    def spawn(self, func, *args, **kwargs):
        """Spawn a function to run asynchronously"""
        return gevent.spawn(func, *args, **kwargs)

    def dispatch_callback(self, callback):
        """Dispatch to the callback object

        The callback is put on separate queues to run depending on the
        type as documented for the :class:`SequentialGeventHandler`.

        """
        self.callback_queue.put(lambda: callback.func(*callback.args))
