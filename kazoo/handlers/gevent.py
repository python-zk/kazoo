"""gevent Handler"""
from __future__ import absolute_import

import fcntl
import os

import gevent
import gevent.event
from gevent.queue import Empty
from gevent.queue import Queue
from zope.interface import implementer

from kazoo.interfaces import IAsyncResult
from kazoo.interfaces import IHandler


# Simple wrapper to os.pipe() - but sets to non-block
def _pipe():
    r, w = os.pipe()
    fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK)
    fcntl.fcntl(w, fcntl.F_SETFL, os.O_NONBLOCK)
    return r, w

_core_pipe_read, _core_pipe_write = _pipe()


def _core_pipe_read_callback(event, evtype):
    try:
        os.read(event.fd, 1)
    except:
        pass

gevent.core.event(gevent.core.EV_READ | gevent.core.EV_PERSIST,
                 _core_pipe_read, _core_pipe_read_callback).add()


@implementer(IAsyncResult)
class AsyncResult(gevent.event.AsyncResult):
    def __init__(self, handler):
        self._handler = handler
        gevent.event.AsyncResult.__init__(self)

    def set(self, value=None):
        # Proxy the set call to the gevent thread
        self._handler.completion_queue.put(
            lambda: gevent.event.AsyncResult.set(self, value)
        )

        # Wake gevent wait/gets
        os.write(_core_pipe_write, '\0')

    def set_exception(self, exception):
        # Proxy the set_exception call to the gevent thread
        self._handler.completion_queue.put(
            lambda: gevent.event.AsyncResult.set_exception(self, exception)
        )

        # Wake gevent wait/gets
        os.write(_core_pipe_write, '\0')


@implementer(IHandler)
class SequentialGeventHandler(object):
    timeout_exception = gevent.event.Timeout

    def __init__(self):
        self.completion_queue = Queue()
        self.callback_queue = Queue()
        self.session_queue = Queue()
        self._running = True

        # Spawn our worker greenlets, we have
        # - A completion worker for when values come back to be set on
        #   the AsyncResult object
        # - A callback worker for watch events to be called
        # - A session worker for session events to be called
        self._create_greenlet_worker(self.completion_queue)
        self._create_greenlet_worker(self.callback_queue)
        self._create_greenlet_worker(self.session_queue)

    def _create_greenlet_worker(self, queue):
        def greenlet_worker():
            while self._running:
                # We timeout after 1 and repeat so that we can gracefully
                # shutdown if self_running is set to false
                try:
                    func = queue.get(timeout=1)
                    func()
                except Empty:
                    continue
        gevent.spawn(greenlet_worker)

    def async_result(self):
        return AsyncResult(self)

    def dispatch_callback(self, callback):
        if callback.type == 'session':
            self.session_queue.put(lambda: callback.func(*callback.args))
        else:
            self.callback_queue.put(lambda: callback.func(*callback.args))

        # Wake gevent wait/gets
        os.write(_core_pipe_write, '\0')
