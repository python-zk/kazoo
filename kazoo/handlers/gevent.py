"""gevent Handler"""
from __future__ import absolute_import

import gevent
import gevent.event
import gevent.queue
from zope.interface import implementer

from kazoo.interfaces import IAsyncResult
from kazoo.interfaces import IHandler


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

    def set_exception(self, exception):
        # Proxy the set_exception call to the gevent thread
        self._handler.completion_queue.put(
            lambda: gevent.event.AsyncResult.set_exception(self, exception)
        )


@implementer(IHandler)
class SequentialGeventHandler(object):
    timeout_exception = gevent.event.Timeout

    def __init__(self):
        self.completion_queue = gevent.queue.Queue()
        self.callback_queue = gevent.queue.Queue()
        self.session_queue = gevent.queue.Queue()
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
                try:
                    func = queue.get(timeout=1)
                    print "Got a function, calling it"
                    func()
                except gevent.queue.Empty:
                    continue
        gevent.spawn(greenlet_worker)

    def async_result(self):
        return AsyncResult(self)

    def dispatch_callback(self, callback):
        if callback.type == 'session':
            self.session_queue.put(lambda: callback.func(*callback.args))
        else:
            self.callback_queue.put(lambda: callback.func(*callback.args))
