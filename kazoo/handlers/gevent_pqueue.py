"""Implement a peekable queue for gevent 0.13"""
from __future__ import absolute_import

import sys

from gevent.timeout import Timeout
from gevent.hub import Waiter, _NONE
from gevent.queue import (
    Empty,
    Full,
    ItemWaiter,
    Queue
)


# No peek method on queue in 0.13, so add one from gevent 1.0
class PeekableQueue(Queue):
    def put(self, item, block=True, timeout=None):
        """Put an item into the queue.

        If optional arg *block* is true and *timeout* is ``None`` (the default),
        block if necessary until a free slot is available. If *timeout* is
        a positive number, it blocks at most *timeout* seconds and raises
        the :class:`Full` exception if no free slot was available within that time.
        Otherwise (*block* is false), put an item on the queue if a free slot
        is immediately available, else raise the :class:`Full` exception (*timeout*
        is ignored in that case).
        """
        if self.maxsize is None or self.qsize() < self.maxsize:
            # there's a free slot, put an item right away
            self._put(item)
            if self.getters:
                self._schedule_unlock()
        elif block:
            waiter = ItemWaiter(item)
            self.putters.add(waiter)
            timeout = Timeout.start_new(timeout, Full)
            try:
                if self.getters:
                    self._schedule_unlock()
                result = waiter.get()
                assert result is waiter, "Invalid switch into Queue.put: %r" % (result, )
                if waiter.item is not _NONE:
                    self._put(item)
            finally:
                timeout.cancel()
                self.putters.discard(waiter)
        else:
            raise Full

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args *block* is true and *timeout* is ``None`` (the default),
        block if necessary until an item is available. If *timeout* is a positive number,
        it blocks at most *timeout* seconds and raises the :class:`Empty` exception
        if no item was available within that time. Otherwise (*block* is false), return
        an item if one is immediately available, else raise the :class:`Empty` exception
        (*timeout* is ignored in that case).
        """
        if self.qsize():
            if self.putters:
                self._schedule_unlock()
            return self._get()
        elif block:
            waiter = Waiter()
            timeout = Timeout.start_new(timeout, Empty)
            try:
                self.getters.add(waiter)
                if self.putters:
                    self._schedule_unlock()
                result = waiter.get()
                assert result is waiter, 'Invalid switch into Queue.get: %r' % (result, )
                return self._get()
            finally:
                self.getters.discard(waiter)
                timeout.cancel()
        else:
            raise Empty

    def _peek(self):
        return self.queue[0]

    def peek(self, block=True, timeout=None):
        if self.qsize():
            if self.putters:
                self._schedule_unlock()
            return self._peek()
        elif block:
            waiter = Waiter()
            timeout = Timeout.start_new(timeout, Empty)
            try:
                self.getters.add(waiter)
                if self.putters:
                    self._schedule_unlock()
                result = waiter.get()
                assert result is waiter, "Invalid switch into Queue.put: %r" % (result, )
                return self._peek()
            finally:
                self.getters.discard(waiter)
                timeout.cancel()
        else:
            raise Empty

    def peek_nowait(self):
        return self.peek(False)

    def _unlock(self):
        while True:
            repeat = False
            if self.putters and (self.maxsize is None or self.qsize() < self.maxsize):
                repeat = True
                try:
                    putter = self.putters.pop()
                    self._put(putter.item)
                except:
                    putter.throw(*sys.exc_info())
                else:
                    putter.switch(putter)
            if self.getters and self.qsize():
                repeat = True
                getter = self.getters.pop()
                getter.switch(getter)
            if not repeat:
                return
