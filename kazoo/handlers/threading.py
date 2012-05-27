"""Threading Handler"""
import Queue
import threading

from zope.interface import implementer

from kazoo.interfaces import IAsyncResult
from kazoo.interfaces import IHandler

# sentinal object
_NONE = object()


class TimeoutError(Exception):
    pass


@implementer(IAsyncResult)
class AsyncResult(object):
    """A one-time event that stores a value or an exception"""
    def __init__(self):
        self.value = None
        self._exception = _NONE
        self._condition = threading.Condition()

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
        """Store the value. Wake up the waiters.
        """
        with self._condition:
            self.value = value
            self._exception = None

            self._condition.notify_all()

    def set_exception(self, exception):
        """Store the exception. Wake up the waiters.
        """
        with self._condition:
            self._exception = exception

            self._condition.notify_all()

    def get(self, block=True, timeout=None):
        """Return the stored value or raise the exception.

        If there is no value raises Timeout
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


@implementer(IHandler)
class SequentialThreadingHandler(object):
    name = "sequential_threading_handler"
    timeout_exception = TimeoutError

    def __init__(self):
        self.callback_queue = Queue.Queue()
        self.session_queue = Queue.Queue()
        self._running = True

        # Spawn our worker threads, we have
        # - A callback worker for watch events to be called
        # - A session worker for session events to be called
        self._create_thread_worker(self.callback_queue)
        self._create_thread_worker(self.session_queue)

    def _create_thread_worker(self, queue):
        def thread_worker():
            while self._running:
                try:
                    func = queue.get(timeout=1)
                    func()
                except Queue.Empty:
                    continue
        thread = threading.Thread(target=thread_worker)
        thread.start()

    def async_result(self):
        return AsyncResult(self)

    def dispatch_callback(self, callback):
        if callback.type == 'session':
            self.session_queue.put(lambda: callback.func(*callback.args))
        else:
            self.callback_queue.put(lambda: callback.func(*callback.args))
