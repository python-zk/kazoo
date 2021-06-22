import asyncio
import threading

from kazoo.handlers.threading import AsyncResult, SequentialThreadingHandler


class AioAsyncResult(AsyncResult):
    def __init__(self, handler):
        self.future = handler.loop.create_future()
        AsyncResult.__init__(self, handler)

    def set(self, value=None):
        """
        The completion of the future has the same guarantees as the notification emitting of the condition.
        Provided that no callbacks raise it will complete.
        """
        AsyncResult.set(self, value)
        self._handler.loop.call_soon_threadsafe(self.future.set_result, value)

    def set_exception(self, exception):
        """
        The completion of the future has the same guarantees as the notification emitting of the condition.
        Provided that no callbacks raise it will complete.
        """
        AsyncResult.set_exception(self, exception)
        self._handler.loop.call_soon_threadsafe(self.future.set_exception, exception)


class AioSequentialThreadingHandler(SequentialThreadingHandler):
    def __init__(self):
        """
        Creating the handler must be done on the asyncio-loop's thread.
        """
        self.loop = asyncio.get_running_loop()
        self._aio_thread = threading.current_thread()
        SequentialThreadingHandler.__init__(self)

    def async_result(self):
        """
        Almost all async-result objects are created by a method that is invoked from the user's thead. The
        one exception I'm aware of is in the PatientChildrenWatch utility, that creates an async-result in
        its worker thread. Just because of that it is imperative to only create asyncio compatible results
        when the invoking code is from the loop's thread. There is no PEP/API guarantee that implementing
        the create_future() has to be thread-safe. The default is mostly thread-safe. The only thing that
        may get synchronization issue is a debug-feature for asyncio development. Quickly looking at the
        alternate implementation of uvloop, they use the default Future implementation, so no change there.
        For now, just to be safe, we check the current thread and create an async-result object based on the
        invoking thread's identity.
        """
        if threading.current_thread() is self._aio_thread:
            return AioAsyncResult(self)
        return AsyncResult(self)
