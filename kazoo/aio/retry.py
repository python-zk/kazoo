import asyncio
import random
import time

from kazoo.exceptions import (
    ConnectionClosedError,
    ConnectionLoss,
    OperationTimeoutError,
    SessionExpiredError,
)
from kazoo.retry import ForceRetryError, RetryFailedError


class AioKazooRetry(object):
    """
    This is similar to KazooRetry, but they do not have compatible
    interfaces. The threaded and asyncio constructs are too different
    to easily wrap the KazooRetry implementation. Unless, all retries
    always get their own thread to work in.

    There is no equivalent analogue to the interrupt API.
    If interrupting the retry is necessary, it must be wrapped in
    an asyncio.Task, which can be cancelled. Be aware though that
    this will quit waiting on the Zookeeper API call immediately
    unlike the threaded API. There is no way to interrupt/cancel an
    internal request thread so it will continue and stop eventually
    on its own. This means caller can't know if the call is still
    in progress and may succeed or the retry was cancelled while it
    was waiting for delay.

    Usage example. These are equivalent except that the latter lines
    will retry the requests on specific exceptions:
        await zk.create_aio("/x")
        await zk.create_aio("/x/y")

        aio_retry = AioKazooRetry()
        await aio_retry(zk.create_aio, "/x")
        await aio_retry(zk.create_aio, "/x/y")

    Re-using an instance is fine as long as it is done serially.
    """

    EXCEPTIONS = (
        ConnectionLoss,
        OperationTimeoutError,
        ForceRetryError,
    )

    EXCEPTIONS_WITH_EXPIRED = EXCEPTIONS + (SessionExpiredError,)

    def __init__(
        self,
        max_tries=1,
        delay=0.1,
        backoff=2,
        max_jitter=0.4,
        max_delay=60.0,
        ignore_expire=True,
        deadline=None,
    ):
        self.max_tries = max_tries
        self.delay = delay
        self.backoff = backoff
        self.max_jitter = max(min(max_jitter, 1.0), 0.0)
        self.max_delay = float(max_delay)
        self._attempts = 0
        self._cur_delay = delay
        self.deadline = deadline
        self.retry_exceptions = (
            self.EXCEPTIONS_WITH_EXPIRED if ignore_expire else self.EXCEPTIONS
        )

    def reset(self):
        self._attempts = 0
        self._cur_delay = self.delay

    def copy(self):
        obj = AioKazooRetry(
            max_tries=self.max_tries,
            delay=self.delay,
            backoff=self.backoff,
            max_jitter=self.max_jitter,
            max_delay=self.max_delay,
            deadline=self.deadline,
        )
        obj.retry_exceptions = self.retry_exceptions
        return obj

    async def __call__(self, func, *args, **kwargs):
        self.reset()

        stop_time = (
            None
            if self.deadline is None
            else time.perf_counter() + self.deadline
        )
        while True:
            try:
                return await func(*args, **kwargs)
            except ConnectionClosedError:
                raise
            except self.retry_exceptions:
                # Note: max_tries == -1 means infinite tries.
                if self._attempts == self.max_tries:
                    raise RetryFailedError("Too many retry attempts")
                self._attempts += 1
                jitter = random.uniform(
                    1.0 - self.max_jitter, 1.0 + self.max_jitter
                )
                sleeptime = self._cur_delay * jitter
                if (
                    stop_time is not None
                    and time.perf_counter() + sleeptime >= stop_time
                ):
                    raise RetryFailedError("Exceeded retry deadline")
                await asyncio.sleep(sleeptime)
                self._cur_delay = min(
                    sleeptime * self.backoff, self.max_delay
                )
