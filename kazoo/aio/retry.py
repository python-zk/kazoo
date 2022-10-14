import asyncio
import random
import time
from functools import partial

from kazoo.exceptions import (
    ConnectionClosedError,
    ConnectionLoss,
    OperationTimeoutError,
    SessionExpiredError,
)
from kazoo.retry import ForceRetryError, RetryFailedError


EXCEPTIONS = (
    ConnectionLoss,
    OperationTimeoutError,
    ForceRetryError,
)

EXCEPTIONS_WITH_EXPIRED = EXCEPTIONS + (SessionExpiredError,)


def kazoo_retry_aio(
    max_tries=1,
    delay=0.1,
    backoff=2,
    max_jitter=0.4,
    max_delay=60.0,
    ignore_expire=True,
    deadline=None,
):
    """
    This is similar to KazooRetry, but they do not have compatible
    interfaces. The threaded and asyncio constructs are too different
    to easily wrap the KazooRetry implementation. Unless, all retries
    always get their own thread to work in. This is much more lightweight
    compared to the object-copying and resetting implementation.

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

        aio_retry = kazoo_retry_aio()
        await aio_retry(zk.create_aio, "/x")
        await aio_retry(zk.create_aio, "/x/y")
    """
    retry_exceptions = (
        EXCEPTIONS_WITH_EXPIRED if ignore_expire else EXCEPTIONS
    )
    max_jitter = max(min(max_jitter, 1.0), 0.0)
    get_jitter = partial(random.uniform, 1.0 - max_jitter, 1.0 + max_jitter)
    del max_jitter

    async def _retry(func, *args, **kwargs):
        attempts = 0
        cur_delay = delay
        stop_time = (
            None if deadline is None else time.perf_counter() + deadline
        )
        while True:
            try:
                return await func(*args, **kwargs)
            except ConnectionClosedError:
                raise
            except retry_exceptions:
                # Note: max_tries == -1 means infinite tries.
                if attempts == max_tries:
                    raise RetryFailedError("Too many retry attempts")
                attempts += 1
                sleep_time = cur_delay * get_jitter()
                if (
                    stop_time is not None
                    and time.perf_counter() + sleep_time >= stop_time
                ):
                    raise RetryFailedError("Exceeded retry deadline")
                await asyncio.sleep(sleep_time)
                cur_delay = min(sleep_time * backoff, max_delay)

    return _retry
