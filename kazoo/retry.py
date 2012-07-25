import random
import time

from zookeeper import (
    ClosingException,
    ConnectionLossException,
    InvalidStateException,
    OperationTimeoutException,
    SessionExpiredException
)


class ForceRetryError(Exception):
    """Raised when some recipe logic wants to force a retry"""


class KazooRetry(object):
    """Helper for retrying a method in the face of retry-able exceptions"""
    RETRY_EXCEPTIONS = (
        ClosingException,
        ConnectionLossException,
        OperationTimeoutException,
        ForceRetryError
    )

    EXPIRED_EXCEPTIONS = (
        SessionExpiredException,

        # Occurs when a command is run on a session handle that expired, if it
        # manages to run exactly when it expired but before the handle was
        # removed for reconnection
        InvalidStateException,
    )

    def __init__(self, max_tries=1, delay=0.1, backoff=2, max_jitter=0.8,
                 ignore_expire=True, sleep_func=time.sleep):
        """Create a :class:`KazooRetry` instance

        :param max_tries: How many times to retry the command.
        :param delay: Initial delay between retry attempts
        :param backoff: Backoff multiplier between retry attempts. Defaults
                        to 2 for exponential backoff.
        :param max_jitter: Additional max jitter period to wait between retry
                           attempts to avoid slamming the server.
        :param ignore_expire: Whether a session expiration should be ignored
                              and treated as a retry-able command.

        """
        self.sleep_func = sleep_func
        self.max_tries = max_tries
        self.delay = delay
        self.backoff = backoff
        self.max_jitter = int(max_jitter * 100)
        self.retry_exceptions = self.RETRY_EXCEPTIONS
        if ignore_expire:
            self.retry_exceptions += self.EXPIRED_EXCEPTIONS

    def run(self, func, *args, **kwargs):
        self(func, *args, **kwargs)

    def __call__(self, func, *args, **kwargs):
        tries = 1
        delay = self.delay

        while True:
            try:
                return func(*args, **kwargs)

            except self.retry_exceptions:
                if tries == self.max_tries:
                    raise
                tries += 1
                jitter = random.randint(0, self.max_jitter) / 100.0
                self.sleep_func(delay + jitter)
                delay *= self.backoff
