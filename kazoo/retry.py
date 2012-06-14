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

    def __init__(self, max_tries=1, ignore_expire=True):
        """Create a :class:`KazooRetry` instance

        :param max_tries: How many times to retry the command.
        :param ignore_expire: Whether a session expiration should be ignored
                              and treated as a retry-able command.

        """
        self.max_tries = max_tries
        self.retry_exceptions = self.RETRY_EXCEPTIONS
        if ignore_expire:
            self.retry_exceptions += self.EXPIRED_EXCEPTIONS

    def run(self, func, *args, **kwargs):
        self(func, *args, **kwargs)

    def __call__(self, func, *args, **kwargs):
        tries = 1

        while True:
            try:
                return func(*args, **kwargs)

            except self.retry_exceptions:
                if tries == self.max_tries:
                    raise
                tries += 1
