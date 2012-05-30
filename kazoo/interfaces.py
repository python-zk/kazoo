"""Kazoo Interfaces"""
from zope.interface import (
    Attribute,
    Interface,
)

# public API


class IHandler(Interface):
    """A Callback Handler for Zookeeper completion and watcher callbacks

    This object must implement several methods responsible for determining
    how completion / watch callbacks are handled as well as the method for
    calling :class:`IAsyncResult` callback functions.

    The Handler should document how callbacks are called for:

    * :class:`IAsyncResult` callbacks registered via
      :meth:`~IAsyncResult.rawlink`
    * :class:`~kazoo.client.Callback` objects dispatched via
      :meth:`dispatch_callback`, including whether session callbacks
      are handled differently than watcher callbacks

    """
    name = Attribute(
        """Human readable name of the Handler interface""")

    timeout_exception = Attribute(
        """Exception class that should be thrown and captured if a result
        is not available within the given time""")

    def async_result():
        """Return an instance that conforms to the :class:`~IAsyncResult`
        interface appropriate for this handler"""

    def dispatch_callback(callback):
        """Dispatch to the callback object

        :param callback: A :class:`~kazoo.client.Callback` object to be
                         called

        """


class IAsyncResult(Interface):
    """An Async Result object that can be queried for a value that has been
    set asyncronously

    This object is modeled on the ``gevent`` AsyncResult object.

    The implementation must account for the fact that the :meth:`set` and
    :meth:`set_exception` methods will be called from within the Zookeeper
    thread which may require extra care under asynchronous environments.

    """
    value = Attribute(
        """Holds the value passed to :meth:`set` if :meth:`set` was called.
        Otherwise `None`""")

    exception = Attribute(
        """Holds the exception instance passed to :meth:`set_exception` if
        :meth:`set_exception` was called. Otherwise `None`""")

    def ready():
        """Return `True` if and only if it holds a value or an exception"""

    def successful():
        """Return `True` if and only if it is ready and holds a value"""

    def set(value=None):
        """Store the value. Wake up the waiters.

        Any waiters blocking on :meth:`get` or :meth:`wait` are woken up.
        Sequential calls to :meth:`wait` and :meth:`get` will not block at
        all."""

    def set_exception(exception):
        """Store the exception. Wake up the waiters.

        Any waiters blocking on :meth:`get` or :meth:`wait` are woken up.
        Sequential calls to :meth:`wait` and :meth:`get` will not block at
        all."""

    def get(block=True, timeout=None):
        """Return the stored value or raise the exception

        If this instance already holds a value / an exception, return / raise
        it immediately. Otherwise, block until :meth:`set` or
        :meth:`set_exception` has been called or until the optional timeout
        occurs.

        When the `timeout` argument is present and not `None`, it should be a
        float specifying a timeout for the operation in seconds (or fractions
        thereof)."""

    def get_nowait():
        """Return the value or raise the exception without blocking.

        If nothing is available, raise the Timeout exception class on the
        associated :class:`IHandler` interface."""

    def wait(timeout=None):
        """Block until the instance is ready.

        If this instance already holds a value / an exception, return / raise
        it immediately. Otherwise, block until :meth:`set` or
        :meth:`set_exception` has been called or until the optional timeout
        occurs.

        When the `timeout` argument is present and not `None`, it should be a
        float specifying a timeout for the operation in seconds (or fractions
        thereof)."""

    def rawlink(callback):
        """Register a callback to call when a value or an exception is set

        ``callback`` will be called per the calling system of the associated
        :class:`IHandler` interface. ``callback`` will be passed one argument:
        this instance."""

    def unlink(callback):
        """Remove the callback set by :meth:`rawlink`"""
