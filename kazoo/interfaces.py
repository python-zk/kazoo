"""Kazoo Interfaces

.. versionchanged:: 1.4

    The classes in this module used to be interface declarations based on
    `zope.interface.Interface`. They were converted to normal classes and
    now serve as documentation only.

"""

from __future__ import annotations

import abc
import queue

from typing import (
    Any,
    Callable,
    Iterable,
    Protocol,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from kazoo.protocol.states import Callback

# public API


class HasFileNo(Protocol):
    """Protocol for things like select"""

    def fileno(self) -> int:
        ...


class Socket(HasFileNo, Protocol):
    """This is for things that provide a socket.socket-like interface.

    This is required because:
    1. The socket in gevent doesn't inherit from socket.socket
    2. mypy gets confused if you have a method called socket and
       subsequently attempt to use socket or socket.socket as a return type
    """

    def close(self) -> None:
        ...

    def fileno(self) -> int:
        ...

    def recv(self, bufsize: int, flags: int = 0) -> bytes:
        ...

    def send(self, data: bytes | memoryview, flags: int = 0) -> int:
        ...

    def sendall(self, data: bytes, flags: int = 0) -> None:
        ...

    def setblocking(self, flags: bool) -> None:
        ...

    def setsockopt(self, level: int, optname: int, value: int) -> None:
        ...


class Lockable(Protocol):
    """This is what threading.Lock implements.

    In python 3.9+ it's available natively. Though given it has some
    very odd typing, I wouldn't put money on it.
    """

    def __enter__(self) -> None:
        ...

    def __exit__(self, x: Any, y: Any, z: Any) -> None:
        ...

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        ...

    def release(self) -> int | None:
        """The gevent release returns an int..."""
        ...

    def locked(self) -> bool:
        ...


class ReentrantLock(Protocol):
    """This is what threading.RLock implements.

    In python 3.14+, it's the same as Lock, which adds to the fun.
    """

    def __enter__(self) -> None:
        ...

    def __exit__(self, x: Any, y: Any, z: Any) -> None:
        ...

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        ...

    def release(self) -> None:
        ...


class Event(Protocol):
    """Protocol for threading.Event"""

    def is_set(self) -> bool:
        ...

    def set(self) -> None:
        ...

    def clear(self) -> None:
        ...

    def wait(self, timeout: float | None = None) -> bool:
        ...


SpawnedFunc = Callable[..., None]


class IHandler(abc.ABC):
    """A Callback Handler for Zookeeper completion and watch callbacks.

    This object must implement several methods responsible for
    determining how completion / watch callbacks are handled as well as
    the method for calling :class:`IAsyncResult` callback functions.

    These functions are used to abstract differences between a Python
    threading environment and asynchronous single-threaded environments
    like gevent. The minimum functionality needed for Kazoo to handle
    these differences is encompassed in this interface.

    The Handler should document how callbacks are called for:

    * Zookeeper completion events
    * Zookeeper watch events

    .. attribute:: name

        Human readable name of the Handler interface.

    .. attribute:: timeout_exception

        Exception class that should be thrown and captured if a
        result is not available within the given time.

    .. attribute:: sleep_func

        Appropriate sleep function that can be called with a single
        argument and sleep.

    """

    timeout_exception: type[Exception] = None  # type: ignore[assignment]
    sleep_func: staticmethod[[float], None] = None  # type: ignore[assignment]
    queue_impl: type[queue.Queue[Any]] = None  # type: ignore[assignment]

    @abc.abstractmethod
    def start(self) -> None:
        """Start the handler, used for setting up the handler."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop the handler. Should block until the handler is safely
        stopped."""

    @abc.abstractmethod
    def select(
        self,
        rlist: Iterable[int | HasFileNo],
        wlist: Iterable[int | HasFileNo],
        xlist: Iterable[int | HasFileNo],
        timeout: float | None = None,
    ) -> tuple[
        Iterable[int | HasFileNo],
        Iterable[int | HasFileNo],
        Iterable[int | HasFileNo],
    ]:
        """A select method that implements Python's select.select
        API"""

    @abc.abstractmethod
    def socket(self) -> Socket:
        """A socket method that implements Python's socket.socket API"""

    @abc.abstractmethod
    def create_connection(self, *args: Any, **kwargs: Any) -> Socket:
        """A socket method that implements Python's socket.create_connection
        API"""

    @abc.abstractmethod
    def create_socket_pair(self) -> tuple[Socket, Socket]:
        """A socket method that implements Python's socket.socketpair API"""

    @abc.abstractmethod
    def event_object(self) -> Event:
        """Return an appropriate object that implements Python's
        threading.Event API"""

    @abc.abstractmethod
    def lock_object(self) -> Lockable:
        """Return an appropriate object that implements Python's
        threading.Lock API"""

    @abc.abstractmethod
    def rlock_object(self) -> ReentrantLock:
        """Return an appropriate object that implements Python's
        threading.RLock API"""

    @abc.abstractmethod
    def async_result(self) -> IAsyncResult:
        """Return an instance that conforms to the
        :class:`~IAsyncResult` interface appropriate for this
        handler"""

    @abc.abstractmethod
    def spawn(self, func: SpawnedFunc, *args: Any, **kwargs: Any) -> Any:
        """Spawn a function to run asynchronously

        :param args: args to call the function with.
        :param kwargs: keyword args to call the function with.

        This method should return immediately and execute the function
        with the provided args and kwargs in an asynchronous manner.

        """

    @abc.abstractmethod
    def dispatch_callback(self, callback: Callback) -> None:
        """Dispatch to the callback object

        :param callback: A :class:`~kazoo.protocol.states.Callback`
                         object to be called.

        """


class IAsyncResult(abc.ABC):
    """An Async Result object that can be queried for a value that has
    been set asynchronously.

    This object is modeled on the ``gevent`` AsyncResult object.

    The implementation must account for the fact that the :meth:`set`
    and :meth:`set_exception` methods will be called from within the
    Zookeeper thread which may require extra care under asynchronous
    environments.

    .. attribute:: value

        Holds the value passed to :meth:`set` if :meth:`set` was
        called. Otherwise `None`.

    .. attribute:: exception

        Holds the exception instance passed to :meth:`set_exception`
        if :meth:`set_exception` was called. Otherwise `None`.

    """

    @abc.abstractmethod
    def ready(self) -> bool:
        """Return `True` if and only if it holds a value or an
        exception"""

    @abc.abstractmethod
    def successful(self) -> bool:
        """Return `True` if and only if it is ready and holds a
        value"""

    @abc.abstractmethod
    def set(self, value: Any = None) -> None:
        """Store the value. Wake up the waiters.

        :param value: Value to store as the result.

        Any waiters blocking on :meth:`get` or :meth:`wait` are woken
        up. Sequential calls to :meth:`wait` and :meth:`get` will not
        block at all."""

    @abc.abstractmethod
    def set_exception(self, exception: Exception) -> None:
        """Store the exception. Wake up the waiters.

        :param exception: Exception to raise when fetching the value.

        Any waiters blocking on :meth:`get` or :meth:`wait` are woken
        up. Sequential calls to :meth:`wait` and :meth:`get` will not
        block at all."""

    @abc.abstractmethod
    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        """Return the stored value or raise the exception

        :param block: Whether this method should block or return
                      immediately.
        :type block: bool
        :param timeout: How long to wait for a value when `block` is
                        `True`.
        :type timeout: float

        If this instance already holds a value / an exception, return /
        raise it immediately. Otherwise, block until :meth:`set` or
        :meth:`set_exception` has been called or until the optional
        timeout occurs."""

    @abc.abstractmethod
    def get_nowait(self) -> Any:
        """Return the value or raise the exception without blocking.

        If nothing is available, raise the Timeout exception class on
        the associated :class:`IHandler` interface."""

    @abc.abstractmethod
    def wait(self, timeout: float | None = None) -> Any:
        """Block until the instance is ready.

        :param timeout: How long to wait for a value when `block` is
                        `True`.
        :type timeout: float

        If this instance already holds a value / an exception, return /
        raise it immediately. Otherwise, block until :meth:`set` or
        :meth:`set_exception` has been called or until the optional
        timeout occurs."""

    @abc.abstractmethod
    def rawlink(self, callback: Callable[[IAsyncResult], Any]) -> None:
        """Register a callback to call when a value or an exception is
        set

        :param callback:
            A callback function to call after :meth:`set` or
            :meth:`set_exception` has been called. This function will
            be passed a single argument, this instance.
        :type callback: func

        """

    @abc.abstractmethod
    def unlink(self, callback: Callable[[IAsyncResult], Any]) -> None:
        """Remove the callback set by :meth:`rawlink`

        :param callback: A callback function to remove.
        :type callback: func

        """

    @property
    @abc.abstractmethod
    def exception(self) -> Exception | None:
        """The exception set by :meth:`set_exception` or `None` if no
        exception has been set"""
