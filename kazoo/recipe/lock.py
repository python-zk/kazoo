"""Zookeeper Locking Implementations

:Maintainer: Ben Bangert <ben@groovie.org>
:Status: Production

Error Handling
==============

It's highly recommended to add a state listener with
:meth:`~KazooClient.add_listener` and watch for
:attr:`~KazooState.LOST` and :attr:`~KazooState.SUSPENDED` state
changes and re-act appropriately. In the event that a
:attr:`~KazooState.LOST` state occurs, its certain that the lock
and/or the lease has been lost.

"""

import sys
try:
    from time import monotonic as now
except ImportError:
    from time import time as now
import uuid

import six

from kazoo.retry import (
    KazooRetry,
    RetryFailedError,
    ForceRetryError
)
from kazoo.exceptions import CancelledError
from kazoo.exceptions import KazooException
from kazoo.exceptions import LockTimeout
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import KazooState


class _Watch(object):
    def __init__(self, duration=None):
        self.duration = duration
        self.started_at = None

    def start(self):
        self.started_at = now()

    def leftover(self):
        if self.duration is None:
            return None
        else:
            elapsed = now() - self.started_at
            return max(0, self.duration - elapsed)


class Lock(object):
    """Kazoo Lock

    Example usage with a :class:`~kazoo.client.KazooClient` instance:

    .. code-block:: python

        zk = KazooClient()
        zk.start()
        lock = zk.Lock("/lockpath", "my-identifier")
        with lock:  # blocks waiting for lock acquisition
            # do something with the lock

    Note: This lock is not *re-entrant*. Repeated calls after already
    acquired will block.

    """
    _NODE_NAME = '__lock__'

    def __init__(self, client, path, identifier=None):
        """Create a Kazoo lock.

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The lock path to use.
        :param identifier: Name to use for this lock contender. This
                           can be useful for querying to see who the
                           current lock contenders are.

        """
        self.client = client
        self.path = path

        # some data is written to the node. this can be queried via
        # contenders() to see who is contending for the lock
        self.data = str(identifier or "").encode('utf-8')
        self.node = None

        self.wake_event = client.handler.event_object()

        # props to Netflix Curator for this trick. It is possible for our
        # create request to succeed on the server, but for a failure to
        # prevent us from getting back the full path name. We prefix our
        # lock name with a uuid and can check for its presence on retry.
        self.prefix = uuid.uuid4().hex + self._NODE_NAME
        self.create_path = self.path + "/" + self.prefix

        self.create_tried = False
        self.is_acquired = False
        self.assured_path = False
        self.cancelled = False
        self._retry = KazooRetry(max_tries=None,
                                 sleep_func=client.handler.sleep_func)
        self._lock = client.handler.lock_object()

    def _ensure_path(self):
        self.client.ensure_path(self.path)
        self.assured_path = True

    def cancel(self):
        """Cancel a pending lock acquire."""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self, blocking=True, timeout=None):
        """
        Acquire the lock. By defaults blocks and waits forever.

        :param blocking: Block until lock is obtained or return immediately.
        :type blocking: bool
        :param timeout: Don't wait forever to acquire the lock.
        :type timeout: float or None

        :returns: Was the lock acquired?
        :rtype: bool

        :raises: :exc:`~kazoo.exceptions.LockTimeout` if the lock
                 wasn't acquired within `timeout` seconds.

        .. versionadded:: 1.1
            The timeout option.
        """

        def _acquire_lock():
            got_it = self._lock.acquire(False)
            if not got_it:
                raise ForceRetryError()
            return True

        retry = self._retry.copy()
        retry.deadline = timeout

        # Ensure we are locked so that we avoid multiple threads in
        # this acquistion routine at the same time...
        locked = self._lock.acquire(False)
        if not locked and not blocking:
            return False
        if not locked:
            # Lock acquire doesn't take a timeout, so simulate it...
            try:
                locked = retry(_acquire_lock)
            except RetryFailedError:
                return False
        already_acquired = self.is_acquired
        try:
            gotten = False
            try:
                gotten = retry(self._inner_acquire,
                               blocking=blocking, timeout=timeout)
            except RetryFailedError:
                pass
            except KazooException:
                # if we did ultimately fail, attempt to clean up
                exc_info = sys.exc_info()
                if not already_acquired:
                    self._best_effort_cleanup()
                    self.cancelled = False
                six.reraise(exc_info[0], exc_info[1], exc_info[2])
            if gotten:
                self.is_acquired = gotten
            if not gotten and not already_acquired:
                self._best_effort_cleanup()
            return gotten
        finally:
            self._lock.release()

    def _watch_session(self, state):
        self.wake_event.set()
        return True

    def _inner_acquire(self, blocking, timeout):

        # wait until it's our chance to get it..
        if self.is_acquired:
            if not blocking:
                return False
            raise ForceRetryError()

        # make sure our election parent node exists
        if not self.assured_path:
            self._ensure_path()

        node = None
        if self.create_tried:
            node = self._find_node()
        else:
            self.create_tried = True

        if not node:
            node = self.client.create(self.create_path, self.data,
                                      ephemeral=True, sequence=True)
            # strip off path to node
            node = node[len(self.path) + 1:]

        self.node = node

        while True:
            self.wake_event.clear()

            # bail out with an exception if cancellation has been requested
            if self.cancelled:
                raise CancelledError()

            children = self._get_sorted_children()

            try:
                our_index = children.index(node)
            except ValueError:  # pragma: nocover
                # somehow we aren't in the children -- probably we are
                # recovering from a session failure and our ephemeral
                # node was removed
                raise ForceRetryError()

            if self.acquired_lock(children, our_index):
                return True

            if not blocking:
                return False

            # otherwise we are in the mix. watch predecessor and bide our time
            predecessor = self.path + "/" + children[our_index - 1]
            self.client.add_listener(self._watch_session)
            try:
                if self.client.exists(predecessor, self._watch_predecessor):
                    self.wake_event.wait(timeout)
                    if not self.wake_event.isSet():
                        raise LockTimeout("Failed to acquire lock on %s after "
                                          "%s seconds" % (self.path, timeout))
            finally:
                self.client.remove_listener(self._watch_session)

    def acquired_lock(self, children, index):
        return index == 0

    def _watch_predecessor(self, event):
        self.wake_event.set()

    def _get_sorted_children(self):
        children = self.client.get_children(self.path)

        # can't just sort directly: the node names are prefixed by uuids
        lockname = self._NODE_NAME

        children = list(filter(lambda c: lockname in c, children))
        children.sort(key=lambda c: c[c.find(lockname) + len(lockname):])
        return children

    def _find_node(self):
        children = self.client.get_children(self.path)
        for child in children:
            if child.startswith(self.prefix):
                return child
        return None

    def _delete_node(self, node):
        self.client.delete(self.path + "/" + node)

    def _best_effort_cleanup(self):
        try:
            node = self.node or self._find_node()
            if node:
                self._delete_node(node)
        except KazooException:  # pragma: nocover
            pass

    def release(self):
        """Release the lock immediately."""
        return self.client.retry(self._inner_release)

    def _inner_release(self):
        if not self.is_acquired:
            return False

        try:
            self._delete_node(self.node)
        except NoNodeError:  # pragma: nocover
            pass

        self.is_acquired = False
        self.node = None
        return True

    def contenders(self):
        """Return an ordered list of the current contenders for the
        lock.

        .. note::

            If the contenders did not set an identifier, it will appear
            as a blank string.

        """
        # make sure our election parent node exists
        if not self.assured_path:
            self._ensure_path()

        children = self._get_sorted_children()

        contenders = []
        for child in children:
            try:
                data, stat = self.client.get(self.path + "/" + child)
                contenders.append(data.decode('utf-8'))
            except NoNodeError:  # pragma: nocover
                pass
        return contenders

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


class Semaphore(object):
    """A Zookeeper-based Semaphore

    This synchronization primitive operates in the same manner as the
    Python threading version only uses the concept of leases to
    indicate how many available leases are available for the lock
    rather than counting.

    Note: This lock is not meant to be *re-entrant*.

    Example:

    .. code-block:: python

        zk = KazooClient()
        semaphore = zk.Semaphore("/leasepath", "my-identifier")
        with semaphore:  # blocks waiting for lock acquisition
            # do something with the semaphore

    .. warning::

        This class stores the allowed max_leases as the data on the
        top-level semaphore node. The stored value is checked once
        against the max_leases of each instance. This check is
        performed when acquire is called the first time. The semaphore
        node needs to be deleted to change the allowed leases.

    .. versionadded:: 0.6
        The Semaphore class.

    .. versionadded:: 1.1
        The max_leases check.

    """
    def __init__(self, client, path, identifier=None, max_leases=1):
        """Create a Kazoo Lock

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The semaphore path to use.
        :param identifier: Name to use for this lock contender. This
                           can be useful for querying to see who the
                           current lock contenders are.
        :param max_leases: The maximum amount of leases available for
                           the semaphore.

        """
        # Implementation notes about how excessive thundering herd
        # and watches are avoided
        # - A node (lease pool) holds children for each lease in use
        # - A lock is acquired for a process attempting to acquire a
        #   lease. If a lease is available, the ephemeral node is
        #   created in the lease pool and the lock is released.
        # - Only the lock holder watches for children changes in the
        #   lease pool
        self.client = client
        self.path = path

        # some data is written to the node. this can be queried via
        # contenders() to see who is contending for the lock
        self.data = str(identifier or "").encode('utf-8')
        self.max_leases = max_leases
        self.wake_event = client.handler.event_object()

        self.create_path = self.path + "/" + uuid.uuid4().hex
        self.lock_path = path + '-' + '__lock__'
        self.is_acquired = False
        self.assured_path = False
        self.cancelled = False
        self._session_expired = False

    def _ensure_path(self):
        result = self.client.ensure_path(self.path)
        self.assured_path = True
        if result is True:
            # node did already exist
            data, _ = self.client.get(self.path)
            try:
                leases = int(data.decode('utf-8'))
            except (ValueError, TypeError):
                # ignore non-numeric data, maybe the node data is used
                # for other purposes
                pass
            else:
                if leases != self.max_leases:
                    raise ValueError(
                        "Inconsistent max leases: %s, expected: %s" %
                        (leases, self.max_leases)
                    )
        else:
            self.client.set(self.path, str(self.max_leases).encode('utf-8'))

    def cancel(self):
        """Cancel a pending semaphore acquire."""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self, blocking=True, timeout=None):
        """Acquire the semaphore. By defaults blocks and waits forever.

        :param blocking: Block until semaphore is obtained or
                         return immediately.
        :type blocking: bool
        :param timeout: Don't wait forever to acquire the semaphore.
        :type timeout: float or None

        :returns: Was the semaphore acquired?
        :rtype: bool

        :raises:
            ValueError if the max_leases value doesn't match the
            stored value.

            :exc:`~kazoo.exceptions.LockTimeout` if the semaphore
            wasn't acquired within `timeout` seconds.

        .. versionadded:: 1.1
            The blocking, timeout arguments and the max_leases check.
        """
        # If the semaphore had previously been canceled, make sure to
        # reset that state.
        self.cancelled = False

        try:
            self.is_acquired = self.client.retry(
                self._inner_acquire, blocking=blocking, timeout=timeout)
        except KazooException:
            # if we did ultimately fail, attempt to clean up
            self._best_effort_cleanup()
            self.cancelled = False
            raise

        return self.is_acquired

    def _inner_acquire(self, blocking, timeout=None):
        """Inner loop that runs from the top anytime a command hits a
        retryable Zookeeper exception."""
        self._session_expired = False
        self.client.add_listener(self._watch_session)

        if not self.assured_path:
            self._ensure_path()

        # Do we already have a lease?
        if self.client.exists(self.create_path):
            return True

        w = _Watch(duration=timeout)
        w.start()
        lock = self.client.Lock(self.lock_path, self.data)
        gotten = lock.acquire(blocking=blocking, timeout=w.leftover())
        if not gotten:
            return False
        try:
            while True:
                self.wake_event.clear()

                # Attempt to grab our lease...
                if self._get_lease():
                    return True

                if blocking:
                    # If blocking, wait until self._watch_lease_change() is
                    # called before returning
                    self.wake_event.wait(w.leftover())
                    if not self.wake_event.isSet():
                        raise LockTimeout(
                            "Failed to acquire semaphore on %s "
                            "after %s seconds" % (self.path, timeout))
                else:
                    return False
        finally:
            lock.release()

    def _watch_lease_change(self, event):
        self.wake_event.set()

    def _get_lease(self, data=None):
        # Make sure the session is still valid
        if self._session_expired:
            raise ForceRetryError("Retry on session loss at top")

        # Make sure that the request hasn't been canceled
        if self.cancelled:
            raise CancelledError("Semaphore cancelled")

        # Get a list of the current potential lock holders. If they change,
        # notify our wake_event object. This is used to unblock a blocking
        # self._inner_acquire call.
        children = self.client.get_children(self.path,
                                            self._watch_lease_change)

        # If there are leases available, acquire one
        if len(children) < self.max_leases:
            self.client.create(self.create_path, self.data, ephemeral=True)

        # Check if our acquisition was successful or not. Update our state.
        if self.client.exists(self.create_path):
            self.is_acquired = True
        else:
            self.is_acquired = False

        # Return current state
        return self.is_acquired

    def _watch_session(self, state):
        if state == KazooState.LOST:
            self._session_expired = True
            self.wake_event.set()

            # Return true to de-register
            return True

    def _best_effort_cleanup(self):
        try:
            self.client.delete(self.create_path)
        except KazooException:  # pragma: nocover
            pass

    def release(self):
        """Release the lease immediately."""
        return self.client.retry(self._inner_release)

    def _inner_release(self):
        if not self.is_acquired:
            return False
        try:
            self.client.delete(self.create_path)
        except NoNodeError:  # pragma: nocover
            pass
        self.is_acquired = False
        return True

    def lease_holders(self):
        """Return an unordered list of the current lease holders.

        .. note::

            If the lease holder did not set an identifier, it will
            appear as a blank string.

        """
        if not self.client.exists(self.path):
            return []

        children = self.client.get_children(self.path)

        lease_holders = []
        for child in children:
            try:
                data, stat = self.client.get(self.path + "/" + child)
                lease_holders.append(data.decode('utf-8'))
            except NoNodeError:  # pragma: nocover
                pass
        return lease_holders

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
