"""Zookeeper Locking Implementations

Error Handling
==============

It's highly recommended to add a state listener with
:meth:`~KazooClient.add_listener` and watch for
:attr:`~KazooState.LOST` and :attr:`~KazooState.SUSPENDED` state
changes and re-act appropriately. In the event that a
:attr:`~KazooState.LOST` state occurs, its certain that the lock
and/or the lease has been lost.

"""
import uuid

from kazoo.retry import (
    KazooRetry,
    RetryFailedError,
    ForceRetryError
)
from kazoo.exceptions import CancelledError
from kazoo.exceptions import KazooException
from kazoo.exceptions import LockTimeout
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import KazooState, EventType


class Lock(object):
    """Kazoo Lock

    Kazoo `Lock` supports three different locking strategies for a lock path.

    **Exclusive locks** represent the least complex locking strategy and
    guarantee that only a single ``Lock`` instance can acquire a lock path at
    any given time. This applies even if other locking strategies (as described
    below) are simultaneously in use for the same lock path. Exclusive locks
    are the default and will be provided if :py:meth:`__init__` is invoked
    with the default ``exclusive=True`` parameter.

    **Shared locks** allow different ``Lock`` instances to simultaneously
    acquire locks to the same lock path. In this strategy, a ``Lock`` instance
    is constructed with either ``exclusive=True`` (which is known as an
    "exclusive lock" and is described above) or ``exclusive=False`` (which is
    known as a "shared lock"). A shared lock will only be acquired if no
    exclusive locks are pending at the time acquisition is attempted. This
    means multiple shared locks can be acquired simultaneously, however
    additional shared locks will not be acquired once any exclusive lock for
    the lock path is pending. The shared lock strategy is most useful when
    multiple clients require read-only access to a resource but writing to that
    resource requires exclusive access. To use the shared locks strategy,
    invoke :py:meth:`__init__` and indicate a shared or exclusive lock via the
    ``exclusive`` parameter.

    **Revocable shared locks** provide the same locking guarantees and usage
    behavior as the shared locks strategy described above, however add the
    ability for any blocked lock acquisition request to signal to the blocking
    locks (or other lock requests which would be granted before it) to revoke.
    This is useful if shared lock holders do not routinely release resources
    (eg they are long-running readers) but are able to do so on request. Given
    cooperation from earlier lock holders or requestors is required, a callback
    is used to signal a revocation request. In the callback any resources
    should be released and then :py:meth:`cancel` and :py:meth:`release`
    invoked so the lock is removed. Note that a callback may safely ignore the
    callback notification if desired. To use the revocable shared locks
    strategy, invoke :py:meth:`acquire` with ``revoke=True``. This indicates a
    blocked lock request should request the revocation of any earlier blocking
    locks. For locks that can be interrupted and respond to such revocation
    requests, use the ``unlock`` parameter of :py:meth:`acquire` to provide the
    callback function that should be invoked on the first (and only first)
    revocation request.

    Example exclusive lock usage with a :class:`~kazoo.client.KazooClient`
    instance:

    .. code-block:: python

        zk = KazooClient()
        lock = zk.Lock("/lockpath", "my-identifier")
        with lock:  # blocks waiting for exclusive lock acquisition
            # do something with the lock

    """
    _MODE_SHARED = '__SHARED__'
    _MODE_EXCLUSIVE = '__EXCLUSIVE__'
    _UNLOCK_REQUEST = '__UNLOCK__'
    _UNLOCK_SUFFIX = ' ' + _UNLOCK_REQUEST

    def __init__(self, client, path, identifier=None, exclusive=True):
        """Create a Kazoo lock.

        :param client: The Kazoo client
        :type client: :class:`~kazoo.client.KazooClient`
        :param path: The lock path to use. May not contain the strings
                     ``__SHARED__`` or ``__EXCLUSIVE__``, as they are used
                     internally
        :type path: str
        :param identifier: Name to use for this lock contender, which may be
                           useful for querying to see who the current lock
                           :py:meth:`contenders` are. May not contain the
                           string ``__UNLOCK__``, as this is used internally.
        :type identifier: str
        :param exclusive: Whether this is an exclusive lock (``False`` means
                          a "shared lock" as described above)
        :type exclusive: bool

        .. versionadded:: 1.4
            The exclusive option.
        """
        if self._MODE_SHARED in path or self._MODE_EXCLUSIVE in path:
            raise ValueError('Path "{}" contains a reserved word'.format(path))

        if identifier and self._UNLOCK_REQUEST in str(identifier):
            raise ValueError('Identifier "{}" contains a reserved word'.format(
                identifier))

        self.client = client
        self.path = path
        self.exclusive = exclusive

        # some data is written to the node. this can be queried via
        # contenders() to see who is contending for the lock
        self.data = str(identifier or "").encode('utf-8')

        self.wake_event = client.handler.event_object()

        mode_suffix = self._MODE_EXCLUSIVE if exclusive else self._MODE_SHARED

        # props to Netflix Curator for this trick. It is possible for our
        # create request to succeed on the server, but for a failure to
        # prevent us from getting back the full path name. We prefix our
        # lock name with a uuid and can check for its presence on retry.
        self.prefix = uuid.uuid4().hex + mode_suffix
        self.create_path = self.path + "/" + self.prefix

        self.create_tried = False
        self.is_acquired = False
        self.assured_path = False
        self.cancelled = False
        self._retry = KazooRetry(max_tries=None)

    def _ensure_path(self):
        self.client.ensure_path(self.path)
        self.assured_path = True

    def cancel(self):
        """Cancel a pending lock acquire."""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self, blocking=True, timeout=None, revoke=False, unlock=None):
        """
        Acquire the lock. By defaults blocks and waits forever.

        :param blocking: Block until lock is obtained or return immediately.
        :type blocking: bool
        :param timeout: Don't wait forever to acquire the lock.
        :type timeout: float or None
        :param revoke: Identify all existing locks and lock requests that
                       prevent this lock being acquired and immediately request
                       them to unlock (this does not mean they will unlock or
                       are even listening for such requests though)
        :type revoke: bool
        :param unlock: The callback which will be invoked exactly once if
                       another lock used ``revoke=True`` and this lock or lock
                       request is blocking that lock from being acquired (it is
                       legal to use ``None`` to ignore revocation requests, or
                       provide a callback which takes no action)
        :type unlock: a zero-parameter function

        :returns: Was the lock acquired?
        :rtype: bool

        :raises: :exc:`~kazoo.exceptions.LockTimeout` if the lock
                 wasn't acquired within `timeout` seconds.

        .. versionadded:: 1.1
            The timeout option.
        .. versionadded:: 1.4
            The revoke and unlock options.
        """
        try:
            retry = self._retry.copy()
            retry.deadline = timeout
            self.is_acquired = retry(self._inner_acquire, blocking=blocking,
                                     timeout=timeout, revoke=revoke,
                                     unlock=unlock)
        except KazooException:
            # if we did ultimately fail, attempt to clean up
            self._best_effort_cleanup()
            self.cancelled = False
            raise
        except RetryFailedError:  # pragma: nocover
            self._best_effort_cleanup()

        if not self.is_acquired:
            self._delete_node(self.node)

        return self.is_acquired

    def _inner_acquire(self, blocking, timeout, revoke, unlock):
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
            if unlock:
                # watch this node for its first data change (the only other
                # events would be deletion or additional data change events, so
                # either way the absence of additional events is fine)
                def unlock_callback(event):
                    if event.type == EventType.CHANGED:
                        unlock()

                data, _ = self.client.get(node, unlock_callback)
                if self._UNLOCK_REQUEST in data.decode('utf-8'):
                    # a request to revoke our request has already been received
                    # (we let the callback know about this, but we keep going
                    # given the callback is under no obligation to comply)
                    unlock()  # pragma: nocover

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

            acquired, blockers = self.acquired_lock(children, our_index)
            if acquired:
                return True

            if not blocking:
                return False

            # we are in the mix
            if revoke:
                for child in blockers:
                    try:
                        child_path = self.path + "/" + child
                        data, stat = self.client.get(child_path)
                        decoded_data = data.decode('utf-8')
                        if self._UNLOCK_REQUEST not in decoded_data:
                            data = str(decoded_data +
                                       self._UNLOCK_SUFFIX).encode('utf-8')
                            self.client.set(child_path, data)
                    except NoNodeError:  # pragma: nocover
                        pass

            # watch the last blocker and bide our time
            predecessor = self.path + "/" + blockers[-1]
            if self.client.exists(predecessor, self._watch_predecessor):
                self.wake_event.wait(timeout)
                if not self.wake_event.isSet():
                    raise LockTimeout("Failed to acquire lock on %s after %s "
                                      "seconds" % (self.path, timeout))

    def acquired_lock(self, children, index):
        """Return if we acquired the lock, and if not, the blocking
        contenders.

        """
        prior_nodes = children[:index]
        if self.exclusive:
            return (index == 0, prior_nodes)

        # Shared locks are only unavailable if a prior lock is exclusive
        prior_exclusive = [x for x in prior_nodes
                           if self._MODE_EXCLUSIVE in x]
        if prior_exclusive:
            return (False, prior_exclusive)
        return (True, None)

    def _watch_predecessor(self, event):
        self.wake_event.set()

    def _get_sorted_children(self):
        children = self.client.get_children(self.path)

        # zookeeper sequence node suffix of %010d is relied upon for sorting
        children.sort(key=lambda c: c[-10:])
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
            node = self._find_node()
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

    def contenders(self, unlocks_only=False):
        """Return an ordered list of the current contenders for the
        lock.

        .. note::

            If the contenders did not set an identifier, it will appear
            as a blank string.

        :param unlocks_only: indicates whether to only return those contenders
                             which have been requested to revoke their locks or
                             lock requests
        :type unlocks_only: bool
        :return: a list of contender identifiers
        :type: list

        .. versionadded:: 1.4
            The unlocks_only option.
        """
        # make sure our election parent node exists
        if not self.assured_path:
            self._ensure_path()

        children = self._get_sorted_children()

        contenders = []
        for child in children:
            try:
                data, stat = self.client.get(self.path + "/" + child)
                identifier = data.decode('utf-8')
                if not unlocks_only or self._UNLOCK_REQUEST in identifier:
                    identifier = identifier.replace(self._UNLOCK_SUFFIX, '')
                    contenders.append(identifier)
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

        with self.client.Lock(self.lock_path, self.data):
            while True:
                self.wake_event.clear()

                # Attempt to grab our lease...
                if self._get_lease():
                    return True

                if blocking:
                    # If blocking, wait until self._watch_lease_change() is
                    # called before returning
                    self.wake_event.wait(timeout)
                    if not self.wake_event.isSet():
                        raise LockTimeout(
                            "Failed to acquire semaphore on %s "
                            "after %s seconds" % (self.path, timeout))
                else:
                    # If not blocking, register another watch that will trigger
                    # self._get_lease() as soon as the children change again.
                    self.client.get_children(self.path, self._get_lease)
                    return False

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
