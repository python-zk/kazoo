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

from kazoo.retry import ForceRetryError
from kazoo.exceptions import CancelledError
from kazoo.exceptions import KazooException
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import KazooState


class Lock(object):
    """Kazoo Basic Lock

    Example usage with a :class:`~kazoo.client.KazooClient` instance:

    .. code-block:: python

        zk = KazooClient()
        lock = zk.Lock("/lockpath", "my-identifier")
        with lock:  # blocks waiting for lock acquisition
            # do something with the lock

    """
    _NODE_NAME = '__lock__'

    def __init__(self, client, path, identifier=None):
        """Create a Kazoo Lock

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

    def cancel(self):
        """Cancel a pending lock acquire"""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self, blocking=True):
        """
        Acquire the mutex, if blocking=True (default) block until it is
        obtained.

        Return acquisition result.
        """
        try:
            self.is_acquired = self.client.retry(
                self._inner_acquire, blocking=blocking)
        except KazooException:
            # if we did ultimately fail, attempt to clean up
            self._best_effort_cleanup()
            self.cancelled = False
            raise

        if not self.is_acquired:
            self._delete_node(self.node)

        return self.is_acquired

    def _inner_acquire(self, blocking):
        # make sure our election parent node exists
        if not self.assured_path:
            self.client.ensure_path(self.path)

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
            if self.client.exists(predecessor, self._watch_predecessor):
                self.wake_event.wait()

    def acquired_lock(self, children, index):
        return index == 0

    def _watch_predecessor(self, event):
        self.wake_event.set()

    def _get_sorted_children(self):
        children = self.client.get_children(self.path)

        # can't just sort directly: the node names are prefixed by uuids
        lockname = self._NODE_NAME
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
            node = self._find_node()
            if node:
                self._delete_node(node)
        except KazooException:  # pragma: nocover
            pass

    def release(self):
        """Release the mutex immediately"""
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
        lock

        .. note::

            If the contenders did not set an identifier, it will appear
            as a blank string.

        """
        # make sure our election parent node exists
        if not self.assured_path:
            self.client.ensure_path(self.path)

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

    Example:

    .. code-block:: python

        zk = KazooClient()
        semaphore = zk.Semaphore("/leasepath", "my-identifier")
        with semaphore:  # blocks waiting for lock acquisition
            # do something with the semaphore

    .. warning::

        This class does not make any guarantee's that the amount of
        leases for a path is agreed-upon by the :class:`Semaphore`
        objects using it. It is up to the developer to ensure that they
        all have the same `max_leases` value.

    .. versionadded:: 0.6
        The Semaphore class.

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

    def cancel(self):
        """Cancel a pending lock acquire"""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self):
        """Acquire the semaphore, blocking until acquired."""
        try:
            self.client.retry(self._inner_acquire)
            self.is_acquired = True
        except KazooException:
            # if we did ultimately fail, attempt to clean up
            self._best_effort_cleanup()
            self.cancelled = False
            raise

    def _inner_acquire(self):
        """Inner loop that runs from the top anytime a command hits a
        retryable Zookeeper exception."""
        self._session_expired = False
        self.client.add_listener(self._watch_session)

        if not self.assured_path:
            self.client.ensure_path(self.path)

        # Do we already have a lease?
        if self.client.exists(self.create_path):
            return True

        with self.client.Lock(self.lock_path, self.data):
            while True:
                self.wake_event.clear()

                if self._session_expired:
                    raise ForceRetryError("Retry on session loss at top")

                if self.cancelled:
                    raise CancelledError("Semaphore cancelled")

                # Is there a lease free?
                children = self.client.get_children(self.path,
                                                    self._watch_lease_change)
                if len(children) < self.max_leases:
                    self.client.create(self.create_path, self.data,
                                       ephemeral=True)
                    return True
                else:
                    self.wake_event.wait()

    def _watch_lease_change(self, event):
        self.wake_event.set()

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
