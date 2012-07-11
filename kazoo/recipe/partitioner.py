"""Zookeeper Partitioner Implementation

:class:`SetPartitioner` implements a partitioning scheme using
Zookeeper for dividing up resources amongst members of a party.

This is useful when there is a set of resources that should only be
accessed by a single process at a time, and which multiple processes
across a cluster might want to divide up.

Example Use-Case
----------------

- Multiple workers across a cluster need to divide up a list of queues
  so that no two workers own the same queue.

"""
import os
import socket
import time
import threading

from kazoo.client import KazooState


class PartitionState(object):
    """High level partition state values

    .. attribute:: ALLOCATING

        The set needs to be partitioned, and may require an existing
        partition set to be released before acquiring a new partition
        of the set.

    .. attribute:: ACQUIRED

        The set has been partitioned and acquired.

    .. attribute:: RELEASE

        The set needs to be repartitioned, and the current partitions
        must be released before a new allocation can be made.

    .. attribute:: FAILURE

        The set partition has failed. This occurs when the maximum
        time to partition the set fails or the Zookeeper session is
        lost. The partitioner is unusable after this state and must
        be recreated.

    """
    ALLOCATING = "ALLOCATING"
    ACQUIRED = "ACQUIRED"
    RELEASE = "RELEASE"
    FAILURE = "FAILURE"


class SetPartitioner(object):
    """Partitions a set amongst members of a party

    This class will partition a set amongst members of a party such
    that each party will be given one or more members of the set and
    each set item will be given to a single member. When new members
    enter or leave the party, the set will be re-partioned amongst the
    members.

    When the :class:`SetPartitioner` enters the
    :attr:`~PartitionState.FAILURE` state, it is unrecoverable
    and a new :class:`SetPartitioner` should be created.


    Simple Example:

    .. code-block:: python

        from kazoo.client import KazooClient
        client = KazooClient()

        qp = client.SetPartitioner(
            path='/work_queues', set=('queue-1', 'queue-2', 'queue-3'))

        def use_setlist(partitions):
            # do something with a partition passed in

        # Run the use_setlist function when the partition is acquired
        # repeatedly
        qp.run(func=use_setlist)

    Sometimes, more control is needed over handling the state
    transitions. Or the program may need to do other things during
    specific states, in which case a more verbose example that allows
    for finer grained control:

    .. code-block:: python

        from kazoo.client import KazooClient
        from kazoo.recipe.partioner import PartitionState
        client = KazooClient()

        qp = client.SetPartitioner(
            path='/work_queues', set=('queue-1', 'queue-2', 'queue-3'))

        while 1:
            if qp.state == PartitionState.ACQUIRED:
                # Do something with the qp
            elif qp.state == PartitionState.RELEASE:
                qp.release()
            elif qp.state == PartitionState.ALLOCATING:
                qp.wait_for_acquire()
            elif qp.state == PartitionState.FAILURE:
                raise Exception("Lost or unable to acquire partition")

    State Transitions
    -----------------

    When created, the :class:`SetPartitioner` enters the
    :attr:`PartitionState.ALLOCATING` state.

    :attr:`~PartitionState.ALLOCATING` ->
    :attr:`~PartitionState.ACQUIRED`

        Set was partitioned successfully, the partition list assigned
        is accessible via list/iter methods or calling list() on the
        :class:`SetPartitioner` instance.

    :attr:`~PartitionState.ALLOCATING` ->
    :attr:`~PartitionState.FAILURE`

        Allocating the set failed either due to a Zookeeper session
        expiration, or failure to acquire the partition members of the
        set withing the timeout period.

    :attr:`~PartitionState.ACQUIRED` ->
    :attr:`~PartitionState.RELEASE`

        The members of the party has changed, and the set needs to be
        repartioned. :meth:`SetPartitioner.release` should be called
        as soon as possible.

    :attr:`~PartitionState.ACQUIRED` ->
    :attr:`~PartitionState.FAILURE`

        The current partition was lost due to a Zookeeper session
        expiration.

    :attr:`~PartitionState.RELEASE` ->
    :attr:`~PartitionState.ALLOCATING`

        The current partition was released and is being re-allocated.

    """
    def __init__(self, client, path, set, partition_func=None,
                 identifier=None):
        """Create a :class:~SetPartitioner` instance

        :param client: A :class:`~kazoo.client.KazooClient` instance
        :param path: The partition path to use
        :param set: The set of items to partition
        :param partition_func: A function to use to decide how to partition the
                               set
        :param identifier: An identifier to use for this member of the
                           party when participating. Defaults to the
                           hostname + process id.

        """
        self.state = PartitionState.ALLOCATING

        self._client = client
        self._path = path
        self._set = set
        self._partition_set = []
        self._partition_func = partition_func or self._partition_set
        self._identifier = identifier or '%s-%s' % (
            socket.gethostname(), os.getpid())
        self._locks = []
        self._lock_path = '/'.join([path, 'locks'])
        self._party_path = '/'.join([path, 'party'])

        # Create basic path nodes
        client.ensure_path(path)
        client.ensure_path(self._lock_path)
        client.ensure_path(self._party_path)

        # Join the party
        self._party = client.ShallowParty(self._party_path,
                                          identifier=self.identifier)
        self._party.join()
        self._party_members = self._party.get_participants()

        self._state_change = threading.Condition()
        client.add_listener(self._establish_sessionwatch)

        # Now watch the party and set the callback on the async result
        # so we know when we're ready
        watcher = ChildrenWatcher(client, self._party_path)
        asy = watcher.start()
        asy.rawlink(self._allocate_set)

    def _allocate_transition(self, result):
        # First time running
        # Did we get an exception?
        if result.exception:
            with self._state_change:
                self.state = PartitionState.FAILURE

    def _establish_sessionwatch(self, state):
        """Register ourself to listen for session events, we shut down if we
        become lost"""
        if state == KazooState.LOST:
            self.client.remove_listener(self._establish_sessionwatch)

            # Ensure we don't get flipped elsewhere
            with self._state_change:
                self.state = PartitionState.FAILURE

    def _partition_set(self, identifier, members, ):
        pass

    def release(self):
        for lock in self._locks:
            lock.release()


class ChildrenWatcher(object):
    """A separate watcher for the children of a node, that ignores changes
    within a boundary time and sets the result only when the boundary
    time has elapsed and there is no change of children from beginning to end

    Example::

        watcher = ChildrenWatcher(client, '/some/path')
        result = watcher.start()
        children = result.get()  # blocks until the children has held steady
                                 # for the specified time_boundary

    """
    def __init__(self, client, path, time_boundary=30):
        self.client = client
        self.path = path
        self.time_boundary = time_boundary
        self.children = []
        self.last_change = None

    def start(self):
        """Begin the watching process

        :returns: An :class:`~kazoo.interfaces.IAsyncResult` instance

        """
        self.asy = asy = self.client._handler.async_result()
        self.client._handler.spawn(self._inner_start)
        return asy

    def _inner_start(self):
        if not self.last_change:
            self.last_change = time.time()

        try:
            while time.time() - self.last_change < self.time_boundary:
                self.children = self.client.retry(
                    self.client.get_children, self.path, self.children_watcher)

                # Now sleep for a time boundary's worth from the last change
                sleep_till = self.time_boundary - (time.time() - self.last_change)
                time.sleep(sleep_till)

            self.asy.set(self.children)
        except Exception as exc:
            self.asy.set_exception(exc)

    def children_watcher(self, event):
        self.last_change = time.time()
