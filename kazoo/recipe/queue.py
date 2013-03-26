"""Queue

A Zookeeper based queue implementation.
"""

import uuid
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.retry import ForceRetryError
from kazoo.protocol.states import EventType


class Queue(object):
    """A distributed queue with optional priority support.

    This queue does not offer reliable consumption. An entry is removed
    from the queue prior to being processed. So if an error occurs, the
    consumer has to re-queue the item or it will be lost.

    """

    prefix = "entry-"

    def __init__(self, client, path):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path to use.
        """
        self.client = client
        self.path = path
        self.ensured_path = False

    def _ensure_parent(self):
        if not self.ensured_path:
            # make sure our parent node exists
            self.client.ensure_path(self.path)
            self.ensured_path = True

    def __len__(self):
        """Return queue size."""
        self._ensure_parent()
        _, stat = self.client.retry(self.client.get, self.path)
        return stat.children_count

    def get(self):
        """Get and remove an item from the queue."""
        self._ensure_parent()
        children = self.client.retry(self.client.get_children, self.path)
        children = list(sorted(children))
        return self.client.retry(self._inner_get, children)

    def _inner_get(self, children):
        if not children:
            return None
        name = children.pop(0)
        try:
            data, stat = self.client.get(self.path + "/" + name)
        except NoNodeError:  # pragma: nocover
            # the first node has vanished in the meantime, try to
            # get another one
            raise ForceRetryError()
        try:
            self.client.delete(self.path + "/" + name)
        except NoNodeError:  # pragma: nocover
            # we were able to get the data but someone else has removed
            # the node in the meantime. consider the item as processed
            # by the other process
            raise ForceRetryError()
        return data

    def put(self, value, priority=100):
        """Put an item into the queue.

        :param value: Byte string to put into the queue.
        :param priority:
            An optional priority as an integer with at most 3 digits.
            Lower values signify higher priority.
        """
        if not isinstance(value, bytes):
            raise TypeError("value must be a byte string")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        elif priority < 0 or priority > 999:
            raise ValueError("priority must be between 0 and 999")
        self._ensure_parent()
        path = '{path}/{prefix}{priority:03d}-'.format(
            path=self.path, prefix=self.prefix, priority=priority)
        self.client.create(path, value, sequence=True)


class LockingQueue(object):
    """A distributed queue with priority and locking support.

    Upon retrieving an entry from the queue, the entry gets locked with an
    ephemeral node (instead of deleted). If an error occours, this lock gets
    released so that others could retake the entry. This adds a little penalty
    as compared to :class:`Queue` implementation.

    The user should firstly call :meth:`LockingQueue.get` method to lock and
    retrieve a next entry. When finished processing the entry, a user should
    call :meth:`LockingQueue.consume` method that will remove the entry from
    the queue.

    This queue will not track connection status with ZooKeeper. If a node locks
    an element, then loses connection with ZooKeeper and later reconnects, the
    lock will probably be removed by Zookeeper in the meantime, but a node
    would still think that it holds a lock. The user should check the
    connection status with Zookeeper or call :meth:`LockingQueue.holds_lock`
    method that will check if a node still holds the lock.

    """
    lock = "/taken"
    entries = "/entries"
    entry = "entry"

    def __init__(self, client, path):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path to use in ZooKeeper.
        """
        self.id = uuid.uuid4().hex.encode()
        self.client = client
        self.path = path
        self.ensured_path = False
        self.processing_element = None

        self._lock_path = self.path + LockingQueue.lock
        self._entries_path = self.path + LockingQueue.entries

    def __len__(self):
        """Returns the current length of the queue.

        :returns: queue size (includes locked entries count).

        """
        self._ensure_paths()
        _, stat = self.client.retry(self.client.get, self._entries_path)
        return stat.children_count

    def put(self, value, priority=100):
        """Put an entry into the queue.

        :param value: Byte string to put into the queue.
        :param priority:
            An optional priority as an integer with at most 3 digits.
            Lower values signify higher priority.

        """
        if not isinstance(value, bytes):
            raise TypeError("value must be a byte string")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        elif priority < 0 or priority > 999:
            raise ValueError("priority must be between 0 and 999")
        self._ensure_paths()

        self.client.create(
            "{path}/{prefix}-{priority:03d}-".format(
                path=self._entries_path,
                prefix=LockingQueue.entry,
                priority=priority),
            value, sequence=True)

    def put_all(self, values, priority=100):
        """Put several entries into the queue.

        :param values: A list of values to put into the queue.
        :param priority:
            An optional priority as an integer with at most 3 digits.
            Lower values signify higher priority.

        """
        if not isinstance(values, list):
            raise TypeError("values must be a list of byte strings")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        elif priority < 0 or priority > 999:
            raise ValueError("priority must be between 0 and 999")
        self._ensure_paths()

        transaction = self.client.transaction()
        for value in values:
            if not isinstance(value, bytes):
                raise TypeError("value must be a byte string")
            transaction.create(
                "{path}/{prefix}-{priority:03d}-".format(
                    path=self._entries_path,
                    prefix=LockingQueue.entry,
                    priority=priority),
                value, sequence=True)
        transaction.commit()

    def get(self, timeout=None):
        """Locks and gets an entry from the queue. If a previously got entry
        was not consumed, this method will return that entry.

        :param timeout:
            Maximum waiting time in seconds. If None then it will wait
            untill an entry appears in the queue.
        :returns: A locked entry value or None if the timeout was reached.

        """
        self._ensure_paths()
        if not self.processing_element is None:
            return self.processing_element[1]
        else:
            return self._inner_get(timeout)

    def holds_lock(self):
        """Checks if a node still holds the lock.

        :returns: True if a node still holds the lock, False otherwise.

        """
        if self.processing_element is None:
            return False
        lock_id, _ = self.processing_element
        lock_path = "{path}/{id}".format(path=self._lock_path, id=lock_id)
        self.client.sync(lock_path)
        value, stat = self.client.retry(self.client.get, lock_path)
        return value == self.id

    def consume(self):
        """Removes a currently processing entry from the queue.

        :returns: True if element was removed successfully, False otherwise.

        """
        if not self.processing_element is None and self.holds_lock:
            id_, value = self.processing_element
            transaction = self.client.transaction()
            transaction.delete("{path}/{id}".format(
                path=self._entries_path,
                id=id_))
            transaction.delete("{path}/{id}".format(
                path=self._lock_path,
                id=id_))
            transaction.commit()
            self.processing_element = None
            return True
        else:
            return False

    def _inner_get(self, timeout):
        flag = self.client.handler.event_object()
        lock = self.client.handler.lock_object()
        canceled = False
        value = []

        def check_for_updates(event):
            if not event is None and event.type != EventType.CHILD:
                return
            with lock:
                if canceled or flag.isSet():
                    return
                values = self.client.retry(self.client.get_children,
                    self._entries_path,
                    check_for_updates)
                taken = self.client.retry(self.client.get_children,
                    self._lock_path,
                    check_for_updates)
                available = self._filter_locked(values, taken)
                if len(available) > 0:
                    ret = self._take(available[0])
                    if not ret is None:
                        # By this time, no one took the task
                        value.append(ret)
                        flag.set()

        check_for_updates(None)
        retVal = None
        flag.wait(timeout)
        with lock:
            canceled = True
            if len(value) > 0:
                # We successfully locked an entry
                self.processing_element = value[0]
                retVal = value[0][1]
        return retVal

    def _filter_locked(self, values, taken):
        taken = set(taken)
        available = sorted(values)
        return (available if len(taken) == 0 else
            [x for x in available if x not in taken])

    def _take(self, id_):
        try:
            self.client.create(
                "{path}/{id}".format(
                    path=self._lock_path,
                    id=id_),
                self.id,
                ephemeral=True)
            value, stat = self.client.retry(self.client.get,
                "{path}/{id}".format(path=self._entries_path, id=id_))
        except (NoNodeError, NodeExistsError):
            # Item is already consumed or locked
            return None
        return (id_, value)

    def _ensure_paths(self):
        if not self.ensured_path:
            self._create_if_not_exists(self._lock_path)
            self._create_if_not_exists(self._entries_path)
            self.ensured_path = True

    def _create_if_not_exists(self, path):
        if not self.client.exists(path):
            try:
                self.client.create(path, makepath=True)
            except NodeExistsError:
                # Someone created one for us
                pass
