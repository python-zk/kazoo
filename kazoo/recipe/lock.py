"""Zookeeper Locking Implementations"""
import threading
import uuid

from kazoo.retry import ForceRetryError
from kazoo.exceptions import CancelledError
from kazoo.exceptions import NoNodeException


class Lock(object):
    """Kazoo Basic Lock

    Example usage with a :class:`~kazoo.client.KazooClient` instance::

        zk = KazooClient()
        lock = zk.Lock("/lockpath", "my identifier")
        with lock:  # blocks waiting for lock acquisition
            # do something with the lock

    """
    _LOCK_NAME = '_lock_'

    def __init__(self, client, path, contender_name=None):
        """Create a Kazoo Lock

        :param client: A :class:`~kazoo.client.KazooClient` instance
        :param path: The lock path to use
        :param contender_name: Name to use for this lock contender. This
                               can be useful for querying to see who the
                               current lock contenders are.

        """
        self.client = client
        self.path = path

        # some data is written to the node. this can be queries via
        # get_contenders() to see who is contending for the lock
        self.data = str(contender_name or "")

        self.condition = threading.Condition()

        # props to Netflix Curator for this trick. It is possible for our
        # create request to succeed on the server, but for a failure to
        # prevent us from getting back the full path name. We prefix our
        # lock name with a uuid and can check for its presence on retry.
        self.prefix = uuid.uuid4().hex + self._LOCK_NAME
        self.create_path = self.path + "/" + self.prefix

        self.create_tried = False

        self.is_acquired = False

        self.assured_path = False

        self.cancelled = False

    def cancel(self):
        """Cancel a pending lock acquire"""
        with self.condition:
            self.cancelled = True
            self.condition.notify_all()

    def acquire(self):
        """Acquire the mutex, blocking until it is obtained"""
        try:
            self.client.retry(self._inner_acquire)

            self.is_acquired = True

        except Exception:
            # if we did ultimately fail, attempt to clean up
            self._best_effort_cleanup()
            self.cancelled = False
            raise

    def _inner_acquire(self):
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

            # bail out with an exception if cancellation has been requested
            if self.cancelled:
                raise CancelledError()

            children = self._get_sorted_children()

            try:
                our_index = children.index(node)
            except ValueError:
                # somehow we aren't in the children -- probably we are
                # recovering from a session failure and our ephemeral
                # node was removed
                raise ForceRetryError()

            #noinspection PySimplifyBooleanCheck
            if our_index == 0:
                # we have the lock
                return True

            # otherwise we are in the mix. watch predecessor and bide our time
            predecessor = self.path + "/" + children[our_index - 1]
            with self.condition:
                if self.client.exists(predecessor, self._watch_predecessor):
                    self.condition.wait()

    def _watch_predecessor(self, event):
        with self.condition:
            self.condition.notify_all()

    def _get_sorted_children(self):
        children = self.client.get_children(self.path)

        # can't just sort directly: the node names are prefixed by uuids
        lockname = self._LOCK_NAME
        children.sort(key=lambda c: c[c.find(lockname) + len(lockname):])
        return children

    def _find_node(self):
        children = self.client.get_children(self.path)
        for child in children:
            if child.startswith(self.prefix):
                return child
        return None

    def _best_effort_cleanup(self):
        try:
            node = self._find_node()
            if node:
                self.client.delete(self.path + "/" + node)
        except Exception:
            pass

    def release(self):
        """Release the mutex immediately"""
        return self.client.retry(self._inner_release)

    def _inner_release(self):
        if not self.is_acquired:
            return False

        self.client.delete(self.path + "/" + self.node)

        self.is_acquired = False
        self.node = None

        return True

    def get_contenders(self):
        """Return an ordered list of the current contenders for the lock

        .. note::

            If the contenders did not set a contender_name, it will appear
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
                contenders.append(data)
            except NoNodeException:
                pass
        return contenders

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
