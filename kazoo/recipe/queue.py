"""Queue

A Zookeeper based queue implementation.
"""

from kazoo.exceptions import NoNodeError
from kazoo.retry import ForceRetryError


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
