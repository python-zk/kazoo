"""Queue

A Zookeeper based queue implementation.
"""


class Queue(object):
    """A distributed queue."""

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

    def qsize(self):
        """Return queue size."""
        self._ensure_parent()
        _, stat = self.client.retry(self.client.get, self.path)
        return stat.children_count

    __len__ = qsize

    def get(self):
        """Get and remove an item from the queue."""
        self._ensure_parent()
        children = sorted(self.client.get_children(self.path))
        name = children.pop(0)
        data, stat = self.client.get(self.path + "/" + name)
        self.client.delete(self.path + "/" + name)
        return data

    def put(self, value):
        """Put an item into the queue.
        :param value: Byte string to put into the queue.
        """
        if not isinstance(value, bytes):
            raise TypeError("value must be a byte string")
        self._ensure_parent()
        self.client.create(self.path + "/" + self.prefix, value,
            sequence=True)
