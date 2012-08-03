"""Zookeeper Barriers"""
from kazoo.client import EventType
from kazoo.exceptions import NoNodeException


class Barrier(object):
    """Kazoo Barrier

    Implements a barrier to block processing of a set of nodes until
    a condition is met at which point the nodes will be allowed to
    proceed. The barrier is in place if its node exists.

    .. warning::

        The :meth:`wait` function does not handle connection loss and
        may raise :exc:`~kazoo.exceptions.ConnectionLossException` if
        the connection is lost while waiting.

    """
    def __init__(self, client, path):
        """Create a Kazoo Barrier

        :param client: A :class:`~kazoo.client.KazooClient` instance
        :param path: The barrier path to use

        """
        self.client = client
        self.path = path

    def create(self):
        """Establish the barrier if it doesn't exist already"""
        self.client.retry(self.client.ensure_path, self.path)

    def remove(self):
        """Remove the barrier

        :returns: Whether the barrier actually needed to be removed.
        :rtype: bool

        """
        try:
            self.client.retry(self.client.delete, self.path)
            return True
        except NoNodeException:
            return False

    def wait(self, timeout=None):
        """Wait on the barrier to be cleared

        :returns: True if the barrier has been cleared, otherwise
                  False.
        :rtype: bool

        """
        cleared = self.client.handler.event_object()

        def wait_for_clear(event):
            if event.type == EventType.DELETED:
                cleared.set()

        exists = self.client.exists(self.path, watch=wait_for_clear)
        if not exists:
            return True

        cleared.wait(timeout)
        return cleared.is_set()
