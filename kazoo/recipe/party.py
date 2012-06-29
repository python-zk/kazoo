"""Party

A zookeeper pool of party members. The :class:`Party` object can be
used for determining members of a party.

"""
import uuid

from kazoo.exceptions import NodeExistsException, NoNodeException


class Party(object):
    """Simple pool of participating processes"""
    _NODE_NAME = "__party__"

    def __init__(self, client, path, identifier=None):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance
        :param path: The party path to use
        :param identifier: An identifier to use for this member of the party
                           when participating.

        """
        self.client = client
        self.path = path

        self.data = str(identifier or "")

        self.node = uuid.uuid4().hex + self._NODE_NAME
        self.create_path = self.path + "/" + self.node

        self.ensured_path = False
        self.participating = False

    def join(self):
        """Join the party"""
        return self.client.retry(self._inner_join)

    def _inner_join(self):
        if not self.ensured_path:
            # make sure our election parent node exists
            self.client.ensure_path(self.path)
            self.ensured_path = True

        try:
            self.client.create(self.create_path, self.data, ephemeral=True)
            self.participating = True
        except NodeExistsException:
            # node was already created, perhaps we are recovering from a
            # suspended connection
            self.participating = True

    def leave(self):
        """Leave the party"""
        return self.client.retry(self._inner_leave)

    def _inner_leave(self):
        try:
            self.client.delete(self.create_path)
        except NoNodeException:
            return False

        return True

    def get_participants(self):
        """
        Get a list of participating clients' data values
        """
        if not self.ensured_path:
            # make sure our election parent node exists
            self.client.ensure_path(self.path)
            self.ensured_path = True

        children = self._get_children()
        participants = []
        for child in children:
            try:
                d, _ = self.client.retry(self.client.get, self.path +
                                         "/" + child)
                participants.append(d)
            except NoNodeException:
                pass
        return participants

    def get_participant_count(self):
        """Return a count of participating clients"""
        if not self.ensured_path:
            # make sure our election parent node exists
            self.client.ensure_path(self.path)
            self.ensured_path = True
        return len(self._get_children())

    def _get_children(self):
        children = self.client.retry(self.client.get_children, self.path)
        return filter(lambda child: self._NODE_NAME in child, children)
