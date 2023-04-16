"""Party

:Maintainer: Ben Bangert <ben@groovie.org>
:Status: Production

A Zookeeper pool of party members. The :class:`Party` object can be
used for determining members of a party.

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
import uuid

from kazoo.exceptions import NodeExistsError, NoNodeError

if TYPE_CHECKING:
    from typing import Generator, List, Optional

    from kazoo.client import KazooClient


class BaseParty(ABC):
    """Base implementation of a party."""

    def __init__(
        self, client: KazooClient, path: str, identifier: Optional[str] = None
    ):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The party path to use.
        :param identifier: An identifier to use for this member of the
                           party when participating.

        """
        self.client = client
        self.path = path
        self.data = str(identifier or "").encode("utf-8")
        self.ensured_path = False
        self.participating = False

    def _ensure_parent(self) -> None:
        if not self.ensured_path:
            # make sure our parent node exists
            self.client.ensure_path(self.path)
            self.ensured_path = True

    def join(self) -> None:
        """Join the party"""
        return self.client.retry(self._inner_join)

    def _inner_join(self) -> None:
        self._ensure_parent()
        try:
            self.client.create(self.create_path, self.data, ephemeral=True)
            self.participating = True
        except NodeExistsError:
            # node was already created, perhaps we are recovering from a
            # suspended connection
            self.participating = True

    def leave(self) -> bool:
        """Leave the party"""
        self.participating = False
        return self.client.retry(self._inner_leave)

    def _inner_leave(self) -> bool:
        try:
            self.client.delete(self.create_path)
        except NoNodeError:
            return False
        return True

    def __len__(self) -> int:
        """Return a count of participating clients"""
        self._ensure_parent()
        return len(self._get_children())

    def _get_children(self) -> List[str]:
        return self.client.retry(self.client.get_children, self.path)

    @property
    @abstractmethod
    def create_path(self) -> str:
        ...


class Party(BaseParty):
    """Simple pool of participating processes"""

    _NODE_NAME = "__party__"

    def __init__(
        self, client: KazooClient, path: str, identifier: Optional[str] = None
    ):
        BaseParty.__init__(self, client, path, identifier=identifier)
        self.node = uuid.uuid4().hex + self._NODE_NAME
        self._create_path = self.path + "/" + self.node

    @property
    def create_path(self) -> str:
        return self._create_path

    def __iter__(self) -> Generator[str, None, None]:
        """Get a list of participating clients' data values"""
        self._ensure_parent()
        children = self._get_children()
        for child in children:
            try:
                d, _ = self.client.retry(
                    self.client.get, self.path + "/" + child
                )
                yield d.decode("utf-8")
            except NoNodeError:  # pragma: nocover
                pass

    def _get_children(self) -> List[str]:
        children = BaseParty._get_children(self)
        return [c for c in children if self._NODE_NAME in c]


class ShallowParty(BaseParty):
    """Simple shallow pool of participating processes

    This differs from the :class:`Party` as the identifier is used in
    the name of the party node itself, rather than the data. This
    places some restrictions on the length as it must be a valid
    Zookeeper node (an alphanumeric string), but reduces the overhead
    of getting a list of participants to a single Zookeeper call.

    """

    def __init__(
        self, client: KazooClient, path: str, identifier: Optional[str] = None
    ):
        BaseParty.__init__(self, client, path, identifier=identifier)
        self.node = "-".join([uuid.uuid4().hex, self.data.decode("utf-8")])
        self._create_path = self.path + "/" + self.node

    @property
    def create_path(self) -> str:
        return self._create_path

    def __iter__(self) -> Generator[str, None, None]:
        """Get a list of participating clients' identifiers"""
        self._ensure_parent()
        children = self._get_children()
        for child in children:
            yield child[child.find("-") + 1 :]
