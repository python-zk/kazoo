"""Zookeeper Counter

:Maintainer: None
:Status: Unknown

"""
from __future__ import annotations

import struct
from typing import cast, TYPE_CHECKING

from kazoo.exceptions import BadVersionError
from kazoo.retry import ForceRetryError

if TYPE_CHECKING:
    from typing import Optional, Tuple, Type, Union

    from kazoo.client import KazooClient

    CountT = Union[int, float]


class Counter(object):
    """Kazoo Counter

    A shared counter of either int or float values. Changes to the
    counter are done atomically. The general retry policy is used to
    retry operations if concurrent changes are detected.

    The data is marshaled using `repr(value)` and converted back using
    `type(counter.default)(value)` both using an ascii encoding. As
    such other data types might be used for the counter value.

    If you would like to support clients updating the same znode path using
    either kazoo's counter recipe or curator's SharedCount recipe, you will
    need to enable the support_curator flag. This flag limits
    support to integers only and does not use ascii encoding as described
    above.

    Counter changes can raise
    :class:`~kazoo.exceptions.BadVersionError` if the retry policy
    wasn't able to apply a change.

    Example usage:

    .. code-block:: python

        zk = KazooClient()
        zk.start()
        counter = zk.Counter("/int")
        counter += 2
        counter -= 1
        counter.value == 1
        counter.pre_value == 2
        counter.post_value == 1

        counter = zk.Counter("/float", default=1.0)
        counter += 2.0
        counter.value == 3.0
        counter.pre_value == 1.0
        counter.post_value == 3.0

        counter = zk.Counter("/curator", support_curator=True)
        counter += 2
        counter -= 1
        counter.value == 1
        counter.pre_value == 2
        counter.post_value == 1

    """

    def __init__(
        self,
        client: KazooClient,
        path: str,
        default: CountT = 0,
        support_curator: bool = False,
    ):
        """Create a Kazoo Counter

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The counter path to use.
        :param default: The default value to use for new counter paths.
        :param support_curator: Enable if support for curator's SharedCount
                                recipe is desired.

        """
        self.client = client
        self.path = path
        self.default: CountT = default
        self.default_type: Type[CountT] = type(default)
        self.support_curator = support_curator
        self._ensured_path = False
        self.pre_value: Optional[CountT] = None
        self.post_value: Optional[CountT] = None
        if self.support_curator and not isinstance(self.default, int):
            raise TypeError(
                "when support_curator is enabled the default "
                "type must be an int"
            )

    def _ensure_node(self) -> None:
        if not self._ensured_path:
            # make sure our node exists
            self.client.ensure_path(self.path)
            self._ensured_path = True

    def _value(self) -> Tuple[CountT, int]:
        self._ensure_node()
        old, stat = self.client.get(self.path)
        if self.support_curator:
            parsed_old: Union[int, float, str] = (
                cast(int, struct.unpack(">i", old)[0])
                if old != b""
                else self.default
            )
        else:
            parsed_old = old.decode("ascii") if old != b"" else self.default
        version = stat.version
        data = self.default_type(parsed_old)
        return data, version

    @property
    def value(self) -> CountT:
        return self._value()[0]

    def _change(self, value: CountT) -> "Counter":
        if not isinstance(value, self.default_type):
            raise TypeError("invalid type for value change")
        self.client.retry(self._inner_change, value)
        return self

    def _inner_change(self, value: CountT) -> None:
        self.pre_value, version = self._value()
        post_value = self.pre_value + value
        if self.support_curator:
            data = struct.pack(">i", post_value)
        else:
            data = repr(post_value).encode("ascii")
        try:
            self.client.set(self.path, data, version=version)
        except BadVersionError:  # pragma: nocover
            self.post_value = None
            raise ForceRetryError()
        self.post_value = post_value

    def __add__(self, value: CountT) -> "Counter":
        """Add value to counter."""
        return self._change(value)

    def __sub__(self, value: CountT) -> "Counter":
        """Subtract value from counter."""
        return self._change(-value)
