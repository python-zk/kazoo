"""Zookeeper Counter"""

from kazoo.exceptions import BadVersionError
from kazoo.retry import ForceRetryError


class Counter(object):
    """Kazoo Counter

    Example usage:

    .. code-block:: python

        zk = KazooClient()
        counter = zk.Counter("/int")
        counter += 2
        counter -= 1
        print(counter.value)

        counter = zk.Counter("/float", default=1.0)
        counter += 2.0
        print(counter.value)

    """
    def __init__(self, client, path, default=0):
        """Create a Kazoo Counter

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The counter path to use.
        :param default: The default value.

        """
        self.client = client
        self.path = path
        self.default = default
        self.default_type = type(default)
        self._ensured_path = False

    def _ensure_node(self):
        if not self._ensured_path:
            # make sure our parent node exists
            self.client.ensure_path(self.path)
            self._ensured_path = True

    def _value(self):
        self._ensure_node()
        old, stat = self.client.get(self.path)
        old = old.decode('ascii') if old is not b'' else self.default
        version = stat.version
        data = self.default_type(old)
        return data, version

    @property
    def value(self):
        return self._value()[0]

    def _change(self, value):
        if not isinstance(value, self.default_type):
            raise TypeError('invalid type for value change')
        self.client.retry(self._inner_change, value)
        return self

    def _inner_change(self, value):
        data, version = self._value()
        data = repr(data + value).encode('ascii')
        try:
            self.client.set(self.path, data, version=version)
        except BadVersionError:  # pragma: nocover
            raise ForceRetryError()

    def __add__(self, value):
        self._change(value)
        return self

    def __sub__(self, value):
        self._change(-value)
        return self
