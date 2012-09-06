from collections import namedtuple
import os
import threading

from nose.tools import eq_
from nose.tools import raises

from kazoo.protocol.serialization import (
    write_string,
    int_struct
)
from kazoo.testing import KazooTestCase


class Delete(namedtuple('Delete', 'path version')):
    type = 2

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(self, bytes, offset):
        raise ValueError("oh my")


class TestConnectionHandler(KazooTestCase):
    def test_bad_deserialization(self):
        async_object = self.client.handler.async_result()
        self.client._queue.put((Delete(self.client.chroot, -1), async_object))

        @raises(ValueError)
        def testit():
            async_object.get()
        testit()

    def test_with_bad_sessionid(self):
        from kazoo.protocol.states import KazooState
        ev = threading.Event()

        def expired(state):
            if state == KazooState.CONNECTED:
                ev.set()

        password = os.urandom(16)
        client = self._get_client(client_id=(82838284824L, password))
        client.add_listener(expired)
        client.start()
        ev.wait(15)
        eq_(ev.is_set(), True)
        client.stop()
