from collections import namedtuple
import os
import threading
import time

from nose import SkipTest
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
        client = self._get_client(client_id=(82838284824, password))
        client.add_listener(expired)
        client.start()
        ev.wait(15)
        eq_(ev.is_set(), True)
        client.stop()


class TestReadOnlyMode(KazooTestCase):
    def setUp(self):
        self.setup_zookeeper(read_only=True)
        ver = self.client.server_version()
        if ver[1] < 4:
            raise SkipTest("Must use zookeeper 3.4 or above")

    def tearDown(self):
        self.client.stop()

    def test_read_only(self):
        from kazoo.exceptions import NotReadOnlyCallError
        from kazoo.protocol.states import KeeperState

        client = self.client
        states = []
        ev = threading.Event()

        @client.add_listener
        def listen(state):
            states.append(state)
            if client.client_state == KeeperState.CONNECTED_RO:
                ev.set()
        try:
            self.cluster[1].stop()
            self.cluster[2].stop()
            ev.wait(6)
            eq_(client.client_state, KeeperState.CONNECTED_RO)

            # Test read only command
            eq_(client.get_children('/'), [])

            # Test error with write command
            @raises(NotReadOnlyCallError)
            def testit():
                client.create('/fred')
            testit()

            # Wait for a ping
            time.sleep(15)
        finally:
            self.cluster[1].run()
            self.cluster[2].run()
