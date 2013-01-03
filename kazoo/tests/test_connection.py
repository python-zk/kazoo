from collections import namedtuple
import os
import threading
import time
import uuid

from nose import SkipTest
from nose.tools import eq_
from nose.tools import raises

from kazoo.exceptions import ConnectionLoss
from kazoo.protocol.serialization import (
    write_string,
    int_struct
)
from kazoo.protocol.states import KazooState
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

    def test_connection_read_timeout(self):
        client = self.client
        ev = threading.Event()
        path = "/" + uuid.uuid4().hex
        handler = client.handler
        _select = handler.select
        _socket = client._connection._socket

        def delayed_select(*args, **kwargs):
            result = _select(*args, **kwargs)
            if _socket in args[0]:
                # for any socket read, simulate a timeout
                return [], [], []
            return result

        def back(state):
            if state == KazooState.CONNECTED:
                ev.set()

        client.add_listener(back)
        client.create(path, b"1")
        try:
            handler.select = delayed_select
            self.assertRaises(ConnectionLoss, client.get, path)
        finally:
            handler.select = _select
        # the client reconnects automatically
        ev.wait(5)
        eq_(ev.is_set(), True)
        eq_(client.get(path)[0], b"1")

    def test_connection_write_timeout(self):
        client = self.client
        ev = threading.Event()
        path = "/" + uuid.uuid4().hex
        handler = client.handler
        _select = handler.select
        _socket = client._connection._socket

        def delayed_select(*args, **kwargs):
            result = _select(*args, **kwargs)
            if _socket in args[1]:
                # for any socket write, simulate a timeout
                return [], [], []
            return result

        def back(state):
            if state == KazooState.CONNECTED:
                ev.set()
        client.add_listener(back)

        try:
            handler.select = delayed_select
            self.assertRaises(ConnectionLoss, client.create, path)
        finally:
            handler.select = _select
        # the client reconnects automatically
        ev.wait(5)
        eq_(ev.is_set(), True)
        eq_(client.exists(path), None)


class TestConnectionDropped(KazooTestCase):

    def setUp(self):
        self.setup_zookeeper(randomize_hosts=False)

    def tearDown(self):
        self.cluster.start()
        self.client.stop()

    def test_connection_dropped(self):
        client = self.client
        client.start()
        ev = threading.Event()

        def back(state):
            if state == KazooState.CONNECTED:
                ev.set()

        # make sure we are connected to cluster node 0
        eq_(self.cluster[0].server_info.client_port,
            client._connection._socket.getpeername()[1])
        # create a node with a large value and stop the ZK node
        path = "/" + uuid.uuid4().hex
        client.create(path)
        client.add_listener(back)
        result = client.set_async(path, b'a' * 1000 * 1024)
        self.cluster[0].stop()
        self.assertRaises(ConnectionLoss, result.get)
        # we have a working connection to a new node
        ev.wait(30)
        eq_(ev.is_set(), True)
        client.delete(path)


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
