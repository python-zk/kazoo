import os
import struct
import threading
import time
import uuid
from collections import namedtuple
from unittest.mock import patch

import pytest

from kazoo.exceptions import ConnectionLoss
from kazoo.protocol.connection import _CONNECTION_DROP
from kazoo.protocol.serialization import (
    Connect,
    int_struct,
    write_string,
)
from kazoo.protocol.states import KazooState
from kazoo.tests.util import CI, CI_ZK_VERSION, wait


class Delete(namedtuple("Delete", "path version")):
    type = 2

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        raise ValueError("oh my")


class TestConnectionHandler:
    def test_bad_deserialization(self, zkclient):
        async_object = zkclient.handler.async_result()
        zkclient._queue.append((Delete(zkclient.chroot, -1), async_object))
        zkclient._connection._write_sock.send(b"\0")

        with pytest.raises(ValueError):
            async_object.get()

    def test_with_bad_sessionid(self, zkensemble):
        ev = threading.Event()

        def expired(state):
            if state == KazooState.CONNECTED:
                ev.set()

        password = os.urandom(16)
        client = zkensemble.get_client(client_id=(82838284824, password))
        client.add_listener(expired)
        client.start()
        try:
            ev.wait(15)
            assert ev.is_set()
        finally:
            client.stop()

    def test_connection_read_timeout(self, zkclient):
        ev = threading.Event()
        path = "/" + uuid.uuid4().hex
        handler = zkclient.handler
        _select = handler.select
        _socket = zkclient._connection._socket

        def delayed_select(*args, **kwargs):
            result = _select(*args, **kwargs)
            if len(args[0]) == 1 and _socket in args[0]:
                # for any socket read, simulate a timeout
                return [], [], []
            return result

        def back(state):
            if state == KazooState.CONNECTED:
                ev.set()

        zkclient.add_listener(back)
        zkclient.create(path, b"1")
        try:
            handler.select = delayed_select
            with pytest.raises(ConnectionLoss):
                zkclient.get(path)
        finally:
            handler.select = _select
        # the client reconnects automatically
        ev.wait(5)
        assert ev.is_set()
        assert zkclient.get(path)[0] == b"1"

    def test_connection_write_timeout(self, zkclient):
        ev = threading.Event()
        path = "/" + uuid.uuid4().hex
        handler = zkclient.handler
        _select = handler.select
        _socket = zkclient._connection._socket

        def delayed_select(*args, **kwargs):
            result = _select(*args, **kwargs)
            if _socket in args[1]:
                # for any socket write, simulate a timeout
                return [], [], []
            return result

        def back(state):
            if state == KazooState.CONNECTED:
                ev.set()

        zkclient.add_listener(back)

        try:
            handler.select = delayed_select
            with pytest.raises(ConnectionLoss):
                zkclient.create(path)
        finally:
            handler.select = _select
        # the client reconnects automatically
        ev.wait(5)
        assert ev.is_set()
        assert zkclient.exists(path) is None

    def test_connection_deserialize_fail(self, zkclient):
        ev = threading.Event()
        path = "/" + uuid.uuid4().hex
        handler = zkclient.handler
        _select = handler.select
        _socket = zkclient._connection._socket

        def delayed_select(*args, **kwargs):
            result = _select(*args, **kwargs)
            if _socket in args[1]:
                # for any socket write, simulate a timeout
                return [], [], []
            return result

        def back(state):
            if state == KazooState.CONNECTED:
                ev.set()

        zkclient.add_listener(back)

        deserialize_ev = threading.Event()

        def bad_deserialize(_bytes, offset):
            deserialize_ev.set()
            raise struct.error()

        # force the connection to die but, on reconnect, cause the
        # server response to be non-deserializable. ensure that the client
        # continues to retry. This partially reproduces a rare bug seen
        # in production.

        with patch.object(Connect, "deserialize") as mock_deserialize:
            mock_deserialize.side_effect = bad_deserialize
            try:
                handler.select = delayed_select
                with pytest.raises(ConnectionLoss):
                    zkclient.create(path)
            finally:
                handler.select = _select
            # the client reconnects automatically but the first attempt will
            # hit a deserialize failure. wait for that.
            deserialize_ev.wait(5)
            assert deserialize_ev.is_set()

        # this time should succeed
        ev.wait(5)
        assert ev.is_set()
        assert zkclient.exists(path) is None

    def test_connection_close(self, zkclient):
        with pytest.raises(Exception):
            zkclient.close()
        zkclient.stop()
        zkclient.close()

        # should be able to restart
        zkclient.start()

    def test_connection_sock(self, zkclient):
        read_sock = zkclient._connection._read_sock
        write_sock = zkclient._connection._write_sock

        assert read_sock is not None
        assert write_sock is not None

        # stop client and socket should not yet be closed
        zkclient.stop()
        assert read_sock is not None
        assert write_sock is not None

        read_sock.getsockname()
        write_sock.getsockname()

        # close client, and sockets should be closed
        zkclient.close()

        # Todo check socket closing

        # start client back up. should get a new, valid socket
        zkclient.start()
        read_sock = zkclient._connection._read_sock
        write_sock = zkclient._connection._write_sock

        assert read_sock is not None
        assert write_sock is not None
        read_sock.getsockname()
        write_sock.getsockname()

    def test_dirty_sock(self, zkclient):
        read_sock = zkclient._connection._read_sock
        write_sock = zkclient._connection._write_sock

        # add a stray byte to the socket and ensure that doesn't
        # blow up client. simulates case where some error leaves
        # a byte in the socket which doesn't correspond to the
        # request queue.
        write_sock.send(b"\0")

        # eventually this byte should disappear from socket
        wait(lambda: zkclient.handler.select([read_sock], [], [], 0)[0] == [])


class TestConnectionDrop:
    def test_connection_dropped(self, zkclient):
        ev = threading.Event()

        def back(state):
            if state == KazooState.CONNECTED:
                ev.set()

        # create a node with a large value and stop the ZK node
        path = "/" + uuid.uuid4().hex
        zkclient.create(path)
        zkclient.add_listener(back)
        result = zkclient.set_async(path, b"a" * 1000 * 1024)
        zkclient._call(_CONNECTION_DROP, None)

        with pytest.raises(ConnectionLoss):
            result.get()
        # we have a working connection to a new node
        ev.wait(30)
        assert ev.is_set()


@pytest.mark.zk_features(require=["readonly"])
@pytest.mark.zk_version(">=3.4")
def test_read_only(zkensemble):
    """Test Read-Only mode connection behavior and operation constraints.

    Verifies:
    1. Healthy ensemble accepts connections in standard CONNECTED state.
    2. Partitioned ensemble (quorum lost) rejects `read_only=False` clients.
       When a Read-Only server receives a Connect request with
       `read_only=False`, it immediately closes the socket. The client
       connection retry loop fails to establish a read-write session,
       raising `KazooTimeoutError`.
    3. Partitioned ensemble accepts `read_only=True` clients in
       `CONNECTED_RO` state using node-local sessions.
    4. Read operations (e.g. `get_children`) succeed in `CONNECTED_RO` state.
    5. Write operations (e.g. `create`) fail and raise
       `NotReadOnlyCallError`.
    """
    from kazoo.exceptions import NotReadOnlyCallError
    from kazoo.handlers.threading import KazooTimeoutError
    from kazoo.protocol.states import KeeperState

    # 1. Verify client connects normally when ensemble is healthy
    client = zkensemble.get_client(read_only=True)
    client.start()
    assert client.client_state == KeeperState.CONNECTED
    client.stop()
    client.close()

    # 2. Stop 2 of 3 nodes to break quorum and force zoo1 into Read-Only mode
    zk_stop_threads = [
        threading.Thread(target=zkensemble.stop, args=("zoo2",), daemon=True),
        threading.Thread(target=zkensemble.stop, args=("zoo3",), daemon=True),
    ]
    for thread in zk_stop_threads:
        thread.start()
    for thread in zk_stop_threads:
        thread.join()

    ro_client = zkensemble.get_client(
        hosts=f"{zkensemble.zk_ip}:{zkensemble.zk1_port}", read_only=True
    )
    rw_client = zkensemble.get_client(
        hosts=f"{zkensemble.zk_ip}:{zkensemble.zk1_port}", read_only=False
    )
    try:
        # Sleep to allow zoo1's leader election rounds to time out and
        # start ReadOnlyZooKeeperServer
        time.sleep(12)

        # 3. Negative test: connecting with read_only=False to a Read-Only
        # server fails. The server drops non-RO connect packets, causing
        # client.start() to time out.
        with pytest.raises(KazooTimeoutError):
            rw_client.start(timeout=5)

        # 4. Positive test: connecting with read_only=True succeeds in
        # CONNECTED_RO mode
        ro_client.start(timeout=15)
        assert ro_client.client_state == KeeperState.CONNECTED_RO

        # 5. Test read-only command succeeds
        assert isinstance(ro_client.get_children("/"), list)

        # 6. Test write command raises NotReadOnlyCallError
        with pytest.raises(NotReadOnlyCallError):
            ro_client.create("/fred")
    finally:
        rw_client.stop()
        rw_client.close()
        ro_client.stop()
        ro_client.close()
        zkensemble.start("zoo2")
        zkensemble.start("zoo3")


# class TestUnorderedXids(KazooTestCase):
#     def setUp(self):
#         super(TestUnorderedXids, self).setUp()

#         self.connection = self.client._connection
#         self.connection_routine = self.connection._connection_routine

#         self._pending = self.client._pending
#         self.client._pending = _naughty_deque()

#     def tearDown(self):
#         self.client._pending = self._pending
#         super(TestUnorderedXids, self).tearDown()

#     def _get_client(self, **kwargs):
#         # overrides for patching zk_loop
#         c = KazooTestCase._get_client(self, **kwargs)
#         self._zk_loop = c._connection.zk_loop
#         self._zk_loop_errors = []
#         c._connection.zk_loop = self._zk_loop_func
#         return c

#     def _zk_loop_func(self, *args, **kwargs):
#         # patched zk_loop which will catch and collect all RuntimeError
#         try:
#             self._zk_loop(*args, **kwargs)
#         except RuntimeError as e:
#             self._zk_loop_errors.append(e)

#     def test_xids_mismatch(self):
#         from kazoo.protocol.states import KeeperState

#         ev = threading.Event()
#         error_stack = []

#         @self.client.add_listener
#         def listen(state):
#             if self.client.client_state == KeeperState.CLOSED:
#                 ev.set()

#         def log_exception(*args):
#             error_stack.append((args, sys.exc_info()))

#         self.connection.logger.exception = log_exception

#         ev.clear()
#         with pytest.raises(RuntimeError):
#             self.client.get_children("/")

#         ev.wait()
#         assert self.client.connected is False
#         assert self.client.state == "LOST"
#         assert self.client.client_state == KeeperState.CLOSED

#         args, exc_info = error_stack[-1]
#         assert args == ("Unhandled exception in connection loop",)
#         assert exc_info[0] == RuntimeError

#         self.client.handler.sleep_func(0.2)
#         assert not self.connection_routine.is_alive()
#         assert len(self._zk_loop_errors) == 1
#         assert self._zk_loop_errors[0] == exc_info[1]


# class _naughty_deque(deque):
#     def append(self, s):
#         request, async_object, xid = s
#         return deque.append(self, (request, async_object, xid + 1))  # +1s
