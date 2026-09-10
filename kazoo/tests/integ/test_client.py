from __future__ import annotations

import socket
import ssl
import threading
import time
import uuid

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from kazoo import security
from kazoo.exceptions import (
    AuthFailedError,
    BadArgumentsError,
    BadVersionError,
    ConfigurationError,
    ConnectionClosedError,
    ConnectionLoss,
    InvalidACLError,
    NoAuthError,
    NoNodeError,
    NodeExistsError,
    SessionExpiredError,
    KazooException,
)
from kazoo.protocol.connection import _CONNECTION_DROP
from kazoo.protocol.states import KeeperState, KazooState

if TYPE_CHECKING:
    from kazoo.client import KazooClient


class TestClientTransitions:
    def test_connection_and_disconnection(self, zkclient):
        client = zkclient

        states = []
        rc = client.handler.event_object()

        @client.add_listener
        def listener(state):
            states.append(state)
            if state == KazooState.CONNECTED:
                rc.set()

        client.stop()
        assert states == [KazooState.LOST]
        states.pop()

        client.start()
        rc.wait(2)
        assert states == [KazooState.CONNECTED]
        rc.clear()
        states.pop()

        client.harness_expire_session()
        rc.wait(2)

        req_states = [KazooState.LOST, KazooState.CONNECTED]
        assert states == req_states


class TestAuthentication:
    def _makeAuth(self, *args, **kwargs):
        return security.make_digest_acl(*args, **kwargs)

    def test_auth(self, zkclient, zkensemble):
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = self._makeAuth(username, password, all=True)

        client = zkclient
        client.add_auth("digest", digest_auth)
        client.default_acl = (acl,)

        # Create a second client
        eve = zkensemble.get_client()
        eve.chroot = client.chroot
        eve.start()
        try:
            client.create("/1")
            client.create("/1/2")
            client.ensure_path("/1/2/3")

            with pytest.raises(NoAuthError):
                eve.get("/1/2")

            # try again with the wrong auth token
            eve.add_auth("digest", "badbad:bad")

            with pytest.raises(NoAuthError):
                eve.get("/1/2")

        finally:
            # Ensure we remove the ACL protected nodes
            client.delete("/1", recursive=True)
            eve.stop()
            eve.close()

    def test_connect_auth(self, zkclient, zkensemble):
        client1 = zkclient

        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = self._makeAuth(username, password, all=True)

        client2 = zkensemble.get_client(auth_data=[("digest", digest_auth)])
        client2.chroot = client1.chroot
        client2.start()
        try:
            client2.create("/1", acl=(acl,))
            # Give ZK a chance to copy data to other ensemble nodes.
            # Follower reads are sequentially consistent, but may briefly lag
            # the leader until the commit is applied locally; sync flushes
            # the channel between client1's connected server and the leader.
            client1.sync("/1")

            with pytest.raises(NoAuthError):
                client1.get("/1")

        finally:
            client2.delete("/1")
            client2.stop()
            client2.close()

    def test_unicode_auth(self, zkclient, zkensemble):
        username = r"xe4/\hm"
        password = r"/\xe4hm"
        digest_auth = "%s:%s" % (username, password)
        acl = self._makeAuth(username, password, all=True)

        client = zkclient
        client.add_auth("digest", digest_auth)
        client.default_acl = (acl,)

        eve = zkensemble.get_client()
        eve.chroot = client.chroot
        eve.start()
        try:
            client.create("/1")
            client.ensure_path("/1/2/3")

            with pytest.raises(NoAuthError):
                eve.get("/1/2")

            # try again with the wrong auth token
            eve.add_auth("digest", "badbad:bad")

            with pytest.raises(NoAuthError):
                eve.get("/1/2")

        finally:
            # Ensure we remove the ACL protected nodes
            client.delete("/1", recursive=True)
            eve.stop()
            eve.close()

    def test_invalid_auth(self, zkclient):
        client = zkclient

        with pytest.raises(TypeError):
            client.add_auth("digest", ("user", "pass"))

        with pytest.raises(TypeError):
            client.add_auth(None, ("user", "pass"))

    def test_async_auth(self, zkclient):
        client = zkclient
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex
        digest_auth = "%s:%s" % (username, password)
        result = client.add_auth_async("digest", digest_auth)
        assert result.get() is True

    def test_async_auth_failure(self, zkclient):
        client = zkclient
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex
        digest_auth = "%s:%s" % (username, password)

        with pytest.raises(AuthFailedError):
            client.add_auth("unknown-scheme", digest_auth)

    def test_add_auth_on_reconnect(self, zkclient):
        client = zkclient
        client.add_auth("digest", "jsmith:jsmith")

        ev_lost = client.handler.event_object()
        ev_connected = client.handler.event_object()

        def listener(state):
            if state in (KazooState.SUSPENDED, KazooState.LOST):
                ev_lost.set()
            elif state == KazooState.CONNECTED and ev_lost.is_set():
                ev_connected.set()

        client.add_listener(listener)
        if client._connection._socket is not None:
            client._connection._socket.shutdown(socket.SHUT_RDWR)

        ev_connected.wait(15)
        assert ev_connected.is_set()
        assert ("digest", "jsmith:jsmith") in client.auth_data


class TestConnection:
    @staticmethod
    def make_condition():
        # FIXME: gevent??
        return threading.Condition()

    def test_chroot_warning(self, zkensemble):
        k = zkensemble.get_client()
        k.chroot = "abba"
        try:
            with mock.patch("warnings.warn") as mock_func:
                k.start()
                assert mock_func.called
        finally:
            k.stop()

    def test_session_expire(self, zkclient):
        from kazoo.protocol.states import KazooState

        client = zkclient

        cv = client.handler.event_object()

        def watch_events(event):
            if event == KazooState.LOST:
                cv.set()

        client.add_listener(watch_events)
        client.harness_expire_session()
        cv.wait(3)
        assert cv.is_set()

    def test_bad_session_expire(self, zkclient):
        from kazoo.protocol.states import KazooState

        client = zkclient

        cv = client.handler.event_object()
        ab = client.handler.event_object()

        def watch_events(event):
            if event == KazooState.LOST:
                ab.set()
                raise Exception("oops")
                cv.set()

        client.add_listener(watch_events)
        client.harness_expire_session()
        ab.wait(5.0)
        assert ab.is_set()
        cv.wait(0.5)
        assert not cv.is_set()

    def test_state_listener(self, zkclient):
        from kazoo.protocol.states import KazooState

        client = zkclient

        states = []
        condition = self.make_condition()

        def listener(state):
            with condition:
                states.append(state)
                condition.notify_all()

        client.stop()
        assert client.state == KazooState.LOST
        client.add_listener(listener)
        client.start(5)

        with condition:
            if not states:
                condition.wait(5)

        assert len(states) == 1
        assert states[0] == KazooState.CONNECTED

    def test_invalid_listener(self, zkclient):
        client = zkclient
        with pytest.raises(ConfigurationError):
            client.add_listener(15)

    def test_listener_only_called_on_real_state_change(self, zkclient):
        from kazoo.protocol.states import KazooState

        client = zkclient

        assert client.state == KazooState.CONNECTED
        called = [False]
        condition = client.handler.event_object()

        def listener(state):
            called[0] = True
            condition.set()

        client.add_listener(listener)
        client._make_state_change(KazooState.CONNECTED)
        condition.wait(3)
        assert called[0] is False

    def test_no_connection(self, zkclient):
        client = zkclient
        client.stop()
        assert client.connected is False
        assert client.client_id is None

        with pytest.raises(ConnectionClosedError):
            client.exists("/")

    def test_close_connecting_connection(self, zkclient):
        client = zkclient
        client.stop()
        ev = client.handler.event_object()

        def close_on_connecting(state):
            if state in (KazooState.CONNECTED, KazooState.LOST):
                ev.set()

        client.add_listener(close_on_connecting)
        client.start()

        # Wait until we connect
        ev.wait(5)
        ev.clear()
        client._call(_CONNECTION_DROP, client.handler.async_result())

        client.stop()

        # ...and then wait until the connection is lost
        ev.wait(5)

        with pytest.raises(ConnectionClosedError):
            client.create("/foobar")

    def test_double_start(self, zkensemble):
        client = zkensemble.get_client()
        client.start()
        assert client.connected is True
        client.start()
        assert client.connected is True

    def test_double_stop(self, zkensemble):
        client = zkensemble.get_client()
        client.start()

        client.stop()
        assert client.connected is False
        client.stop()
        assert client.connected is False

    @staticmethod
    def test_restart(zkensemble):
        client = zkensemble.get_client()
        client.start()

        assert client.connected is True
        client.restart()
        assert client.connected is True

    def test_closed(self, zkensemble):
        client = zkensemble.get_client()
        client.stop()

        write_sock = client._connection._write_sock

        # close the connection to free the socket
        client.close()
        assert client._connection._write_sock is None

        # sneak in and patch client to simulate race between a thread
        # calling stop(); close() and one running a command
        oldstate = client._state
        client._state = KeeperState.CONNECTED
        client._connection._write_sock = write_sock

        try:
            # simulate call made after write socket is closed
            with pytest.raises(ConnectionClosedError):
                client.exists("/")

            # simulate call made after write socket is set to None
            client._connection._write_sock = None

            with pytest.raises(ConnectionClosedError):
                client.exists("/")

        finally:
            # reset for teardown
            client._state = oldstate
            client._connection._write_sock = None

    def test_watch_trigger_expire(self, zkclient):
        client = zkclient
        cv = client.handler.event_object()

        client.create("/test", b"")

        def test_watch(event):
            cv.set()

        client.get("/test/", watch=test_watch)
        client.harness_expire_session()

        cv.wait(3)
        assert cv.is_set()


class TestClient:
    def _makeOne(self, *args):
        from kazoo.handlers.threading import SequentialThreadingHandler

        return SequentialThreadingHandler(*args)

    def test_server_version_retries_fail(self, zkclient):
        client = zkclient
        side_effects = [
            "",
            "zookeeper.version=",
            "zookeeper.version=1.",
            "zookeeper.ver",
        ]
        client.command = mock.MagicMock()
        client.command.side_effect = side_effects
        with pytest.raises(KazooException):
            client.server_version(retries=len(side_effects) - 1)

    def test_server_version_retries_eventually_ok(self, zkclient):
        client = zkclient
        actual_version = "zookeeper.version=1.2"
        side_effects = []
        for i in range(0, len(actual_version) + 1):
            side_effects.append(actual_version[0:i])
        client.command = mock.MagicMock()
        client.command.side_effect = side_effects
        assert client.server_version(retries=len(side_effects) - 1) == (1, 2)

    def test_client_id(self, zkclient):
        client = zkclient
        client_id = client.client_id
        assert type(client_id) is tuple
        # make sure password is of correct length
        assert len(client_id[1]) == 16

    def test_connected(self, zkclient):
        client = zkclient
        assert client.connected

    def test_create(self, zkclient):
        client = zkclient
        path = client.create("/1")
        assert path == "/1", f"{client.chroot} is wrong"
        assert client.exists("/1")

    def test_create_on_broken_connection(self, zkclient):
        client = zkclient

        client._state = KeeperState.EXPIRED_SESSION
        with pytest.raises(SessionExpiredError):
            client.create("/closedpath", b"bar")

        client._state = KeeperState.AUTH_FAILED
        with pytest.raises(AuthFailedError):
            client.create("/closedpath", b"bar")

        client.stop()
        client.close()

        with pytest.raises(ConnectionClosedError):
            client.create("/closedpath", b"bar")

    def test_create_null_data(self, zkclient):
        client = zkclient
        client.create("/nulldata", None)
        value, _ = client.get("/nulldata")
        assert value is None

    def test_create_empty_string(self, zkclient):
        client = zkclient
        client.create("/empty", b"")
        value, _ = client.get("/empty")
        assert value == b""

    def test_create_unicode_path(self, zkclient):
        client = zkclient
        path = client.create("/ascii")
        assert path == "/ascii"
        path = client.create("/\xe4hm")
        assert path == "/\xe4hm"

    def test_create_async_returns_unchrooted_path(self, zkclient):
        client = zkclient
        path = client.create_async("/1").get()
        assert path == "/1"

    def test_create_invalid_path(self, zkclient):
        client = zkclient
        with pytest.raises(TypeError):
            client.create(("a",))
        with pytest.raises(ValueError):
            client.create(".")
        with pytest.raises(ValueError):
            client.create("/a/../b")
        with pytest.raises(BadArgumentsError):
            client.create("/b\x00")
        with pytest.raises(BadArgumentsError):
            client.create("/b\x1e")

    def test_create_invalid_arguments(self, zkclient):
        from kazoo.security import OPEN_ACL_UNSAFE

        single_acl = OPEN_ACL_UNSAFE[0]
        client = zkclient
        with pytest.raises(TypeError):
            client.create("a", acl="all")
        with pytest.raises(TypeError):
            client.create("a", acl=single_acl)
        with pytest.raises(TypeError):
            client.create("a", value=["a"])
        with pytest.raises(TypeError):
            client.create("a", ephemeral="yes")
        with pytest.raises(TypeError):
            client.create("a", sequence="yes")
        with pytest.raises(TypeError):
            client.create("a", makepath="yes")

    def test_create_value(self, zkclient):
        client = zkclient
        client.create("/1", b"bytes")
        data, stat = client.get("/1")
        assert data == b"bytes"

    def test_create_unicode_value(self, zkclient):
        client = zkclient
        with pytest.raises(TypeError):
            client.create("/1", "\xe4hm")

    def test_create_large_value(self, zkclient):
        client = zkclient
        kb_512 = b"a" * (512 * 1024)
        client.create("/1", kb_512)
        assert client.exists("/1")
        mb_2 = b"a" * (2 * 1024 * 1024)
        with pytest.raises(ConnectionLoss):
            client.create("/2", mb_2)

    @pytest.mark.zk_version(">=3.4")
    def test_create_acl_duplicate(self, zkclient):
        from kazoo.security import OPEN_ACL_UNSAFE

        single_acl = OPEN_ACL_UNSAFE[0]
        client = zkclient
        client.create("/1", acl=[single_acl, single_acl])
        acls, stat = client.get_acls("/1")
        # ZK >3.4 removes duplicate ACL entries
        assert len(acls) == 1

    def test_create_acl_empty_list(self, zkclient):
        from kazoo.security import OPEN_ACL_UNSAFE

        client = zkclient
        client.create("/1", acl=[])
        acls, stat = client.get_acls("/1")
        assert acls == OPEN_ACL_UNSAFE

    def test_version_no_connection(self, zkclient):
        zkclient.stop()
        with pytest.raises(ConnectionLoss):
            zkclient.server_version()

    def test_create_ephemeral(self, zkclient):
        client = zkclient
        client.create("/1", b"ephemeral", ephemeral=True)
        data, stat = client.get("/1")
        assert data == b"ephemeral"
        assert stat.ephemeralOwner == client.client_id[0]

    def test_create_no_ephemeral(self, zkclient):
        client = zkclient
        client.create("/1", b"val1")
        data, stat = client.get("/1")
        assert not stat.ephemeralOwner

    def test_create_ephemeral_no_children(self, zkclient):
        from kazoo.exceptions import NoChildrenForEphemeralsError

        client = zkclient
        client.create("/1", b"ephemeral", ephemeral=True)
        with pytest.raises(NoChildrenForEphemeralsError):
            client.create("/1/2", b"val1")
        with pytest.raises(NoChildrenForEphemeralsError):
            client.create("/1/2", b"val1", ephemeral=True)

    def test_create_sequence(self, zkclient):
        client = zkclient
        client.create("/folder")
        path = client.create("/folder/a", b"sequence", sequence=True)
        assert path == "/folder/a0000000000"
        path2 = client.create("/folder/a", b"sequence", sequence=True)
        assert path2 == "/folder/a0000000001"
        path3 = client.create("/folder/", b"sequence", sequence=True)
        assert path3 == "/folder/0000000002"

    def test_create_ephemeral_sequence(self, zkclient):
        basepath = "/" + uuid.uuid4().hex
        realpath = zkclient.create(
            basepath, b"sandwich", sequence=True, ephemeral=True
        )
        assert basepath != realpath and realpath.startswith(basepath)
        data, stat = zkclient.get(realpath)
        assert data == b"sandwich"

    def test_create_makepath(self, zkclient):
        zkclient.create("/1/2", b"val1", makepath=True)
        data, stat = zkclient.get("/1/2")
        assert data == b"val1"

        zkclient.create("/1/2/3/4/5", b"val2", makepath=True)
        data, stat = zkclient.get("/1/2/3/4/5")
        assert data == b"val2"

        with pytest.raises(NodeExistsError):
            zkclient.create("/1/2/3/4/5", b"val2", makepath=True)

    def test_create_makepath_incompatible_acls(self, zkclient, zkensemble):
        from kazoo.security import make_digest_acl

        # Authenticate with the plaintext credential: the server hashes
        # whatever it receives, so a pre-hashed ``make_digest_acl_credential``
        # would be double-hashed and match neither CREATOR_ALL_ACL nor an
        # explicit digest ACL.
        alt_client = zkensemble.get_client(
            max_retries=5,
            auth_data=[("digest", "username:password")],
            handler=self._makeOne(),
        )
        alt_client.chroot = zkclient.chroot
        alt_client.start()
        # Use an explicit digest ACL rather than CREATOR_ALL_ACL: the "auth"
        # scheme resolves to every identity of the creating session, and on
        # the shared-identity axes (tls/sasl_digest/sasl_gssapi) every client
        # shares one identity (the single client cert, the single JAAS user,
        # the single GSSAPI principal), so the isolation assertion would not
        # hold there. A digest ACL keyed to alt_client's own credential keeps
        # the test meaningful on all axes.
        acl = [make_digest_acl("username", "password", all=True)]
        alt_client.create("/1/2", b"val2", makepath=True, acl=acl)

        try:
            with pytest.raises(NoAuthError):
                zkclient.create("/1/2/3/4/5", b"val2", makepath=True)

        finally:
            alt_client.delete("/", recursive=True)
            alt_client.stop()

    def test_create_no_makepath(self, zkclient):
        with pytest.raises(NoNodeError):
            zkclient.create("/1/2", b"val1")
        with pytest.raises(NoNodeError):
            zkclient.create("/1/2", b"val1", makepath=False)

        zkclient.create("/1/2", b"val1", makepath=True)
        with pytest.raises(NoNodeError):
            zkclient.create("/1/2/3/4", b"val1", makepath=False)

    def test_create_exists(self, zkclient):
        from kazoo.exceptions import NodeExistsError

        client = zkclient
        path = client.create("/1")
        with pytest.raises(NodeExistsError):
            client.create(path)

    @pytest.mark.zk_version(">=3.5")
    def test_create_stat(self, zkclient):
        client = zkclient
        _path, stat1 = client.create("/1", b"bytes", include_data=True)
        data, stat2 = client.get("/1")
        assert data == b"bytes"
        assert stat1 == stat2

    def test_create_get_set(self, zkclient):
        client = zkclient
        nodepath = "/test"

        client.create(nodepath, b"sandwich", ephemeral=True)

        data, stat = client.get(nodepath)
        assert data == b"sandwich"

        newstat = client.set(nodepath, b"hats", stat.version)
        assert newstat
        assert newstat.version > stat.version

        # Some other checks of the ZnodeStat object we got
        assert newstat.acl_version == stat.acl_version
        assert newstat.created == stat.ctime / 1000.0
        assert newstat.last_modified == newstat.mtime / 1000.0
        assert newstat.owner_session_id == stat.ephemeralOwner
        assert newstat.creation_transaction_id == stat.czxid
        assert newstat.last_modified_transaction_id == newstat.mzxid
        assert newstat.data_length == newstat.dataLength
        assert newstat.children_count == stat.numChildren
        assert newstat.children_version == stat.cversion

    def test_get_invalid_arguments(self, zkclient):
        client = zkclient
        with pytest.raises(TypeError):
            client.get(("a", "b"))
        with pytest.raises(TypeError):
            client.get("a", watch=True)

    def test_bad_argument(self, zkclient):
        client = zkclient
        client.ensure_path("/1")
        with pytest.raises(TypeError):
            zkclient.set("/1", 1)

    def test_ensure_path(self, zkclient):
        client = zkclient
        client.ensure_path("/1/2")
        assert client.exists("/1/2")

        client.ensure_path("/1/2/3/4")
        assert client.exists("/1/2/3/4")

    def test_sync(self, zkclient):
        client = zkclient
        assert client.sync("/") == "/"
        # Albeit surprising, you can sync anything, even what does not exist.
        assert client.sync("/not_there") == "/not_there"

    def test_exists(self, zkclient):
        client = zkclient
        nodepath = "/test"

        exists = client.exists(nodepath)
        assert exists is None

        client.create(nodepath, b"sandwich", ephemeral=True)
        exists = client.exists(nodepath)
        assert exists
        assert isinstance(exists.version, int)

        multi_node_nonexistent = "/" + uuid.uuid4().hex + "/hats"
        exists = zkclient.exists(multi_node_nonexistent)
        assert exists is None

    def test_exists_invalid_arguments(self, zkclient):
        client = zkclient
        with pytest.raises(TypeError):
            client.exists(("a", "b"))
        with pytest.raises(TypeError):
            client.exists("a", watch=True)

    def test_exists_watch(self, zkclient):
        nodepath = "/test"
        event = zkclient.handler.event_object()

        def w(watch_event):
            assert watch_event.path == nodepath
            event.set()

        exists = zkclient.exists(nodepath, watch=w)
        assert exists is None

        zkclient.create(nodepath, ephemeral=True)

        event.wait(1)
        assert event.is_set() is True

    def test_exists_watcher_exception(self, zkclient):
        nodepath = "/test"
        event = zkclient.handler.event_object()

        # if the watcher throws an exception, all we can really do is log it
        def w(watch_event):
            assert watch_event.path == nodepath
            event.set()

            raise Exception("test exception in callback")

        exists = zkclient.exists(nodepath, watch=w)
        assert exists is None

        zkclient.create(nodepath, ephemeral=True)

        event.wait(1)
        assert event.is_set() is True

    def test_create_delete(self, zkclient):
        nodepath = "/" + uuid.uuid4().hex

        zkclient.create(nodepath, b"zzz")

        zkclient.delete(nodepath)

        exists = zkclient.exists(nodepath)
        assert exists is None

    def test_get_acls(self, zkclient):
        user = "user"
        passw = "pass"
        acl = security.make_digest_acl(user, passw, all=True)
        client = zkclient
        try:
            client.create("/a", acl=[acl])
            client.add_auth("digest", "{}:{}".format(user, passw))
            assert acl in client.get_acls("/a")[0]
        finally:
            client.delete("/a")

    def test_get_acls_invalid_arguments(self, zkclient):
        client = zkclient
        with pytest.raises(TypeError):
            client.get_acls(("a", "b"))

    def test_set_acls(self, zkclient):
        user = "user"
        passw = "pass"
        acl = security.make_digest_acl(user, passw, all=True)
        client = zkclient
        client.create("/a")
        try:
            client.set_acls("/a", [acl])
            client.add_auth("digest", "{}:{}".format(user, passw))
            assert acl in client.get_acls("/a")[0]
        finally:
            client.delete("/a")

    def test_set_acls_empty(self, zkclient):
        client = zkclient
        client.create("/a")
        with pytest.raises(InvalidACLError):
            client.set_acls("/a", [])

    def test_set_acls_no_node(self, zkclient):
        from kazoo.security import OPEN_ACL_UNSAFE

        client = zkclient
        with pytest.raises(NoNodeError):
            client.set_acls("/a", OPEN_ACL_UNSAFE)

    def test_set_acls_invalid_arguments(self, zkclient):
        from kazoo.security import OPEN_ACL_UNSAFE

        single_acl = OPEN_ACL_UNSAFE[0]
        client = zkclient
        with pytest.raises(TypeError):
            client.set_acls(("a", "b"), ())
        with pytest.raises(TypeError):
            client.set_acls("a", single_acl)
        with pytest.raises(TypeError):
            client.set_acls("a", "all")
        with pytest.raises(TypeError):
            client.set_acls("a", [single_acl], "V1")

    def test_set(self, zkclient):
        client = zkclient
        client.create("a", b"first")
        stat = client.set("a", b"second")
        data, stat2 = client.get("a")
        assert data == b"second"
        assert stat == stat2

    def test_set_null_data(self, zkclient):
        client = zkclient
        client.create("/nulldata", b"not none")
        client.set("/nulldata", None)
        value, _ = client.get("/nulldata")
        assert value is None

    def test_set_empty_string(self, zkclient):
        client = zkclient
        client.create("/empty", b"not empty")
        client.set("/empty", b"")
        value, _ = client.get("/empty")
        assert value == b""

    def test_set_invalid_arguments(self, zkclient):
        client = zkclient
        client.create("a", b"first")
        with pytest.raises(TypeError):
            client.set(("a", "b"), b"value")
        with pytest.raises(TypeError):
            client.set("a", ["v", "w"])
        with pytest.raises(TypeError):
            client.set("a", b"value", "V1")

    def test_delete(self, zkclient):
        client = zkclient
        client.ensure_path("/a/b")
        assert "b" in client.get_children("a")
        client.delete("/a/b")
        assert "b" not in client.get_children("a")

    def test_delete_recursive(self, zkclient):
        client = zkclient
        client.ensure_path("/a/b/c")
        client.ensure_path("/a/b/d")
        client.delete("/a/b", recursive=True)
        client.delete("/a/b/c", recursive=True)
        assert "b" not in client.get_children("a")

    def test_delete_invalid_arguments(self, zkclient):
        client = zkclient
        client.ensure_path("/a/b")
        with pytest.raises(TypeError):
            client.delete("/a/b", recursive="all")
        with pytest.raises(TypeError):
            client.delete(("a", "b"))
        with pytest.raises(TypeError):
            client.delete("/a/b", version="V1")

    def test_get_children(self, zkclient):
        client = zkclient
        client.ensure_path("/a/b/c")
        client.ensure_path("/a/b/d")
        assert client.get_children("/a") == ["b"]
        assert set(client.get_children("/a/b")) == set(["c", "d"])
        assert client.get_children("/a/b/c") == []

    def test_get_children2(self, zkclient):
        client = zkclient
        client.ensure_path("/a/b")
        children, stat = client.get_children("/a", include_data=True)
        value, stat2 = client.get("/a")
        assert children == ["b"]
        assert stat2.version == stat.version

    def test_get_children2_many_nodes(self, zkclient):
        client = zkclient
        client.ensure_path("/a/b")
        client.ensure_path("/a/c")
        client.ensure_path("/a/d")
        children, stat = client.get_children("/a", include_data=True)
        value, stat2 = client.get("/a")
        assert set(children) == set(["b", "c", "d"])
        assert stat2.version == stat.version

    def test_get_children_no_node(self, zkclient):
        client = zkclient
        with pytest.raises(NoNodeError):
            client.get_children("/none")
        with pytest.raises(NoNodeError):
            client.get_children("/none", include_data=True)

    def test_get_children_invalid_path(self, zkclient):
        client = zkclient
        with pytest.raises(ValueError):
            client.get_children("../a")

    def test_get_children_invalid_arguments(self, zkclient):
        client = zkclient
        with pytest.raises(TypeError):
            client.get_children(("a", "b"))
        with pytest.raises(TypeError):
            client.get_children("a", watch=True)
        with pytest.raises(TypeError):
            client.get_children("a", include_data="yes")

    def test_invalid_auth(self, zkclient):
        from kazoo.exceptions import AuthFailedError
        from kazoo.protocol.states import KeeperState

        client = zkclient
        client.stop()
        client._state = KeeperState.AUTH_FAILED

        with pytest.raises(AuthFailedError):
            client.get("/")

    def test_client_state(self, zkclient):
        from kazoo.protocol.states import KeeperState

        assert zkclient.client_state == KeeperState.CONNECTED

    def test_update_host_list(self, zkensemble):
        from kazoo.protocol.states import KeeperState

        hosts = f"{zkensemble.zk_ip}:{zkensemble.zk1_port}"
        # create a client with only one server in its list
        handler = self._makeOne()
        client = zkensemble.get_client(
            hosts=hosts,
            handler=handler,
            timeout=30.0,
            connection_retry={
                "max_tries": -1,
                "delay": 0.1,
                "backoff": 1,
                "max_jitter": 0.0,
                "sleep_func": handler.sleep_func,
            },
        )
        client.start(timeout=30.0)

        try:
            # try to change the chroot, not currently allowed
            with pytest.raises(ConfigurationError):
                client.set_hosts(hosts + "/new_chroot")

            # grow the cluster to 3
            hosts = zkensemble.get_hosts()
            client.set_hosts(hosts)

            ev_connected = client.handler.event_object()

            def listener(state):
                if state == KazooState.CONNECTED:
                    ev_connected.set()

            client.add_listener(listener)

            # shut down the first host
            zkensemble.stop("zoo1", handler=handler)
            ev_connected.wait(60)
            assert ev_connected.is_set(), (
                f"Failover timed out after 60s: ev_connected not set. "
                f"client.state={client.state}, "
                f"client.client_state={client.client_state}, "
                f"client.connected={client.connected}, "
                f"hosts={client.hosts}"
            )
            assert client.client_state == KeeperState.CONNECTED
        finally:
            client.stop()
            client.close()
            zkensemble.start("zoo1", handler=handler)

    # utility for test_request_queuing*
    def _make_request_queuing_client(
        self, zkclient, zkensemble
    ) -> tuple[KazooClient, str]:
        server = "zoo1"
        handler = self._makeOne()
        # create a client with only one server in its list, and
        # infinite retries
        client = zkensemble.get_client(
            # connect to the first server in the ensemble
            hosts=f"{zkensemble.zk_ip}:{zkensemble.zk1_port}",
            handler=handler,
            timeout=30.0,
            connection_retry={
                "max_tries": -1,
                "delay": 0.1,
                "backoff": 1,
                "max_jitter": 0.0,
                "sleep_func": handler.sleep_func,
            },
        )
        client.chroot = zkclient.chroot

        return client, server

    # utility for test_request_queuing*
    def _request_queuing_common(
        self,
        zkensemble,
        client: KazooClient,
        server: str,
        path: str,
        expire_session: bool,
    ):
        ev_suspended = client.handler.event_object()
        ev_connected = client.handler.event_object()

        def listener(state):
            if state == KazooState.SUSPENDED:
                ev_suspended.set()
            elif state == KazooState.CONNECTED and ev_suspended.is_set():
                ev_connected.set()

        client.add_listener(listener)

        # wait for the client to connect
        client.start(timeout=30.0)

        try:
            # force the client to suspend
            zkensemble.stop(server)

            ev_suspended.wait(30)
            assert ev_suspended.is_set()

            # submit a request, expecting it to be queued
            result = client.create_async(path)
            assert len(client._queue) != 0
            assert result.ready() is False
            assert client.state == KazooState.SUSPENDED

            # optionally cause a SessionExpiredError to occur by
            # mangling the first byte of the session password.
            if expire_session:
                b0 = b"\x00"
                if client._session_passwd[0] == 0:
                    b0 = b"\xff"
                client._session_passwd = b0 + client._session_passwd[1:]
        finally:
            zkensemble.start(server)

        # wait for the client to reconnect (either with a recovered
        # session, or with a new one if expire_session was set).
        ev_connected.wait(60)
        assert ev_connected.is_set()

        return result

    def test_request_queuing_session_recovered(self, zkclient, zkensemble):
        path = "/" + uuid.uuid4().hex
        client, server = self._make_request_queuing_client(
            zkclient=zkclient, zkensemble=zkensemble
        )

        try:
            result = self._request_queuing_common(
                zkensemble=zkensemble,
                client=client,
                server=server,
                path=path,
                expire_session=False,
            )

            assert result.get(timeout=30) == path
            assert len(client._queue) == 0
            assert client.exists(path) is not None
        finally:
            client.stop()
            client.close()

    def test_request_queuing_session_expired(self, zkclient, zkensemble):
        path = "/" + uuid.uuid4().hex
        client, server = self._make_request_queuing_client(
            zkclient=zkclient, zkensemble=zkensemble
        )

        try:
            result = self._request_queuing_common(
                zkensemble=zkensemble,
                client=client,
                server=server,
                path=path,
                expire_session=True,
            )

            with pytest.raises(SessionExpiredError):
                result.get(timeout=30)
            assert len(client._queue) == 0
        finally:
            client.stop()
            client.close()


@pytest.mark.zk_auth("tls")
@pytest.mark.zk_version(">=3.5")
class TestSSLClient:
    """TLS-transport client tests, run under the tls auth axis.

    The tls axis serves the ensemble on the secureClientPort with mutual TLS
    (ssl.clientAuth=need) and ``zkensemble.get_client()`` implies the
    client cert/key/CA options, so a connected ``zkclient`` has already
    completed a real TLS handshake with a client certificate.
    """

    def test_create(self, zkclient):
        client = zkclient

        # The tls axis must have configured TLS and actually negotiated it:
        # the connection options are implied and the transport socket is a
        # wrapped SSL socket.
        assert client.use_ssl is True
        assert client.certfile and client.keyfile and client.ca
        assert isinstance(client._connection._socket, ssl.SSLSocket)

        path = client.create("/1")
        assert path == "/1"
        assert client.exists("/1")

        data, stat = client.get("/1")
        assert data == b""

        client.delete("/1")
        assert client.exists("/1") is None


@pytest.mark.zk_version(">=3.4")
class TestClientTransactions:
    def test_basic_create(self, zkclient):
        t = zkclient.transaction()
        t.create("/freddy")
        t.create("/fred", ephemeral=True)
        t.create("/smith", sequence=True)
        results = t.commit()
        assert len(results) == 3
        assert results[0] == "/freddy"
        assert results[2].startswith("/smith0") is True

    def test_bad_creates(self, zkclient):
        args_list = [
            (True,),
            ("/smith", 0),
            ("/smith", b"", "bleh"),
            ("/smith", b"", None, "fred"),
            ("/smith", b"", None, True, "fred"),
        ]

        for args in args_list:
            with pytest.raises(TypeError):
                t = zkclient.transaction()
                t.create(*args)

    def test_default_acl(self, zkclient):
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = security.make_digest_acl(username, password, all=True)

        zkclient.add_auth("digest", digest_auth)
        zkclient.default_acl = (acl,)

        t = zkclient.transaction()
        t.create("/freddy")
        results = t.commit()
        assert results[0] == "/freddy"

    def test_basic_delete(self, zkclient):
        zkclient.create("/fred")
        t = zkclient.transaction()
        t.delete("/fred")
        results = t.commit()
        assert results[0] is True

    def test_bad_deletes(self, zkclient):
        args_list = [
            (True,),
            ("/smith", "woops"),
        ]

        for args in args_list:
            with pytest.raises(TypeError):
                t = zkclient.transaction()
                t.delete(*args)

    def test_set(self, zkclient):
        zkclient.create("/fred", b"01")
        t = zkclient.transaction()
        t.set_data("/fred", b"oops")
        t.commit()
        res = zkclient.get("/fred")
        assert res[0] == b"oops"

    def test_bad_sets(self, zkclient):
        args_list = [(42, 52), ("/smith", False), ("/smith", b"", "oops")]

        for args in args_list:
            with pytest.raises(TypeError):
                t = zkclient.transaction()
                t.set_data(*args)

    def test_check(self, zkclient):
        zkclient.create("/fred")
        version = zkclient.get("/fred")[1].version
        t = zkclient.transaction()
        t.check("/fred", version)
        t.create("/blah")
        results = t.commit()
        assert results[0] is True
        assert results[1] == "/blah"

    def test_bad_checks(self, zkclient):
        args_list = [(42, 52), ("/smith", "oops")]

        for args in args_list:
            with pytest.raises(TypeError):
                t = zkclient.transaction()
                t.check(*args)

    def test_bad_transaction(self, zkclient):
        from kazoo.exceptions import RolledBackError, NoNodeError

        t = zkclient.transaction()
        t.create("/fred")
        t.delete("/smith")
        results = t.commit()
        assert results[0].__class__ == RolledBackError
        assert results[1].__class__ == NoNodeError

    def test_bad_commit(self, zkclient):
        t = zkclient.transaction()
        t.committed = True

        with pytest.raises(ValueError):
            t.commit()

    def test_bad_context(self, zkclient):
        with pytest.raises(TypeError):
            with zkclient.transaction() as t:
                t.check(4232)

    def test_context(self, zkclient):
        with zkclient.transaction() as t:
            t.create("/smith", b"32")
        assert zkclient.get("/smith")[0] == b"32"


class TestCallbacks:
    def test_async_result_callbacks_are_always_called(self, zkclient):
        # create a callback object
        callback_mock = mock.Mock()

        # simulate waiting for a response
        async_result = zkclient.handler.async_result()
        async_result.rawlink(callback_mock)

        # begin the procedure to stop the client
        zkclient.stop()

        # the response has just been received;
        # this should be on another thread,
        # simultaneously with the stop procedure
        async_result.set_exception(
            Exception("Anything that throws an exception")
        )

        # with the fix the callback should be called
        assert callback_mock.call_count > 0


class TestNonChrootClient:
    def test_create(self, zkensemble):
        client = zkensemble.get_client()
        assert client.chroot == ""
        client.start()
        node = uuid.uuid4().hex
        path = client.create(node, ephemeral=True)
        client.delete(path)
        client.stop()

    def test_unchroot(self, zkensemble):
        client = zkensemble.get_client()
        client.chroot = "/a"
        # Unchroot'ing the chroot path should return "/"
        assert client.unchroot("/a") == "/"
        assert client.unchroot("/a/b") == "/b"
        assert client.unchroot("/b/c") == "/b/c"


@pytest.mark.zk_features(require=["reconfig"])
@pytest.mark.zk_version(">=3.5")
class TestReconfig:
    def test_no_super_auth(self, zkclient):
        with pytest.raises(NoAuthError):
            zkclient.reconfig(
                joining="server.999=0.0.0.0:1234:2345:observer;3456",
                leaving=None,
                new_members=None,
            )

    def test_add_remove_observer(self, zksuperadmin_client):
        joining = "server.100=0.0.0.0:2181:2182:observer;0.0.0.0:2183"
        data, _ = zksuperadmin_client.reconfig(
            joining=joining,
            leaving=None,
            new_members=None,
        )
        assert joining.encode("utf8") in data

        data, _ = zksuperadmin_client.reconfig(
            joining=None,
            leaving="100",
            new_members=None,
        )
        assert joining.encode("utf8") not in data

        # try to add it again, but a config number in the future
        curver = int(data.decode().split("\n")[-1].split("=")[1], base=16)
        with pytest.raises(BadVersionError):
            zksuperadmin_client.reconfig(
                joining=joining,
                leaving=None,
                new_members=None,
                from_config=curver + 1,
            )

    def test_bad_input(self, zksuperadmin_client):
        with pytest.raises(BadArgumentsError):
            zksuperadmin_client.reconfig(
                joining="some thing",
                leaving=None,
                new_members=None,
            )
