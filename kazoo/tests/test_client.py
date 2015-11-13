import socket
import sys
import threading
import time
import uuid
import unittest

import mock
from mock import patch
from nose import SkipTest
from nose.tools import eq_
from nose.tools import raises

from kazoo.testing import KazooTestCase
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
from kazoo.tests.util import TRAVIS_ZK_VERSION


if sys.version_info > (3, ):  # pragma: nocover
    def u(s):
        return s
else:  # pragma: nocover
    def u(s):
        return unicode(s, "unicode_escape")


class TestClientTransitions(KazooTestCase):
    @staticmethod
    def make_event():
        return threading.Event()

    def test_connection_and_disconnection(self):
        states = []
        rc = threading.Event()

        @self.client.add_listener
        def listener(state):
            states.append(state)
            if state == KazooState.CONNECTED:
                rc.set()

        self.client.stop()
        eq_(states, [KazooState.LOST])
        states.pop()

        self.client.start()
        rc.wait(2)
        eq_(states, [KazooState.CONNECTED])
        rc.clear()
        states.pop()
        self.expire_session(self.make_event)
        rc.wait(2)

        req_states = [KazooState.LOST, KazooState.CONNECTED]
        eq_(states, req_states)


class TestClientConstructor(unittest.TestCase):

    def _makeOne(self, *args, **kw):
        from kazoo.client import KazooClient
        return KazooClient(*args, **kw)

    def test_invalid_handler(self):
        from kazoo.handlers.threading import SequentialThreadingHandler
        self.assertRaises(
            ConfigurationError,
            self._makeOne, handler=SequentialThreadingHandler)

    def test_chroot(self):
        self.assertEqual(self._makeOne(hosts='127.0.0.1:2181/').chroot, '')
        self.assertEqual(self._makeOne(hosts='127.0.0.1:2181/a').chroot, '/a')
        self.assertEqual(self._makeOne(hosts='127.0.0.1/a').chroot, '/a')
        self.assertEqual(self._makeOne(hosts='127.0.0.1/a/b').chroot, '/a/b')
        self.assertEqual(self._makeOne(
            hosts='127.0.0.1:2181,127.0.0.1:2182/a/b').chroot, '/a/b')

    def test_connection_timeout(self):
        from kazoo.handlers.threading import KazooTimeoutError
        client = self._makeOne(hosts='127.0.0.1:9')
        self.assertTrue(client.handler.timeout_exception is KazooTimeoutError)
        self.assertRaises(KazooTimeoutError, client.start, 0.1)

    def test_ordered_host_selection(self):
        client = self._makeOne(hosts='127.0.0.1:9,127.0.0.2:9/a',
                               randomize_hosts=False)
        hosts = [h for h in client.hosts]
        eq_(hosts, [('127.0.0.1', 9), ('127.0.0.2', 9)])

    def test_invalid_hostname(self):
        self.assertRaises(socket.gaierror, self._makeOne, hosts='nosuchhost/a')

    def test_another_invalid_hostname(self):
        self.assertRaises(
            ValueError,
            self._makeOne, hosts='/nosuchhost/a')

    def test_retry_options_dict(self):
        from kazoo.retry import KazooRetry
        client = self._makeOne(command_retry=dict(max_tries=99),
                               connection_retry=dict(delay=99))
        self.assertTrue(type(client._conn_retry) is KazooRetry)
        self.assertTrue(type(client._retry) is KazooRetry)
        eq_(client._retry.max_tries, 99)
        eq_(client._conn_retry.delay, 99)


class TestAuthentication(KazooTestCase):

    def _makeAuth(self, *args, **kwargs):
        from kazoo.security import make_digest_acl
        return make_digest_acl(*args, **kwargs)

    def test_auth(self):
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = self._makeAuth(username, password, all=True)

        client = self._get_client()
        client.start()
        client.add_auth("digest", digest_auth)
        client.default_acl = (acl,)

        try:
            client.create("/1")
            client.create("/1/2")
            client.ensure_path("/1/2/3")

            eve = self._get_client()
            eve.start()

            self.assertRaises(NoAuthError, eve.get, "/1/2")

            # try again with the wrong auth token
            eve.add_auth("digest", "badbad:bad")

            self.assertRaises(NoAuthError, eve.get, "/1/2")
        finally:
            # Ensure we remove the ACL protected nodes
            client.delete("/1", recursive=True)
            eve.stop()
            eve.close()

    def test_connect_auth(self):
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = self._makeAuth(username, password, all=True)

        client = self._get_client(auth_data=[('digest', digest_auth)])
        client.start()
        try:
            client.create('/1', acl=(acl,))
            # give ZK a chance to copy data to other node
            time.sleep(0.1)
            self.assertRaises(NoAuthError, self.client.get, "/1")
        finally:
            client.delete('/1')
            client.stop()
            client.close()

    def test_unicode_auth(self):
        username = u("xe4/\hm")
        password = u("/\xe4hm")
        digest_auth = "%s:%s" % (username, password)
        acl = self._makeAuth(username, password, all=True)

        client = self._get_client()
        client.start()
        client.add_auth("digest", digest_auth)
        client.default_acl = (acl,)

        try:
            client.create("/1")
            client.ensure_path("/1/2/3")

            eve = self._get_client()
            eve.start()

            self.assertRaises(NoAuthError, eve.get, "/1/2")

            # try again with the wrong auth token
            eve.add_auth("digest", "badbad:bad")

            self.assertRaises(NoAuthError, eve.get, "/1/2")
        finally:
            # Ensure we remove the ACL protected nodes
            client.delete("/1", recursive=True)
            eve.stop()
            eve.close()

    def test_invalid_auth(self):
        client = self._get_client()
        client.start()
        self.assertRaises(TypeError, client.add_auth,
                          'digest', ('user', 'pass'))
        self.assertRaises(TypeError, client.add_auth,
                          None, ('user', 'pass'))

    def test_async_auth(self):
        client = self._get_client()
        client.start()
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex
        digest_auth = "%s:%s" % (username, password)
        result = client.add_auth_async("digest", digest_auth)
        self.assertTrue(result.get())

    def test_async_auth_failure(self):
        client = self._get_client()
        client.start()
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex
        digest_auth = "%s:%s" % (username, password)

        self.assertRaises(AuthFailedError, client.add_auth,
                          "unknown-scheme", digest_auth)

    def test_add_auth_on_reconnect(self):
        client = self._get_client()
        client.start()
        client.add_auth("digest", "jsmith:jsmith")
        client._connection._socket.shutdown(socket.SHUT_RDWR)
        while not client.connected:
            time.sleep(0.1)
        self.assertTrue(("digest", "jsmith:jsmith") in client.auth_data)


class TestConnection(KazooTestCase):
    @staticmethod
    def make_event():
        return threading.Event()

    @staticmethod
    def make_condition():
        return threading.Condition()

    def test_chroot_warning(self):
        k = self._get_nonchroot_client()
        k.chroot = 'abba'
        try:
            with patch('warnings.warn') as mock_func:
                k.start()
                assert mock_func.called
        finally:
            k.stop()

    def test_session_expire(self):
        from kazoo.protocol.states import KazooState

        cv = self.make_event()

        def watch_events(event):
            if event == KazooState.LOST:
                cv.set()

        self.client.add_listener(watch_events)
        self.expire_session(self.make_event)
        cv.wait(3)
        assert cv.is_set()

    def test_bad_session_expire(self):
        from kazoo.protocol.states import KazooState

        cv = self.make_event()
        ab = self.make_event()

        def watch_events(event):
            if event == KazooState.LOST:
                ab.set()
                raise Exception("oops")
                cv.set()

        self.client.add_listener(watch_events)
        self.expire_session(self.make_event)
        ab.wait(0.5)
        assert ab.is_set()
        cv.wait(0.5)
        assert not cv.is_set()

    def test_state_listener(self):
        from kazoo.protocol.states import KazooState
        states = []
        condition = self.make_condition()

        def listener(state):
            with condition:
                states.append(state)
                condition.notify_all()

        self.client.stop()
        eq_(self.client.state, KazooState.LOST)
        self.client.add_listener(listener)
        self.client.start(5)

        with condition:
            if not states:
                condition.wait(5)

        eq_(len(states), 1)
        eq_(states[0], KazooState.CONNECTED)

    def test_invalid_listener(self):
        self.assertRaises(ConfigurationError, self.client.add_listener, 15)

    def test_listener_only_called_on_real_state_change(self):
        from kazoo.protocol.states import KazooState
        self.assertTrue(self.client.state, KazooState.CONNECTED)
        called = [False]
        condition = self.make_event()

        def listener(state):
            called[0] = True
            condition.set()

        self.client.add_listener(listener)
        self.client._make_state_change(KazooState.CONNECTED)
        condition.wait(3)
        self.assertFalse(called[0])

    def test_no_connection(self):
        client = self.client
        client.stop()
        self.assertFalse(client.connected)
        self.assertTrue(client.client_id is None)
        self.assertRaises(ConnectionClosedError, client.exists, '/')

    def test_close_connecting_connection(self):
        client = self.client
        client.stop()
        ev = self.make_event()

        def close_on_connecting(state):
            if state in (KazooState.CONNECTED, KazooState.LOST):
                ev.set()

        client.add_listener(close_on_connecting)
        client.start()

        # Wait until we connect
        ev.wait(5)
        ev.clear()
        self.client._call(_CONNECTION_DROP, client.handler.async_result())

        client.stop()

        # ...and then wait until the connection is lost
        ev.wait(5)

        self.assertRaises(ConnectionClosedError,
                          self.client.create, '/foobar')

    def test_double_start(self):
        self.assertTrue(self.client.connected)
        self.client.start()
        self.assertTrue(self.client.connected)

    def test_double_stop(self):
        self.client.stop()
        self.assertFalse(self.client.connected)
        self.client.stop()
        self.assertFalse(self.client.connected)

    def test_restart(self):
        self.assertTrue(self.client.connected)
        self.client.restart()
        self.assertTrue(self.client.connected)

    def test_closed(self):
        client = self.client
        client.stop()

        write_sock = client._connection._write_sock

        # close the connection to free the socket
        client.close()
        eq_(client._connection._write_sock, None)

        # sneak in and patch client to simulate race between a thread
        # calling stop(); close() and one running a command
        oldstate = client._state
        client._state = KeeperState.CONNECTED
        client._connection._write_sock = write_sock
        try:
            # simulate call made after write socket is closed
            self.assertRaises(ConnectionClosedError, client.exists, '/')

            # simulate call made after write socket is set to None
            client._connection._write_sock = None
            self.assertRaises(ConnectionClosedError, client.exists, '/')

        finally:
            # reset for teardown
            client._state = oldstate
            client._connection._write_sock = None


class TestClient(KazooTestCase):
    def _makeOne(self, *args):
        from kazoo.handlers.threading import SequentialThreadingHandler
        return SequentialThreadingHandler(*args)

    def _getKazooState(self):
        from kazoo.protocol.states import KazooState
        return KazooState

    def test_server_version_retries_fail(self):
        client = self.client
        side_effects = [
            '',
            'zookeeper.version=',
            'zookeeper.version=1.',
            'zookeeper.ver',
        ]
        client.command = mock.MagicMock()
        client.command.side_effect = side_effects
        self.assertRaises(KazooException,
                          client.server_version,
                          retries=len(side_effects) - 1)

    def test_server_version_retries_eventually_ok(self):
        client = self.client
        actual_version = 'zookeeper.version=1.2'
        side_effects = []
        for i in range(0, len(actual_version) + 1):
            side_effects.append(actual_version[0:i])
        client.command = mock.MagicMock()
        client.command.side_effect = side_effects
        self.assertEqual((1, 2),
                         client.server_version(retries=len(side_effects) - 1))

    def test_client_id(self):
        client_id = self.client.client_id
        self.assertEqual(type(client_id), tuple)
        # make sure password is of correct length
        self.assertEqual(len(client_id[1]), 16)

    def test_connected(self):
        client = self.client
        self.assertTrue(client.connected)

    def test_create(self):
        client = self.client
        path = client.create("/1")
        eq_(path, "/1")
        self.assertTrue(client.exists("/1"))

    def test_create_on_broken_connection(self):
        client = self.client
        client.start()

        client._state = KeeperState.EXPIRED_SESSION
        self.assertRaises(SessionExpiredError, client.create,
                          '/closedpath', b'bar')

        client._state = KeeperState.AUTH_FAILED
        self.assertRaises(AuthFailedError, client.create,
                          '/closedpath', b'bar')

        client._state = KeeperState.CONNECTING
        self.assertRaises(SessionExpiredError, client.create,
                          '/closedpath', b'bar')
        client.stop()
        client.close()

        self.assertRaises(ConnectionClosedError, client.create,
                          '/closedpath', b'bar')

    def test_create_null_data(self):
        client = self.client
        client.create("/nulldata", None)
        value, _ = client.get("/nulldata")
        self.assertEqual(value, None)

    def test_create_empty_string(self):
        client = self.client
        client.create("/empty", b"")
        value, _ = client.get("/empty")
        eq_(value, b"")

    def test_create_unicode_path(self):
        client = self.client
        path = client.create(u("/ascii"))
        eq_(path, u("/ascii"))
        path = client.create(u("/\xe4hm"))
        eq_(path, u("/\xe4hm"))

    def test_create_async_returns_unchrooted_path(self):
        client = self.client
        path = client.create_async('/1').get()
        eq_(path, "/1")

    def test_create_invalid_path(self):
        client = self.client
        self.assertRaises(TypeError, client.create, ('a', ))
        self.assertRaises(ValueError, client.create, ".")
        self.assertRaises(ValueError, client.create, "/a/../b")
        self.assertRaises(BadArgumentsError, client.create, "/b\x00")
        self.assertRaises(BadArgumentsError, client.create, "/b\x1e")

    def test_create_invalid_arguments(self):
        from kazoo.security import OPEN_ACL_UNSAFE
        single_acl = OPEN_ACL_UNSAFE[0]
        client = self.client
        self.assertRaises(TypeError, client.create, 'a', acl='all')
        self.assertRaises(TypeError, client.create, 'a', acl=single_acl)
        self.assertRaises(TypeError, client.create, 'a', value=['a'])
        self.assertRaises(TypeError, client.create, 'a', ephemeral='yes')
        self.assertRaises(TypeError, client.create, 'a', sequence='yes')
        self.assertRaises(TypeError, client.create, 'a', makepath='yes')

    def test_create_value(self):
        client = self.client
        client.create("/1", b"bytes")
        data, stat = client.get("/1")
        eq_(data, b"bytes")

    def test_create_unicode_value(self):
        client = self.client
        self.assertRaises(TypeError, client.create, "/1", u("\xe4hm"))

    def test_create_large_value(self):
        client = self.client
        kb_512 = b"a" * (512 * 1024)
        client.create("/1", kb_512)
        self.assertTrue(client.exists("/1"))
        mb_2 = b"a" * (2 * 1024 * 1024)
        self.assertRaises(ConnectionLoss, client.create, "/2", mb_2)

    def test_create_acl_duplicate(self):
        from kazoo.security import OPEN_ACL_UNSAFE
        single_acl = OPEN_ACL_UNSAFE[0]
        client = self.client
        client.create("/1", acl=[single_acl, single_acl])
        acls, stat = client.get_acls("/1")
        # ZK >3.4 removes duplicate ACL entries
        if TRAVIS_ZK_VERSION:
            version = TRAVIS_ZK_VERSION
        else:
            version = client.server_version()
        self.assertEqual(len(acls), 1 if version > (3, 4) else 2)

    def test_create_acl_empty_list(self):
        from kazoo.security import OPEN_ACL_UNSAFE
        client = self.client
        client.create("/1", acl=[])
        acls, stat = client.get_acls("/1")
        self.assertEqual(acls, OPEN_ACL_UNSAFE)

    def test_version_no_connection(self):
        @raises(ConnectionLoss)
        def testit():
            self.client.server_version()
        self.client.stop()
        testit()

    def test_create_ephemeral(self):
        client = self.client
        client.create("/1", b"ephemeral", ephemeral=True)
        data, stat = client.get("/1")
        eq_(data, b"ephemeral")
        eq_(stat.ephemeralOwner, client.client_id[0])

    def test_create_no_ephemeral(self):
        client = self.client
        client.create("/1", b"val1")
        data, stat = client.get("/1")
        self.assertFalse(stat.ephemeralOwner)

    def test_create_ephemeral_no_children(self):
        from kazoo.exceptions import NoChildrenForEphemeralsError
        client = self.client
        client.create("/1", b"ephemeral", ephemeral=True)
        self.assertRaises(NoChildrenForEphemeralsError,
                          client.create, "/1/2", b"val1")
        self.assertRaises(NoChildrenForEphemeralsError,
                          client.create, "/1/2", b"val1", ephemeral=True)

    def test_create_sequence(self):
        client = self.client
        client.create("/folder")
        path = client.create("/folder/a", b"sequence", sequence=True)
        eq_(path, "/folder/a0000000000")
        path2 = client.create("/folder/a", b"sequence", sequence=True)
        eq_(path2, "/folder/a0000000001")
        path3 = client.create("/folder/", b"sequence", sequence=True)
        eq_(path3, "/folder/0000000002")

    def test_create_ephemeral_sequence(self):
        basepath = "/" + uuid.uuid4().hex
        realpath = self.client.create(basepath, b"sandwich",
                                      sequence=True, ephemeral=True)
        self.assertTrue(basepath != realpath and realpath.startswith(basepath))
        data, stat = self.client.get(realpath)
        eq_(data, b"sandwich")

    def test_create_makepath(self):
        self.client.create("/1/2", b"val1", makepath=True)
        data, stat = self.client.get("/1/2")
        eq_(data, b"val1")

        self.client.create("/1/2/3/4/5", b"val2", makepath=True)
        data, stat = self.client.get("/1/2/3/4/5")
        eq_(data, b"val2")

        self.assertRaises(NodeExistsError, self.client.create,
                          "/1/2/3/4/5", b"val2", makepath=True)

    def test_create_makepath_incompatible_acls(self):
        from kazoo.client import KazooClient
        from kazoo.security import make_digest_acl_credential, CREATOR_ALL_ACL
        credential = make_digest_acl_credential("username", "password")
        alt_client = KazooClient(
            self.cluster[0].address + self.client.chroot,
            max_retries=5, auth_data=[("digest", credential)],
            handler=self._makeOne())
        alt_client.start()
        alt_client.create("/1/2", b"val2", makepath=True, acl=CREATOR_ALL_ACL)

        try:
            self.assertRaises(NoAuthError, self.client.create,
                              "/1/2/3/4/5", b"val2", makepath=True)
        finally:
            alt_client.delete('/', recursive=True)
            alt_client.stop()

    def test_create_no_makepath(self):
        self.assertRaises(NoNodeError, self.client.create,
                          "/1/2", b"val1")
        self.assertRaises(NoNodeError, self.client.create,
                          "/1/2", b"val1", makepath=False)

        self.client.create("/1/2", b"val1", makepath=True)
        self.assertRaises(NoNodeError, self.client.create,
                          "/1/2/3/4", b"val1", makepath=False)

    def test_create_exists(self):
        from kazoo.exceptions import NodeExistsError
        client = self.client
        path = client.create("/1")
        self.assertRaises(NodeExistsError, client.create, path)

    def test_create_get_set(self):
        nodepath = "/" + uuid.uuid4().hex

        self.client.create(nodepath, b"sandwich", ephemeral=True)

        data, stat = self.client.get(nodepath)
        eq_(data, b"sandwich")

        newstat = self.client.set(nodepath, b"hats", stat.version)
        self.assertTrue(newstat)
        assert newstat.version > stat.version

        # Some other checks of the ZnodeStat object we got
        eq_(newstat.acl_version, stat.acl_version)
        eq_(newstat.created, stat.ctime / 1000.0)
        eq_(newstat.last_modified, newstat.mtime / 1000.0)
        eq_(newstat.owner_session_id, stat.ephemeralOwner)
        eq_(newstat.creation_transaction_id, stat.czxid)
        eq_(newstat.last_modified_transaction_id, newstat.mzxid)
        eq_(newstat.data_length, newstat.dataLength)
        eq_(newstat.children_count, stat.numChildren)
        eq_(newstat.children_version, stat.cversion)

    def test_get_invalid_arguments(self):
        client = self.client
        self.assertRaises(TypeError, client.get, ('a', 'b'))
        self.assertRaises(TypeError, client.get, 'a', watch=True)

    def test_bad_argument(self):
        client = self.client
        client.ensure_path("/1")
        self.assertRaises(TypeError, self.client.set, "/1", 1)

    def test_ensure_path(self):
        client = self.client
        client.ensure_path("/1/2")
        self.assertTrue(client.exists("/1/2"))

        client.ensure_path("/1/2/3/4")
        self.assertTrue(client.exists("/1/2/3/4"))

    def test_sync(self):
        client = self.client
        self.assertTrue(client.sync('/'), '/')

    def test_exists(self):
        nodepath = "/" + uuid.uuid4().hex

        exists = self.client.exists(nodepath)
        eq_(exists, None)

        self.client.create(nodepath, b"sandwich", ephemeral=True)
        exists = self.client.exists(nodepath)
        self.assertTrue(exists)
        assert isinstance(exists.version, int)

        multi_node_nonexistent = "/" + uuid.uuid4().hex + "/hats"
        exists = self.client.exists(multi_node_nonexistent)
        eq_(exists, None)

    def test_exists_invalid_arguments(self):
        client = self.client
        self.assertRaises(TypeError, client.exists, ('a', 'b'))
        self.assertRaises(TypeError, client.exists, 'a', watch=True)

    def test_exists_watch(self):
        nodepath = "/" + uuid.uuid4().hex
        event = self.client.handler.event_object()

        def w(watch_event):
            eq_(watch_event.path, nodepath)
            event.set()

        exists = self.client.exists(nodepath, watch=w)
        eq_(exists, None)

        self.client.create(nodepath, ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_exists_watcher_exception(self):
        nodepath = "/" + uuid.uuid4().hex
        event = self.client.handler.event_object()

        # if the watcher throws an exception, all we can really do is log it
        def w(watch_event):
            eq_(watch_event.path, nodepath)
            event.set()

            raise Exception("test exception in callback")

        exists = self.client.exists(nodepath, watch=w)
        eq_(exists, None)

        self.client.create(nodepath, ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_create_delete(self):
        nodepath = "/" + uuid.uuid4().hex

        self.client.create(nodepath, b"zzz")

        self.client.delete(nodepath)

        exists = self.client.exists(nodepath)
        eq_(exists, None)

    def test_get_acls(self):
        from kazoo.security import make_digest_acl
        acl = make_digest_acl('user', 'pass', all=True)
        client = self.client
        try:
            client.create('/a', acl=[acl])
            self.assertTrue(acl in client.get_acls('/a')[0])
        finally:
            client.delete('/a')

    def test_get_acls_invalid_arguments(self):
        client = self.client
        self.assertRaises(TypeError, client.get_acls, ('a', 'b'))

    def test_set_acls(self):
        from kazoo.security import make_digest_acl
        acl = make_digest_acl('user', 'pass', all=True)
        client = self.client
        client.create('/a')
        try:
            client.set_acls('/a', [acl])
            self.assertTrue(acl in client.get_acls('/a')[0])
        finally:
            client.delete('/a')

    def test_set_acls_empty(self):
        client = self.client
        client.create('/a')
        self.assertRaises(InvalidACLError, client.set_acls, '/a', [])

    def test_set_acls_no_node(self):
        from kazoo.security import OPEN_ACL_UNSAFE
        client = self.client
        self.assertRaises(NoNodeError, client.set_acls, '/a', OPEN_ACL_UNSAFE)

    def test_set_acls_invalid_arguments(self):
        from kazoo.security import OPEN_ACL_UNSAFE
        single_acl = OPEN_ACL_UNSAFE[0]
        client = self.client
        self.assertRaises(TypeError, client.set_acls, ('a', 'b'), ())
        self.assertRaises(TypeError, client.set_acls, 'a', single_acl)
        self.assertRaises(TypeError, client.set_acls, 'a', 'all')
        self.assertRaises(TypeError, client.set_acls, 'a', [single_acl], 'V1')

    def test_set(self):
        client = self.client
        client.create('a', b'first')
        stat = client.set('a', b'second')
        data, stat2 = client.get('a')
        self.assertEqual(data, b'second')
        self.assertEqual(stat, stat2)

    def test_set_null_data(self):
        client = self.client
        client.create("/nulldata", b"not none")
        client.set("/nulldata", None)
        value, _ = client.get("/nulldata")
        self.assertEqual(value, None)

    def test_set_empty_string(self):
        client = self.client
        client.create("/empty", b"not empty")
        client.set("/empty", b"")
        value, _ = client.get("/empty")
        eq_(value, b"")

    def test_set_invalid_arguments(self):
        client = self.client
        client.create('a', b'first')
        self.assertRaises(TypeError, client.set, ('a', 'b'), b'value')
        self.assertRaises(TypeError, client.set, 'a', ['v', 'w'])
        self.assertRaises(TypeError, client.set, 'a', b'value', 'V1')

    def test_delete(self):
        client = self.client
        client.ensure_path('/a/b')
        self.assertTrue('b' in client.get_children('a'))
        client.delete('/a/b')
        self.assertFalse('b' in client.get_children('a'))

    def test_delete_recursive(self):
        client = self.client
        client.ensure_path('/a/b/c')
        client.ensure_path('/a/b/d')
        client.delete('/a/b', recursive=True)
        client.delete('/a/b/c', recursive=True)
        self.assertFalse('b' in client.get_children('a'))

    def test_delete_invalid_arguments(self):
        client = self.client
        client.ensure_path('/a/b')
        self.assertRaises(TypeError, client.delete, '/a/b', recursive='all')
        self.assertRaises(TypeError, client.delete, ('a', 'b'))
        self.assertRaises(TypeError, client.delete, '/a/b', version='V1')

    def test_get_children(self):
        client = self.client
        client.ensure_path('/a/b/c')
        client.ensure_path('/a/b/d')
        self.assertEqual(client.get_children('/a'), ['b'])
        self.assertEqual(set(client.get_children('/a/b')), set(['c', 'd']))
        self.assertEqual(client.get_children('/a/b/c'), [])

    def test_get_children2(self):
        client = self.client
        client.ensure_path('/a/b')
        children, stat = client.get_children('/a', include_data=True)
        value, stat2 = client.get('/a')
        self.assertEqual(children, ['b'])
        self.assertEqual(stat2.version, stat.version)

    def test_get_children2_many_nodes(self):
        client = self.client
        client.ensure_path('/a/b')
        client.ensure_path('/a/c')
        client.ensure_path('/a/d')
        children, stat = client.get_children('/a', include_data=True)
        value, stat2 = client.get('/a')
        self.assertEqual(set(children), set(['b', 'c', 'd']))
        self.assertEqual(stat2.version, stat.version)

    def test_get_children_no_node(self):
        client = self.client
        self.assertRaises(NoNodeError, client.get_children, '/none')
        self.assertRaises(NoNodeError, client.get_children,
                          '/none', include_data=True)

    def test_get_children_invalid_path(self):
        client = self.client
        self.assertRaises(ValueError, client.get_children, '../a')

    def test_get_children_invalid_arguments(self):
        client = self.client
        self.assertRaises(TypeError, client.get_children, ('a', 'b'))
        self.assertRaises(TypeError, client.get_children, 'a', watch=True)
        self.assertRaises(TypeError, client.get_children,
                          'a', include_data='yes')

    def test_invalid_auth(self):
        from kazoo.exceptions import AuthFailedError
        from kazoo.protocol.states import KeeperState

        client = self.client
        client.stop()
        client._state = KeeperState.AUTH_FAILED

        @raises(AuthFailedError)
        def testit():
            client.get('/')
        testit()

    def test_client_state(self):
        from kazoo.protocol.states import KeeperState
        eq_(self.client.client_state, KeeperState.CONNECTED)

    def test_update_host_list(self):
        from kazoo.client import KazooClient
        from kazoo.protocol.states import KeeperState
        hosts = self.cluster[0].address
        # create a client with only one server in its list
        client = KazooClient(hosts=hosts)
        client.start()

        # try to change the chroot, not currently allowed
        self.assertRaises(ConfigurationError,
                          client.set_hosts, hosts + '/new_chroot')

        # grow the cluster to 3
        client.set_hosts(self.servers)

        # shut down the first host
        try:
            self.cluster[0].stop()
            time.sleep(5)
            eq_(client.client_state, KeeperState.CONNECTED)
        finally:
            self.cluster[0].run()


dummy_dict = {
    'aversion': 1, 'ctime': 0, 'cversion': 1,
    'czxid': 110, 'dataLength': 1, 'ephemeralOwner': 'ben',
    'mtime': 1, 'mzxid': 1, 'numChildren': 0, 'pzxid': 1, 'version': 1
}


class TestClientTransactions(KazooTestCase):

    def setUp(self):
        KazooTestCase.setUp(self)
        skip = False
        if TRAVIS_ZK_VERSION and TRAVIS_ZK_VERSION < (3, 4):
            skip = True
        elif TRAVIS_ZK_VERSION and TRAVIS_ZK_VERSION >= (3, 4):
            skip = False
        else:
            ver = self.client.server_version()
            if ver[1] < 4:
                skip = True
        if skip:
            raise SkipTest("Must use Zookeeper 3.4 or above")

    def test_basic_create(self):
        t = self.client.transaction()
        t.create('/freddy')
        t.create('/fred', ephemeral=True)
        t.create('/smith', sequence=True)
        results = t.commit()
        eq_(results[0], '/freddy')
        eq_(len(results), 3)
        self.assertTrue(results[2].startswith('/smith0'))

    def test_bad_creates(self):
        args_list = [(True,), ('/smith', 0), ('/smith', b'', 'bleh'),
                     ('/smith', b'', None, 'fred'),
                     ('/smith', b'', None, True, 'fred')]

        @raises(TypeError)
        def testit(args):
            t = self.client.transaction()
            t.create(*args)

        for args in args_list:
            testit(args)

    def test_default_acl(self):
        from kazoo.security import make_digest_acl
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = make_digest_acl(username, password, all=True)

        self.client.add_auth("digest", digest_auth)
        self.client.default_acl = (acl,)

        t = self.client.transaction()
        t.create('/freddy')
        results = t.commit()
        eq_(results[0], '/freddy')

    def test_basic_delete(self):
        self.client.create('/fred')
        t = self.client.transaction()
        t.delete('/fred')
        results = t.commit()
        eq_(results[0], True)

    def test_bad_deletes(self):
        args_list = [(True,), ('/smith', 'woops'), ]

        @raises(TypeError)
        def testit(args):
            t = self.client.transaction()
            t.delete(*args)

        for args in args_list:
            testit(args)

    def test_set(self):
        self.client.create('/fred', b'01')
        t = self.client.transaction()
        t.set_data('/fred', b'oops')
        t.commit()
        res = self.client.get('/fred')
        eq_(res[0], b'oops')

    def test_bad_sets(self):
        args_list = [(42, 52), ('/smith', False), ('/smith', b'', 'oops')]

        @raises(TypeError)
        def testit(args):
            t = self.client.transaction()
            t.set_data(*args)

        for args in args_list:
            testit(args)

    def test_check(self):
        self.client.create('/fred')
        version = self.client.get('/fred')[1].version
        t = self.client.transaction()
        t.check('/fred', version)
        t.create('/blah')
        results = t.commit()
        eq_(results[0], True)
        eq_(results[1], '/blah')

    def test_bad_checks(self):
        args_list = [(42, 52), ('/smith', 'oops')]

        @raises(TypeError)
        def testit(args):
            t = self.client.transaction()
            t.check(*args)

        for args in args_list:
            testit(args)

    def test_bad_transaction(self):
        from kazoo.exceptions import RolledBackError, NoNodeError
        t = self.client.transaction()
        t.create('/fred')
        t.delete('/smith')
        results = t.commit()
        eq_(results[0].__class__, RolledBackError)
        eq_(results[1].__class__, NoNodeError)

    def test_bad_commit(self):
        t = self.client.transaction()

        @raises(ValueError)
        def testit():
            t.commit()

        t.committed = True
        testit()

    def test_bad_context(self):
        @raises(TypeError)
        def testit():
            with self.client.transaction() as t:
                t.check(4232)
        testit()

    def test_context(self):
        with self.client.transaction() as t:
            t.create('/smith', b'32')
        eq_(self.client.get('/smith')[0], b'32')


class TestCallbacks(unittest.TestCase):
    def test_session_callback_states(self):
        from kazoo.protocol.states import KazooState, KeeperState
        from kazoo.client import KazooClient

        client = KazooClient()
        client._handle = 1
        client._live.set()

        result = client._session_callback(KeeperState.CONNECTED)
        eq_(result, None)

        # Now with stopped
        client._stopped.set()
        result = client._session_callback(KeeperState.CONNECTED)
        eq_(result, None)

        # Test several state transitions
        client._stopped.clear()
        client.start_async = lambda: True
        client._session_callback(KeeperState.CONNECTED)
        eq_(client.state, KazooState.CONNECTED)

        client._session_callback(KeeperState.AUTH_FAILED)
        eq_(client.state, KazooState.LOST)

        client._handle = 1
        client._session_callback(-250)
        eq_(client.state, KazooState.SUSPENDED)


class TestNonChrootClient(KazooTestCase):

    def test_create(self):
        client = self._get_nonchroot_client()
        self.assertEqual(client.chroot, '')
        client.start()
        node = uuid.uuid4().hex
        path = client.create(node, ephemeral=True)
        client.delete(path)
        client.stop()

    def test_unchroot(self):
        client = self._get_nonchroot_client()
        client.chroot = '/a'
        self.assertEquals(client.unchroot('/a/b'), '/b')
        self.assertEquals(client.unchroot('/b/c'), '/b/c')


class TestReconfig(KazooTestCase):

    def setUp(self):
        KazooTestCase.setUp(self)
        if TRAVIS_ZK_VERSION:
            version = TRAVIS_ZK_VERSION
        else:
            version = self.client.server_version()
        if not version or version < (3, 5):
            raise SkipTest("Must use Zookeeper 3.5 or above")

    def test_add_remove_observer(self):
        def free_sock_port():
            s = socket.socket()
            s.bind(('', 0))
            return s, s.getsockname()[1]

        # get ports for election, zab and client endpoints. we need to use
        # ports for which we'd immediately get a RST upon connect(); otherwise
        # the cluster could crash if it gets a SocketTimeoutException:
        # https://issues.apache.org/jira/browse/ZOOKEEPER-2202
        s1, port1 = free_sock_port()
        s2, port2 = free_sock_port()
        s3, port3 = free_sock_port()

        joining = 'server.100=0.0.0.0:%d:%d:observer;0.0.0.0:%d' % (
            port1, port2, port3)
        data, _ = self.client.reconfig(joining=joining,
                                       leaving=None,
                                       new_members=None)
        self.assertIn(joining, data)

        data, _ = self.client.reconfig(joining=None,
                                       leaving='100',
                                       new_members=None)
        self.assertNotIn(joining, data)

        # try to add it again, but a config number in the future
        curver = int(data.split('\n')[-1].split('=')[1], base=16)
        self.assertRaises(BadVersionError,
                          self.client.reconfig,
                          joining=joining,
                          leaving=None,
                          new_members=None,
                          from_config=curver + 1)

    def test_bad_input(self):
        self.assertRaises(BadArgumentsError,
                          self.client.reconfig,
                          joining='some thing',
                          leaving=None,
                          new_members=None)
