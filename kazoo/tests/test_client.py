import threading
import uuid
import unittest

from nose.tools import eq_

from kazoo.testing import KazooTestCase
from kazoo.testing import ZooError
from kazoo.exceptions import BadArgumentsError
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NoAuthError
from kazoo.exceptions import ZookeeperError
from kazoo.exceptions import ConnectionLoss


class TestConnection(KazooTestCase):
    def _makeAuth(self, *args, **kwargs):
        from kazoo.security import make_digest_acl
        return make_digest_acl(*args, **kwargs)

    def test_auth(self):
        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = self._makeAuth(username, password, all=True)

        self.client.add_auth("digest", digest_auth)

        self.client.default_acl = (acl,)

        self.client.create("/1", "")
        self.client.create("/1/2", "")

        eve = self._get_client()
        eve.start()

        try:
            self.assertRaises(NoAuthError, eve.get, "/1/2")

            # try again with the wrong auth token
            eve.add_auth("digest", "badbad:bad")

            self.assertRaises(NoAuthError, eve.get, "/1/2")
        finally:
            # Ensure we remove the ACL protected nodes
            self.client.delete("/1", recursive=True)

    def test_session_expire(self):
        from kazoo.protocol.states import KazooState

        cv = threading.Event()

        def watch_events(event):
            if event == KazooState.LOST:
                cv.set()

        self.client.add_listener(watch_events)
        self.expire_session()
        cv.wait(3)
        assert cv.is_set()

    def test_bad_session_expire(self):
        from kazoo.protocol.states import KazooState

        cv = threading.Event()
        ab = threading.Event()

        def watch_events(event):
            if event == KazooState.LOST:
                ab.set()
                raise Exception("oops")
                cv.set()

        self.client.add_listener(watch_events)
        self.expire_session()
        ab.wait(0.5)
        assert ab.is_set()
        cv.wait(0.5)
        assert not cv.is_set()

    def test_state_listener(self):
        from kazoo.protocol.states import KazooState
        states = []
        condition = threading.Condition()

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

    def test_no_connection(self):
        from kazoo.exceptions import SessionExpiredError
        client = self.client
        client.stop()
        self.assertFalse(client.connected)
        self.assertTrue(client.client_id is None)
        self.assertRaises(SessionExpiredError, client.exists, '/')


class TestClient(KazooTestCase):
    def _getKazooState(self):
        from kazoo.protocol.states import KazooState
        return KazooState

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

    def test_create_unicode_path(self):
        client = self.client
        path = client.create(u"/ascii")
        eq_(path, u"/ascii")
        path = client.create(u"/\xe4hm")
        eq_(path, u"/\xe4hm")

    def test_create_invalid_path(self):
        client = self.client
        self.assertRaises(ValueError, client.create, ".")
        self.assertRaises(ValueError, client.create, "/a/../b")
        self.assertRaises(BadArgumentsError, client.create, "/b\x00")
        self.assertRaises(BadArgumentsError, client.create, "/b\x1e")

    def test_create_value(self):
        client = self.client
        client.create("/1", "bytes")
        data, stat = client.get("/1")
        eq_(data, "bytes")

    def test_create_unicode_value(self):
        client = self.client
        self.assertRaises(TypeError, client.create, "/1", u"\xe4hm")

    def test_create_large_value(self):
        client = self.client
        kb_512 = "a" * (512 * 1024)
        client.create("/1", kb_512)
        self.assertTrue(client.exists("/1"))
        mb_2 = "a" * (2 * 1024 * 1024)
        self.assertRaises(ConnectionLoss, client.create, "/2", mb_2)

    def test_create_ephemeral(self):
        client = self.client
        client.create("/1", "ephemeral", ephemeral=True)
        data, stat = client.get("/1")
        eq_(data, "ephemeral")
        eq_(stat.ephemeralOwner, client.client_id[0])

    def test_create_no_ephemeral(self):
        client = self.client
        client.create("/1", "val1")
        data, stat = client.get("/1")
        self.assertFalse(stat.ephemeralOwner)

    def test_create_ephemeral_no_children(self):
        from kazoo.exceptions import NoChildrenForEphemeralsError
        client = self.client
        client.create("/1", "ephemeral", ephemeral=True)
        self.assertRaises(NoChildrenForEphemeralsError,
            client.create, "/1/2", "val1")
        self.assertRaises(NoChildrenForEphemeralsError,
            client.create, "/1/2", "val1", ephemeral=True)

    def test_create_sequence(self):
        client = self.client
        client.create("/folder", "")
        path = client.create("/folder/a", "sequence", sequence=True)
        eq_(path, "/folder/a0000000000")
        path2 = client.create("/folder/a", "sequence", sequence=True)
        eq_(path2, "/folder/a0000000001")

    def test_create_ephemeral_sequence(self):
        basepath = "/" + uuid.uuid4().hex
        realpath = self.client.create(basepath, "sandwich", sequence=True,
            ephemeral=True)
        self.assertTrue(basepath != realpath and realpath.startswith(basepath))
        data, stat = self.client.get(realpath)
        eq_(data, "sandwich")

    def test_create_makepath(self):
        self.client.create("/1/2", "val1", makepath=True)
        data, stat = self.client.get("/1/2")
        eq_(data, "val1")

        self.client.create("/1/2/3/4/5", "val2", makepath=True)
        data, stat = self.client.get("/1/2/3/4/5")
        eq_(data, "val2")

    def test_create_no_makepath(self):
        self.assertRaises(NoNodeError, self.client.create, "/1/2", "val1")
        self.assertRaises(NoNodeError, self.client.create, "/1/2", "val1",
            makepath=False)

    def test_create_exists(self):
        from kazoo.exceptions import NodeExistsError
        client = self.client
        path = client.create("/1", "")
        self.assertRaises(NodeExistsError, client.create, path, "")

    def test_create_get_set(self):
        nodepath = "/" + uuid.uuid4().hex

        self.client.create(nodepath, "sandwich", ephemeral=True)

        data, stat = self.client.get(nodepath)
        eq_(data, "sandwich")

        newstat = self.client.set(nodepath, "hats", stat.version)
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

    def test_exists(self):
        nodepath = "/" + uuid.uuid4().hex

        exists = self.client.exists(nodepath)
        eq_(exists, None)

        self.client.create(nodepath, "sandwich", ephemeral=True)
        exists = self.client.exists(nodepath)
        self.assertTrue(exists)
        assert isinstance(exists.version, int)

        multi_node_nonexistent = "/" + uuid.uuid4().hex + "/hats"
        exists = self.client.exists(multi_node_nonexistent)
        eq_(exists, None)

    def test_exists_watch(self):
        nodepath = "/" + uuid.uuid4().hex

        event = threading.Event()

        def w(watch_event):
            eq_(watch_event.path, nodepath)
            event.set()

        exists = self.client.exists(nodepath, watch=w)
        eq_(exists, None)

        self.client.create(nodepath, "x", ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_exists_watcher_exception(self):
        nodepath = "/" + uuid.uuid4().hex

        event = threading.Event()

        # if the watcher throws an exception, all we can really do is log it
        def w(watch_event):
            eq_(watch_event.path, nodepath)
            event.set()

            raise Exception("test exception in callback")

        exists = self.client.exists(nodepath, watch=w)
        eq_(exists, None)

        self.client.create(nodepath, "x", ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_create_delete(self):
        nodepath = "/" + uuid.uuid4().hex

        self.client.create(nodepath, "zzz")

        self.client.delete(nodepath)

        exists = self.client.exists(nodepath)
        eq_(exists, None)

dummy_dict = {
    'aversion': 1, 'ctime': 0, 'cversion': 1,
    'czxid': 110, 'dataLength': 1, 'ephemeralOwner': 'ben',
    'mtime': 1, 'mzxid': 1, 'numChildren': 0, 'pzxid': 1, 'version': 1
}


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
