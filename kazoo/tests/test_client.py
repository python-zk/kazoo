import threading
import uuid
import unittest

from nose.tools import eq_

from kazoo.testing import KazooTestCase
from kazoo.testing import ZooError
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NoAuthError


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
        from kazoo.exceptions import ConnectionClosedError
        self.client.stop()
        self.assertRaises(ConnectionClosedError, self.client.exists, '/')


class TestClient(KazooTestCase):
    def _getKazooState(self):
        from kazoo.protocol.states import KazooState
        return KazooState

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

    def test_create_no_makepath(self):
        self.assertRaises(NoNodeError, self.client.create, "/1/2", "val1")
        self.assertRaises(NoNodeError, self.client.create, "/1/2", "val1",
            makepath=False)

    def test_create_makepath(self):
        self.client.create("/1/2", "val1", makepath=True)
        data, stat = self.client.get("/1/2")
        eq_(data, "val1")

        self.client.create("/1/2/3/4/5", "val2", makepath=True)
        data, stat = self.client.get("/1/2/3/4/5")
        eq_(data, "val2")

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

    def test_create_get_sequential(self):
        basepath = "/" + uuid.uuid4().hex
        realpath = self.client.create(basepath, "sandwich", sequence=True,
            ephemeral=True)

        self.assertTrue(basepath != realpath and realpath.startswith(basepath))

        data, stat = self.client.get(realpath)
        eq_(data, "sandwich")

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
