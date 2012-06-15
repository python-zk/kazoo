import threading
import uuid

from nose.tools import eq_

from kazoo.tests import KazooTestCase
from kazoo.exceptions import NoNodeException
from kazoo.exceptions import NoAuthException


class TestClient(KazooTestCase):
    @property
    def zk(self):
        return self.client

    def _getKazooState(self):
        from kazoo.client import KazooState
        return KazooState

    def _makeAuth(self, *args, **kwargs):
        from kazoo.security import make_digest_acl
        return make_digest_acl(*args, **kwargs)

    def test_ensure_path(self):
        client = self.client

        client.connect()

        client.ensure_path("/1/2")
        self.assertTrue(client.exists("/1/2"))

        client.ensure_path("/1/2/3/4")
        self.assertTrue(client.exists("/1/2/3/4"))

    def test_state_listener(self):
        from kazoo.client import KazooState
        states = []
        condition = threading.Condition()

        def listener(state):
            with condition:
                states.append(state)
                condition.notify_all()

        self.client.add_listener(listener)
        self.client.connect(5)

        with condition:
            if not states:
                condition.wait(5)

        eq_(len(states), 1)
        eq_(states[0], KazooState.CONNECTED)

    def test_create_no_makepath(self):
        self.client.connect()

        self.assertRaises(NoNodeException, self.client.create, "/1/2", "val1")
        self.assertRaises(NoNodeException, self.client.create, "/1/2", "val1",
            makepath=False)

    def test_bad_create_args(self):
        # We need a non-namespaced client for this test
        client = self._get_nonchroot_client()
        try:
            client.connect()
            self.assertRaises(ValueError, client.create, "1/2", "val1")
        finally:
            client.stop()

    def test_create_makepath(self):
        self.client.connect()

        self.client.create("/1/2", "val1", makepath=True)
        data, stat = self.client.get("/1/2")
        self.assertEqual(data, "val1")

        self.client.create("/1/2/3/4/5", "val2", makepath=True)
        data, stat = self.client.get("/1/2/3/4/5")
        self.assertEqual(data, "val2")

    def test_create_get_set(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        self.zk.create(nodepath, "sandwich", ephemeral=True)

        data, stat = self.zk.get(nodepath)
        eq_(data, "sandwich")

        newstat = self.zk.set(nodepath, "hats", stat.version)
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

    def test_create_get_sequential(self):
        self.client.connect()
        self.client.ensure_path("/")

        basepath = "/" + uuid.uuid4().hex
        realpath = self.zk.create(basepath, "sandwich", sequence=True,
            ephemeral=True)

        self.assertTrue(basepath != realpath and realpath.startswith(basepath))

        data, stat = self.zk.get(realpath)
        self.assertEqual(data, "sandwich")

    def test_exists(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        exists = self.zk.exists(nodepath)
        eq_(exists, None)

        self.zk.create(nodepath, "sandwich", ephemeral=True)
        exists = self.zk.exists(nodepath)
        self.assertTrue(exists)
        assert "version" in exists

        multi_node_nonexistent = "/" + uuid.uuid4().hex + "/hats"
        exists = self.zk.exists(multi_node_nonexistent)
        eq_(exists, None)

    def test_exists_watch(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        event = threading.Event()

        def w(watch_event):
            eq_(watch_event.path, nodepath)
            event.set()

        exists = self.zk.exists(nodepath, watch=w)
        eq_(exists, None)

        self.zk.create(nodepath, "x", ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_exists_watcher_exception(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        event = threading.Event()

        # if the watcher throws an exception, all we can really do is log it
        def w(watch_event):
            eq_(watch_event.path, nodepath)
            event.set()

            raise Exception("test exception in callback")

        exists = self.zk.exists(nodepath, watch=w)
        eq_(exists, None)

        self.zk.create(nodepath, "x", ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_create_delete(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        self.zk.create(nodepath, "zzz")

        self.zk.delete(nodepath)

        exists = self.zk.exists(nodepath)
        eq_(exists, None)

    def test_auth(self):
        self.client.connect()
        self.client.ensure_path("/")

        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = self._makeAuth(username, password, all=True)

        self.client.add_auth("digest", digest_auth)

        self.client.default_acl = (acl,)

        self.client.create("/1", "")
        self.client.create("/1/2", "")

        eve = self._get_client()
        eve.connect()

        try:
            self.assertRaises(NoAuthException, eve.get, "/1/2")

            # try again with the wrong auth token
            eve.add_auth("digest", "badbad:bad")

            self.assertRaises(NoAuthException, eve.get, "/1/2")
        finally:
            # Ensure we remove the ACL protected nodes
            self.client.recursive_delete("/1")
