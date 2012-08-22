import logging
import os
import threading
import uuid
import unittest

import zookeeper
from nose.tools import eq_

import kazoo.client
import kazoo.klog
from kazoo.testing import KazooTestCase
from kazoo.testing import ZooError
from kazoo.exceptions import NoNodeException
from kazoo.exceptions import NoAuthException
from kazoo.tests.util import InstalledHandler
from kazoo.tests.util import wait


class LoggingTests(unittest.TestCase):
    def test_logging(self):
        handler = InstalledHandler('ZooKeeper')
        try:
            handle = zookeeper.init('zookeeper.example.com:2181')
            zookeeper.close(handle)
        except Exception:
            pass

        wait(lambda: [r for r in handler.records
                       if 'environment' in r.getMessage()]
             )
        handler.clear()
        kazoo.klog.setup_logging()

        # Test that the filter for the "Exceeded deadline by" noise works.
        # cheat and bypass zk by writing to the pipe directly.
        os.write(kazoo.klog._logging_pipe[1],
                 '2012-01-06 16:45:44,572:43673(0x1004f6000):ZOO_WARN@'
                 'zookeeper_interest@1461: Exceeded deadline by 27747ms\n')
        wait(lambda: [r for r in handler.records
                       if ('Exceeded deadline by' in r.getMessage()
                           and r.levelno == logging.DEBUG)
                       ]
             )

        self.assertFalse([r for r in handler.records
                          if ('Exceeded deadline by' in r.getMessage()
                              and r.levelno == logging.WARNING)
                          ])

        handler.uninstall()


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
            self.assertRaises(NoAuthException, eve.get, "/1/2")

            # try again with the wrong auth token
            eve.add_auth("digest", "badbad:bad")

            self.assertRaises(NoAuthException, eve.get, "/1/2")
        finally:
            # Ensure we remove the ACL protected nodes
            self.client.delete("/1", recursive=True)

    def test_session_expire(self):
        from kazoo.client import KazooState

        cv = threading.Event()

        def watch_events(event):
            if event == KazooState.LOST:
                cv.set()

        self.client.add_listener(watch_events)
        self.expire_session()
        cv.wait(3)
        assert cv.is_set()

    def test_bad_session_expire(self):
        from kazoo.client import KazooState

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
        from kazoo.client import KazooState
        states = []
        condition = threading.Condition()

        def listener(state):
            with condition:
                states.append(state)
                condition.notify_all()

        self.client.stop()
        self.client.add_listener(listener)
        self.client.start(5)

        with condition:
            if not states:
                condition.wait(5)

        eq_(len(states), 1)
        eq_(states[0], KazooState.CONNECTED)

    def test_no_connection(self):
        from kazoo.exceptions import ZookeeperStoppedError
        self.client.stop()
        self.assertRaises(ZookeeperStoppedError, self.client.exists, '/')


class TestClient(KazooTestCase):
    def _getKazooState(self):
        from kazoo.client import KazooState
        return KazooState

    def test_legacy_error(self):
        self.add_errors(dict(
            acreate=[ZooError('call', SystemError, False)]
        ))
        self.assertRaises(zookeeper.InvalidStateException, self.client.create,
                          "/1", "val1")

    def test_bad_arguments(self):
        self.add_errors(dict(
            acreate=[ZooError('call', zookeeper.BadArgumentsException, False)]
        ))
        self.assertRaises(zookeeper.BadArgumentsException, self.client.create,
                          "/1", "val1")

    def test_bad_handle_type_error(self):
        self.add_errors(dict(
            acreate=[ZooError('call', TypeError("an integer is required"),
                              False)]
        ))
        self.assertRaises(zookeeper.SessionExpiredException, self.client.create,
                          "/1", "val1")

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
        self.assertRaises(NoNodeException, self.client.create, "/1/2", "val1")
        self.assertRaises(NoNodeException, self.client.create, "/1/2", "val1",
            makepath=False)

    def test_bad_create_args(self):
        # We need a non-namespaced client for this test
        client = self._get_nonchroot_client()
        try:
            client.start()
            self.assertRaises(ValueError, client.create, "1/2", "val1")
        finally:
            client.stop()

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
        assert "version" in exists

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
    def test_exists_callback(self):
        from kazoo.client import _exists_callback
        from kazoo.handlers.threading import SequentialThreadingHandler
        handler = SequentialThreadingHandler()
        asy = handler.async_result()
        _exists_callback(asy, 0, zookeeper.OK, True)
        eq_(asy.get(), True)

        asy = handler.async_result()
        _exists_callback(asy, 0, zookeeper.CONNECTIONLOSS, False)
        self.assertRaises(zookeeper.ConnectionLossException, asy.get)

    def test_generic_callback_ok(self):
        from kazoo.client import _generic_callback
        from kazoo.handlers.threading import SequentialThreadingHandler
        handler = SequentialThreadingHandler()

        # No args
        asy = handler.async_result()
        _generic_callback(asy, 0, zookeeper.OK)
        eq_(asy.get(), None)

        # One arg thats not a dict
        asy = handler.async_result()
        _generic_callback(asy, 0, zookeeper.OK, 12)
        eq_(asy.get(), 12)

        # One arg thats a node struct
        asy = handler.async_result()
        _generic_callback(asy, 0, zookeeper.OK, dummy_dict)
        eq_(asy.get().acl_version, 1)

        # two args, second is struct
        asy = handler.async_result()
        _generic_callback(asy, 0, zookeeper.OK, 11, dummy_dict)
        val = asy.get()
        eq_(val[1].acl_version, 1)
        eq_(val[0], 11)

    def test_generic_callback_error(self):
        from kazoo.client import _generic_callback
        from kazoo.handlers.threading import SequentialThreadingHandler
        handler = SequentialThreadingHandler()

        asy = handler.async_result()
        _generic_callback(asy, 0, zookeeper.CONNECTIONLOSS)
        self.assertRaises(zookeeper.ConnectionLossException, asy.get)

    def test_session_callback_states(self):
        from kazoo.client import (KazooClient, KazooState, KeeperState,
            EventType)

        client = KazooClient()
        client._handle = 1
        client._live.set()

        result = client._session_callback(1, EventType.CREATED,
                                          KeeperState.CONNECTED, '/')
        eq_(result, None)

        # Now with stopped
        client._stopped.set()
        result = client._session_callback(1, EventType.SESSION,
                                          KeeperState.CONNECTED, '/')
        eq_(result, None)

        # Test several state transitions
        client._stopped.clear()
        client.start_async = lambda: True
        client._session_callback(1, EventType.SESSION, KeeperState.CONNECTED,
                                 None)
        eq_(client.state, KazooState.CONNECTED)

        client._session_callback(1, EventType.SESSION, KeeperState.AUTH_FAILED,
                                 None)
        eq_(client._handle, None)
        eq_(client.state, KazooState.LOST)

        client._handle = 1
        client._session_callback(1, EventType.SESSION, -250, None)
        eq_(client.state, KazooState.SUSPENDED)

        # handle mismatch
        client._handle = 0
        # This will be ignored due to handle mismatch
        client._session_callback(1, EventType.SESSION, KeeperState.CONNECTED,
                                 None)
        eq_(client.state, KazooState.SUSPENDED)
