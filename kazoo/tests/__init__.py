import atexit
import os
import unittest
import uuid

import zookeeper
from kazoo.client import KazooClient, KazooState
from kazoo.tests.common import ZookeeperCluster

ZK_HOME = os.environ.get("ZOOKEEPER_PATH")
assert ZK_HOME, (
    "ZOOKEEPER_PATH environment variable must be defined.\n "
    "For deb package installations this is /usr/share/java")

CLUSTER = ZookeeperCluster(ZK_HOME)
atexit.register(lambda cluster: cluster.terminate(), CLUSTER)


class KazooTestCase(unittest.TestCase):
    @property
    def cluster(self):
        return CLUSTER

    @property
    def servers(self):
        return ",".join([s.address for s in self.cluster])

    def _get_nonchroot_client(self):
        return KazooClient(self.servers)

    def _get_client(self, **kwargs):
        return KazooClient(self.hosts, **kwargs)

    def expire_session(self, client_id):
        client = KazooClient(self.cluster[1].address, client_id=client_id)
        client.connect()
        client.stop()

    def setUp(self):
        zookeeper.deterministic_conn_order(True)
        if not self.cluster[0].running:
            self.cluster.start()
        namespace = "/kazootests" + uuid.uuid4().hex
        self.hosts = self.servers + namespace

        self.client = self._get_client()

    def tearDown(self):
        if not self.cluster[0].running:
            self.cluster.start()

        if not self.client.connected:
            self.client.connect()
        self.client.recursive_delete('/')
        self.client.stop()
