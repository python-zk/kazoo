import atexit
import os
import unittest
import time
import uuid

from kazoo.client import KazooClient, KazooState
from kazoo.tests.common import ZookeeperCluster

# if this env variable is set, ZK client integration tests are run
# against the specified host list
ENV_TEST_HOSTS = "KAZOO_TEST_HOSTS"


ZK_HOME = os.environ.get("ZOOKEEPER_PATH")
assert ZK_HOME, (
    "ZOOKEEPER_PATH environment variable must be defined.\n "
    "For deb package installations this is /usr/share/java")

CLUSTER = ZookeeperCluster(ZK_HOME)
atexit.register(lambda cluster: cluster.terminate(), CLUSTER)


def until_timeout(timeout, value=None):
    """Returns an iterator that repeats until a timeout is reached

    timeout is in seconds
    """

    start = time.time()

    while True:
        if time.time() - start >= timeout:
            raise Exception("timed out before success!")
        yield value


_started = []


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

    def setUp(self):
        if not _started:
            self.cluster.start()
            _started.append(True)
        namespace = "/kazootests" + uuid.uuid4().hex
        self.hosts = self.servers + namespace

        self.client = self._get_client()

    def tearDown(self):
        if self.client.state == KazooState.LOST:
            self.client.connect()
        self.client.recursive_delete('/')
        self.client.stop()
