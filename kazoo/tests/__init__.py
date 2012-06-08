import os
import unittest
import time
import uuid

from kazoo.client import KazooClient, KazooState

# if this env variable is set, ZK client integration tests are run
# against the specified host list
ENV_TEST_HOSTS = "KAZOO_TEST_HOSTS"


def get_hosts_or_skip():
    if ENV_TEST_HOSTS in os.environ:
        return os.environ[ENV_TEST_HOSTS]
    raise unittest.SkipTest("Skipping ZooKeeper test. To run, set "+
                            "%s env to a host list. (ex: localhost:2181)" %
                            ENV_TEST_HOSTS)


def get_client_or_skip(**kwargs):
    hosts = get_hosts_or_skip()
    return KazooClient(hosts, **kwargs)


def until_timeout(timeout, value=None):
    """Returns an iterator that repeats until a timeout is reached

    timeout is in seconds
    """

    start = time.time()

    while True:
        if time.time() - start >= timeout:
            raise Exception("timed out before success!")
        yield value


class KazooTestCase(unittest.TestCase):
    def setUp(self):
        self.hosts = get_hosts_or_skip()

        self.namespace = "/kazootests" + uuid.uuid4().hex
        self.client = KazooClient(self.hosts, namespace=self.namespace)

    def tearDown(self):
        if self.client.state == KazooState.LOST:
            self.client.connect()
        self.client.recursive_delete(self.namespace)
        self.client.close()
