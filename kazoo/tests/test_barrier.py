from nose.tools import eq_

from kazoo.testing import KazooTestCase


class KazooBarrierTests(KazooTestCase):
    def test_barrier_not_exist(self):
        b = self.client.Barrier("/some/path")
        eq_(b.wait(), True)

    def test_barrier_exists(self):
        b = self.client.Barrier("/some/path")
        b.create()
        eq_(b.wait(0), False)
        b.remove()
        eq_(b.wait(), True)

    def test_remove_nonexistent_barrier(self):
        b = self.client.Barrier("/some/path")
        eq_(b.remove(), False)
