import uuid
import time

from nose.tools import eq_

from kazoo.testing import KazooTestCase
from kazoo.recipe.partitioner import PartitionState


class KazooPartitionerTests(KazooTestCase):
    def setUp(self):
        super(KazooPartitionerTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex
        self.__partitioners = []

    def test_party_of_one(self):
        self.__create_partitioner(size=3)
        self.__wait_for_acquire()
        self.__assert_state(PartitionState.ACQUIRED)
        self.__assert_partitions([0, 1, 2])
        self.__finish()

    def test_party_of_two(self):
        for i in range(2):
            self.__create_partitioner(size=2, identifier=str(i))

        self.__wait_for_acquire()
        self.__assert_partitions([0], [1])

        self.__partitioners[0].finish()
        self.__wait()
        eq_(self.__partitioners[1].release, True)
        self.__partitioners[1].finish()

    def test_party_expansion(self):
        for i in range(2):
            self.__create_partitioner(size=3, identifier=str(i))

        self.__wait_for_acquire()
        self.__assert_state(PartitionState.ACQUIRED)
        self.__assert_partitions([0, 2], [1])

        # Add another partition, wait till they settle
        self.__create_partitioner(size=3, identifier="2")
        self.__wait()

        self.__assert_state(PartitionState.RELEASE,
                            partitioners=self.__partitioners[:-1])
        self.__release(self.__partitioners[:-1])

        self.__wait_for_acquire()
        self.__assert_partitions([0], [1], [2])

        self.__finish()

    def test_more_members_than_set_items(self):
        for i in range(2):
            self.__create_partitioner(size=1, identifier=str(i))

        self.__wait_for_acquire()
        self.__assert_state(PartitionState.ACQUIRED)
        self.__assert_partitions([0], [])

        self.__finish()

    def test_party_session_failure(self):
        partitioner = self.__create_partitioner(size=3)
        self.__wait_for_acquire()
        eq_(partitioner.state, PartitionState.ACQUIRED)
        # simulate session failure
        partitioner._fail_out()
        partitioner.release_set()
        self.assertTrue(partitioner.failed)

    def __create_partitioner(self, size, identifier=None):
        partitioner = self.client.SetPartitioner(
            self.path, set=range(size), time_boundary=0.2, identifier=identifier)
        self.__partitioners.append(partitioner)
        return partitioner

    def __wait_for_acquire(self):
        for partitioner in self.__partitioners:
            partitioner.wait_for_acquire(14)

    def __assert_state(self, state, partitioners=None):
        if partitioners is None:
            partitioners = self.__partitioners

        for partitioner in partitioners:
            eq_(partitioner.state, state)

    def __assert_partitions(self, *partitions):
        eq_(len(partitions), len(self.__partitioners))
        for partitioner, own_partitions in zip(self.__partitioners, partitions):
            eq_(list(partitioner), own_partitions)

    def __wait(self):
        time.sleep(0.1)

    def __release(self, partitioners=None):
        if partitioners is None:
            partitioners = self.__partitioners

        for partitioner in partitioners:
            partitioner.release_set()

    def __finish(self):
        for partitioner in self.__partitioners:
            partitioner.finish()