import uuid
import time


from nose.tools import eq_
from nose.tools import raises

from kazoo.testing import KazooTestCase


class KazooChildrenWatcherTests(KazooTestCase):
    def setUp(self):
        super(KazooChildrenWatcherTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def _makeOne(self, *args, **kwargs):
        from kazoo.recipe.partitioner import ChildrenWatcher
        return ChildrenWatcher(*args, **kwargs)

    def test_watch(self):
        self.client.ensure_path(self.path)
        watcher = self._makeOne(self.client, self.path, 0.1)
        result = watcher.start()
        children, asy = result.get()
        eq_(len(children), 0)
        eq_(asy.ready(), False)

        self.client.create(self.path + '/' + 'fred', '0')
        asy.get(timeout=1)
        eq_(asy.ready(), True)

    def test_exception(self):
        from kazoo.exceptions import NoNodeException
        watcher = self._makeOne(self.client, self.path, 0.1)
        result = watcher.start()

        @raises(NoNodeException)
        def testit():
            result.get()
        testit()

    def test_watch_iterations(self):
        self.client.ensure_path(self.path)
        watcher = self._makeOne(self.client, self.path, 0.2)
        result = watcher.start()
        eq_(result.ready(), False)

        time.sleep(0.1)
        self.client.create(self.path + '/' + uuid.uuid4().hex, '0')
        eq_(result.ready(), False)
        time.sleep(0.1)
        eq_(result.ready(), False)
        self.client.create(self.path + '/' + uuid.uuid4().hex, '0')
        time.sleep(0.1)
        eq_(result.ready(), False)

        children, asy = result.get()
        eq_(len(children), 2)


class KazooPartitionerTests(KazooTestCase):
    def setUp(self):
        super(KazooPartitionerTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_party_of_one(self):
        partitioner = self.client.SetPartitioner(
            self.path, set=(1, 2, 3), time_boundary=0.2)
        eq_(partitioner.acquired, False)
        partitioner.wait_for_acquire(1)
        eq_(partitioner.acquired, True)
        eq_(list(partitioner), [1, 2, 3])
        partitioner.finish()

    def test_party_of_two(self):
        partitioners = [self.client.SetPartitioner(self.path, (1, 2),
                        identifier="p%s" % i, time_boundary=0.2)
                        for i in range(2)]

        eq_(partitioners[0].acquired, False)
        partitioners[0].wait_for_acquire(1)
        partitioners[1].wait_for_acquire(1)
        eq_(list(partitioners[0]), [1])
        eq_(list(partitioners[1]), [2])
        partitioners[0].finish()
        time.sleep(0.1)
        eq_(partitioners[1].release, True)
        partitioners[1].finish()

    def test_party_expansion(self):
        partitioners = [self.client.SetPartitioner(self.path, (1, 2, 3),
                        identifier="p%s" % i, time_boundary=0.2)
                        for i in range(2)]

        eq_(partitioners[0].acquired, False)
        partitioners[0].wait_for_acquire(1)
        partitioners[1].wait_for_acquire(1)

        eq_(list(partitioners[0]), [1, 3])
        eq_(list(partitioners[1]), [2])

        # Add another partition, wait till they settle
        partitioners.append(self.client.SetPartitioner(self.path, (1, 2, 3),
                            identifier="p2", time_boundary=0.2))
        time.sleep(0.1)
        eq_(partitioners[0].release, True)
        for p in partitioners[:-1]:
            p.release_set()

        for p in partitioners:
            p.wait_for_acquire(1)

        eq_(list(partitioners[0]), [1])
        eq_(list(partitioners[1]), [2])
        eq_(list(partitioners[2]), [3])

        for p in partitioners:
            p.finish()

    def test_more_members_than_set_items(self):
        partitioners = [self.client.SetPartitioner(self.path, (1,),
                        identifier="p%s" % i, time_boundary=0.2)
                        for i in range(2)]

        eq_(partitioners[0].acquired, False)
        partitioners[0].wait_for_acquire(1)
        partitioners[1].wait_for_acquire(1)

        eq_(list(partitioners[0]), [1])
        eq_(list(partitioners[1]), [])

        for p in partitioners:
            p.finish()
