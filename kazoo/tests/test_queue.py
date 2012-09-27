import uuid

from nose.tools import eq_

from kazoo.testing import KazooTestCase


class BaseQueueTests(object):

    def test_queue_validation(self):
        queue = self._makeOne()
        self.assertRaises(TypeError, queue.put, {})

    def test_empty_queue(self):
        queue = self._makeOne()
        eq_(len(queue), 0)
        eq_(queue.qsize(), 0)
        self.assertTrue(queue.get() is None)
        eq_(len(queue), 0)

    def test_queue(self):
        queue = self._makeOne()
        queue.put(b"one")
        queue.put(b"two")
        queue.put(b"three")
        eq_(len(queue), 3)

        eq_(queue.get(), b"one")
        eq_(queue.get(), b"two")
        eq_(queue.get(), b"three")
        eq_(len(queue), 0)


class KazooQueueTests(KazooTestCase, BaseQueueTests):

    def _makeOne(self):
        path = "/" + uuid.uuid4().hex
        return self.client.Queue(path)


class KazooPriorityQueueTests(KazooTestCase, BaseQueueTests):

    def _makeOne(self):
        path = "/" + uuid.uuid4().hex
        return self.client.PriorityQueue(path)
