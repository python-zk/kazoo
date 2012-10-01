import uuid

from nose.tools import eq_

from kazoo.testing import KazooTestCase


class KazooQueueTests(KazooTestCase):

    def _makeOne(self):
        path = "/" + uuid.uuid4().hex
        return self.client.Queue(path)

    def test_queue_validation(self):
        queue = self._makeOne()
        self.assertRaises(TypeError, queue.put, {})
        self.assertRaises(TypeError, queue.put, b"one", b"100")
        self.assertRaises(TypeError, queue.put, b"one", 10.0)
        self.assertRaises(ValueError, queue.put, b"one", -100)
        self.assertRaises(ValueError, queue.put, b"one", 100000)

    def test_empty_queue(self):
        queue = self._makeOne()
        eq_(len(queue), 0)
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

    def test_priority(self):
        queue = self._makeOne()
        queue.put(b"four", priority=101)
        queue.put(b"one", priority=0)
        queue.put(b"two", priority=0)
        queue.put(b"three", priority=10)

        eq_(queue.get(), b"one")
        eq_(queue.get(), b"two")
        eq_(queue.get(), b"three")
        eq_(queue.get(), b"four")
