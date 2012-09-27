import uuid

from nose.tools import eq_

from kazoo.testing import KazooTestCase


class KazooQueueTests(KazooTestCase):

    def setUp(self):
        super(KazooQueueTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_queue_validation(self):
        queue = self.client.Queue(self.path)
        self.assertRaises(TypeError, queue.put, {})

    def test_queue(self):
        queue = self.client.Queue(self.path)

        eq_(len(queue), 0)
        eq_(queue.qsize(), 0)

        queue.put(b"one")
        queue.put(b"two")
        queue.put(b"three")

        eq_(len(queue), 3)

        eq_(queue.get(), b"one")
        eq_(queue.get(), b"two")
        eq_(queue.get(), b"three")

        eq_(len(queue), 0)
