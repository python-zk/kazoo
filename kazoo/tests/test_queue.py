import uuid

from nose.tools import eq_

from kazoo.testing import KazooTestCase


class KazooQueueTests(KazooTestCase):

    def setUp(self):
        super(KazooQueueTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_queue(self):
        queue = self.client.Queue(self.path)

        eq_(len(queue), 0)
        eq_(queue.qsize(), 0)
