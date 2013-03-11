import uuid, threading

from nose.tools import eq_, ok_

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


class KazooLockingQueueTests(KazooTestCase):

    def _makeOne(self):
        path = "/" + uuid.uuid4().hex
        return self.client.LockingQueue(path)

    def test_queue_validation(self):
        queue = self._makeOne()
        self.assertRaises(TypeError, queue.put, {})
        self.assertRaises(TypeError, queue.put, b"one", b"100")
        self.assertRaises(TypeError, queue.put, b"one", 10.0)
        self.assertRaises(ValueError, queue.put, b"one", -100)
        self.assertRaises(ValueError, queue.put, b"one", 100000)
        self.assertRaises(TypeError, queue.put_all, {})
        self.assertRaises(TypeError, queue.put_all, [{}])
        self.assertRaises(TypeError, queue.put_all, [b"one"], b"100")
        self.assertRaises(TypeError, queue.put_all, [b"one"], 10.0)
        self.assertRaises(ValueError, queue.put_all, [b"one"], -100)
        self.assertRaises(ValueError, queue.put_all, [b"one"], 100000)

    def test_empty_queue(self):
        queue = self._makeOne()
        eq_(len(queue), 0)
        self.assertTrue(queue.get(0) is None)
        eq_(len(queue), 0)

    def test_queue(self):
        queue = self._makeOne()
        queue.put(b"one")
        queue.put_all([b"two", b"three"])
        eq_(len(queue), 3)

        ok_(not queue.consume())
        ok_(not queue.holds_lock())
        eq_(queue.get(1), b"one")
        ok_(queue.holds_lock())
        # Without consuming, should return the same element
        eq_(queue.get(1), b"one")
        ok_(queue.consume())
        ok_(not queue.holds_lock())
        eq_(queue.get(1), b"two")
        ok_(queue.holds_lock())
        ok_(queue.consume())
        ok_(not queue.holds_lock())
        eq_(queue.get(1), b"three")
        ok_(queue.holds_lock())
        ok_(queue.consume())
        ok_(not queue.holds_lock())
        ok_(not queue.consume())
        eq_(len(queue), 0)

    def test_priority(self):
        queue = self._makeOne()
        queue.put(b"four", priority=101)
        queue.put(b"one", priority=0)
        queue.put(b"two", priority=0)
        queue.put(b"three", priority=10)

        eq_(queue.get(1), b"one")
        ok_(queue.consume())
        eq_(queue.get(1), b"two")
        ok_(queue.consume())
        eq_(queue.get(1), b"three")
        ok_(queue.consume())
        eq_(queue.get(1), b"four")
        ok_(queue.consume())

    def test_concurrent_execution(self):
        queue = self._makeOne()
        value1 = []
        value2 = []
        value3 = []
        
        def get_concurrently(value):
            q = self.client.LockingQueue(queue.path)
            value.append(q.get(.2))

        t1 = threading.Thread(target=get_concurrently, args=(value1,))
        t2 = threading.Thread(target=get_concurrently, args=(value2,))
        t3 = threading.Thread(target=get_concurrently, args=(value3,))
        t1.start()
        t2.start()
        t3.start()
        queue.put(b"one")
        t1.join()
        t2.join()
        t3.join()

        eq_(len(value1), 1)
        eq_(len(value2), 1)
        eq_(len(value3), 1)
        result = sorted(value1 + value2 + value3)
        eq_(result, [None, None, b"one"])