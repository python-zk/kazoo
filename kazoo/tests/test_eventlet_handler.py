import contextlib
import unittest

from nose import SkipTest
from nose.tools import raises

import mock

from kazoo.handlers import utils
from kazoo.protocol import states as kazoo_states

try:
    import eventlet
    from kazoo.handlers import eventlet as eventlet_handler
    EVENTLET_HANDLER_AVAILABLE = True
except ImportError:
    EVENTLET_HANDLER_AVAILABLE = False


@contextlib.contextmanager
def start_stop_one(handler=None):
    if not handler:
        handler = eventlet_handler.SequentialEventletHandler()
    handler.start()
    try:
        yield handler
    finally:
        handler.stop()


class TestEventletHandler(unittest.TestCase):
    def setUp(self):
        if not EVENTLET_HANDLER_AVAILABLE:
            raise SkipTest('eventlet handler not available.')

    def test_started(self):
        with start_stop_one() as handler:
            self.assertTrue(handler.running)
            self.assertNotEqual(0, len(handler._workers))
        self.assertFalse(handler.running)
        self.assertEqual(0, len(handler._workers))

    def test_spawn(self):
        captures = []

        def cb():
            captures.append(1)

        with start_stop_one() as handler:
            handler.spawn(cb)

        self.assertEqual(1, len(captures))

    def test_dispatch(self):
        captures = []

        def cb():
            captures.append(1)

        with start_stop_one() as handler:
            handler.dispatch_callback(kazoo_states.Callback('watch', cb, []))

        self.assertEqual(1, len(captures))

    def test_async_link(self):
        captures = []

        def cb(handler):
            captures.append(handler)

        with start_stop_one() as handler:
            r = handler.async_result()
            r.rawlink(cb)
            r.set(2)

        self.assertEqual(1, len(captures))
        self.assertEqual(2, r.get())

    def test_timeout_raising(self):
        handler = eventlet_handler.SequentialEventletHandler()

        @raises(handler.timeout_exception)
        def raise_it():
            raise handler.timeout_exception("This is a timeout")

        raise_it()

    def test_async_ok(self):
        captures = []

        def delayed():
            captures.append(1)
            return 1

        def after_delayed(handler):
            captures.append(handler)

        with start_stop_one() as handler:
            r = handler.async_result()
            r.rawlink(after_delayed)
            w = handler.spawn(utils.wrap(r)(delayed))
            w.wait()

        self.assertEqual(2, len(captures))
        self.assertEqual(1, captures[0])
        self.assertEqual(1, r.get())

    def test_get_with_no_block(self):
        handler = eventlet_handler.SequentialEventletHandler()

        @raises(handler.timeout_exception)
        def test_no_block(r):
            r.get(block=False)

        with start_stop_one(handler):
            r = handler.async_result()
            test_no_block(r)
            r.set(1)
            self.assertEqual(1, r.get())

    def test_async_exception(self):

        @raises(IOError)
        def check_exc(r):
            r.get()

        def broken():
            raise IOError("Failed")

        with start_stop_one() as handler:
            r = handler.async_result()
            w = handler.spawn(utils.wrap(r)(broken))
            w.wait()

        self.assertFalse(r.successful())
        check_exc(r)
