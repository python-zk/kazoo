from __future__ import annotations

import contextlib
import unittest

from typing import Generator, Literal

import pytest

from kazoo.handlers.utils import create_tcp_socket
from kazoo.handlers import utils
from kazoo.protocol import states as kazoo_states

try:
    from eventlet.green import socket
    from kazoo.handlers import eventlet as eventlet_handler
    from kazoo.handlers.eventlet import SequentialEventletHandler
except ImportError:
    pytestmark = pytest.mark.skip(reason="eventlet not available")


@contextlib.contextmanager
def start_stop_one(
    handler: SequentialEventletHandler = None,  # type: ignore[assignment]
) -> Generator[SequentialEventletHandler]:
    if not handler:
        handler = eventlet_handler.SequentialEventletHandler()
    handler.start()
    try:
        yield handler
    finally:
        handler.stop()


class TestEventletHandler(unittest.TestCase):
    def test_started(self) -> None:
        with start_stop_one() as handler:
            assert handler.running is True
            assert len(handler._workers) != 0
        assert handler.running is False
        assert len(handler._workers) == 0  # type: ignore[unreachable]

    def test_spawn(self) -> None:
        captures = []

        def cb() -> None:
            captures.append(1)

        with start_stop_one() as handler:
            handler.spawn(cb)

        assert len(captures) == 1

    def test_dispatch(self) -> None:
        captures = []

        def cb() -> None:
            captures.append(1)

        with start_stop_one() as handler:
            handler.dispatch_callback(kazoo_states.Callback("watch", cb, []))

        assert len(captures) == 1

    def test_async_link(self) -> None:
        captures: list[SequentialEventletHandler] = []

        def cb(handler: SequentialEventletHandler) -> None:
            captures.append(handler)

        with start_stop_one() as handler:
            r = handler.async_result()
            r.rawlink(cb)
            r.set(2)

        assert len(captures) == 1
        assert r.get() == 2

    def test_timeout_raising(self) -> None:
        handler = eventlet_handler.SequentialEventletHandler()

        with pytest.raises(handler.timeout_exception):
            raise handler.timeout_exception("This is a timeout")

    def test_async_ok(self) -> None:
        captures: list[Literal[1] | SequentialEventletHandler] = []

        def delayed() -> Literal[1]:
            captures.append(1)
            return 1

        def after_delayed(handler: SequentialEventletHandler) -> None:
            captures.append(handler)

        with start_stop_one() as handler:
            r = handler.async_result()
            r.rawlink(after_delayed)
            w = handler.spawn(utils.wrap(r)(delayed))
            w.join()

        assert len(captures) == 2
        assert captures[0] == 1
        assert r.get() == 1

    def test_get_with_no_block(self) -> None:
        handler = eventlet_handler.SequentialEventletHandler()

        with start_stop_one(handler):
            r = handler.async_result()

            with pytest.raises(handler.timeout_exception):
                r.get(block=False)
            r.set(1)
            assert r.get() == 1

    def test_async_exception(self) -> None:
        def broken() -> None:
            raise IOError("Failed")

        with start_stop_one() as handler:
            r = handler.async_result()
            w = handler.spawn(utils.wrap(r)(broken))
            w.join()

        assert r.successful() is False
        with pytest.raises(IOError):
            r.get()

    def test_huge_file_descriptor(self) -> None:
        try:
            import resource
        except ImportError:
            self.skipTest("resource module unavailable on this platform")

        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (4096, 4096))
        except (ValueError, resource.error):
            self.skipTest("couldn't raise fd limit high enough")
        fd = 0
        socks = []
        while fd < 4000:
            sock = create_tcp_socket(socket)
            fd = sock.fileno()
            socks.append(sock)
        with start_stop_one() as h:
            h.start()
            h.select(socks, [], [], 0)
            h.stop()
        for sock in socks:
            sock.close()
