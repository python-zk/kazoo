from __future__ import annotations

import importlib.util
import unittest
import sys

from typing import Any, Type
import pytest

from kazoo.exceptions import NoNodeError
from kazoo.handlers.utils import create_tcp_socket
from kazoo.interfaces import IAsyncResult
from kazoo.protocol.states import Callback, KazooState, ZnodeStat
from kazoo.testing import KazooTestCase
from kazoo.testing.async_result_common import (
    AsyncResultCommon,
    ThreadlikeContext,
)

if importlib.util.find_spec("gevent") is None:
    # The double test here stops complaints about imports not being at the top
    # of the file.
    pytest.importorskip("gevent", reason="gevent not available")
else:
    import gevent
    from gevent.event import Event
    from gevent.queue import Empty
    from gevent import socket
    from kazoo.handlers.gevent import AsyncResult, SequentialGeventHandler


class GeventContext(ThreadlikeContext):
    def __enter__(self) -> None:
        # Spawn the geventlet and then immediately sleep to hand over control
        gevent.spawn(self._target)
        gevent.sleep(0)


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestGeventAsyncResult(AsyncResultCommon, unittest.TestCase):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(
            SequentialGeventHandler,
            lambda handler: AsyncResult(),
            thread_context_factory=GeventContext,
            *args,
            **kwargs,
        )

    # We override this because the wait behaviour for AsyncResult is different
    # for gevent and everything else even though IASyncResult is meant to be
    # based on the gevent AsyncResult
    def test_wait(self, *, is_green: bool = False) -> None:
        super().test_wait(is_green=True)


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestGeventHandler(unittest.TestCase):
    _makeOne = SequentialGeventHandler

    def _getAsync(self) -> Type[IAsyncResult]:
        return AsyncResult

    def _getEvent(self) -> Type[Event]:
        return Event

    def test_proper_threading(self) -> None:
        h = self._makeOne()
        h.start()
        assert isinstance(h.event_object(), self._getEvent())

    def test_matching_async(self) -> None:
        h = self._makeOne()
        h.start()
        async_handler = self._getAsync()
        assert isinstance(h.async_result(), async_handler)

    def test_exception_in_queue(self) -> None:
        h = self._makeOne()
        h.start()
        ev = self._getEvent()()

        def func() -> None:
            ev.set()
            raise ValueError("bang")

        call1 = Callback("completion", func, ())
        h.dispatch_callback(call1)
        ev.wait()

    def test_queue_empty_exception(self) -> None:
        h = self._makeOne()
        h.start()
        ev = self._getEvent()()

        def func() -> None:
            ev.set()
            raise Empty()

        call1 = Callback("completion", func, ())
        h.dispatch_callback(call1)
        ev.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestBasicGeventClient(KazooTestCase):
    _makeOne = SequentialGeventHandler

    def setUp(self) -> None:
        KazooTestCase.setUp(self)

    def _getEvent(self) -> Type[Event]:
        return Event

    def test_start(self) -> None:
        client = self._get_client(handler=self._makeOne())
        client.start()
        assert client.state == KazooState.CONNECTED
        client.stop()

    def test_start_stop_double(self) -> None:
        client = self._get_client(handler=self._makeOne())
        client.start()
        assert client.state == KazooState.CONNECTED
        client.handler.start()
        client.handler.stop()
        client.stop()

    def test_basic_commands(self) -> None:
        client = self._get_client(handler=self._makeOne())
        client.start()
        assert client.state == KazooState.CONNECTED
        client.create("/anode", b"fred")
        assert client.get("/anode")[0] == b"fred"
        assert client.delete("/anode")
        assert client.exists("/anode") is None
        client.stop()

    def test_failures(self) -> None:
        client = self._get_client(handler=self._makeOne())
        client.start()
        with pytest.raises(NoNodeError):
            client.get("/none")
        client.stop()

    def test_data_watcher(self) -> None:
        client = self._get_client(handler=self._makeOne())
        client.start()
        client.ensure_path("/some/node")
        ev = self._getEvent()()

        @client.DataWatch("/some/node")
        def changed(d: bytes | None, stat: ZnodeStat | None) -> bool | None:
            ev.set()
            return None

        ev.wait()
        ev.clear()
        client.set("/some/node", b"newvalue")
        ev.wait()
        client.stop()

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
        h = self._makeOne()
        h.start()
        h.select(socks, [], [], 0)
        h.stop()
        for sock in socks:
            sock.close()
