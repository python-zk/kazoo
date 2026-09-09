from __future__ import annotations

import functools
import sys
from typing import Any, Type, TYPE_CHECKING

import pytest

from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import Callback, ZnodeStat
from kazoo.tests.integ import test_client

if TYPE_CHECKING:
    from kazoo.client import KazooClient
    from kazoo.handlers.gevent import AsyncResult, SequentialGeventHandler
    from gevent.event import Event


def _require_gevent() -> None:
    try:
        import gevent  # noqa: F401
    except ImportError:
        pytest.skip("gevent not available.")


def _make_gevent_handler() -> Any:
    from kazoo.handlers.gevent import SequentialGeventHandler

    return SequentialGeventHandler()


# The zkclient fixture is shadowed for this module so every test (including
# those inherited from kazoo.tests.integ.test_client.TestClient) talks to the
# ensemble through a gevent-handler client (handler-specific).
@pytest.fixture
def zkclient(zkensemble: Any, zkchroot: str) -> Any:
    # Guard against fixture-ordering: inherited autouse set-up fixtures (e.g.
    # TestKazooLock._setup) pull in ``zkclient`` before the class-level
    # `_skip_without_gevent` autouse fixture runs.
    _require_gevent()
    client = zkensemble.get_client(handler=_make_gevent_handler())
    client.harness_expire_session = functools.partial(
        zkensemble.expire_session,
        client=client,
        event_factory=client.handler.event_object,
    )
    client.start()
    client.ensure_path(zkchroot)
    client.chroot = zkchroot
    yield client
    client.stop()
    client.close()


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestGeventHandler:
    @pytest.fixture(autouse=True)
    def _skip_without_gevent(self) -> None:
        _require_gevent()

    def _makeOne(self, *args: Any) -> Any:
        return _make_gevent_handler()

    def _getAsync(self) -> Type[Any]:
        from kazoo.handlers.gevent import AsyncResult

        return AsyncResult

    def _getEvent(self) -> Type[Any]:
        from gevent.event import Event

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

    def test_exception_raising(self) -> None:
        h = self._makeOne()

        with pytest.raises(h.timeout_exception):
            raise h.timeout_exception("This is a timeout")

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
        from gevent.queue import Empty

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
class TestBasicGeventClient:
    @pytest.fixture(autouse=True)
    def _skip_without_gevent(self) -> None:
        _require_gevent()

    def test_start(self, zkclient: KazooClient) -> None:
        client = zkclient
        client.start()
        assert client.state == "CONNECTED"
        client.stop()

    def test_start_stop_double(self, zkclient: KazooClient) -> None:
        client = zkclient
        client.start()
        assert client.state == "CONNECTED"
        client.handler.start()
        client.handler.stop()
        client.stop()

    def test_basic_commands(self, zkclient: KazooClient) -> None:
        client = zkclient
        client.start()
        assert client.state == "CONNECTED"
        client.create("/anode", b"fred")
        assert client.get("/anode")[0] == b"fred"
        assert client.delete("/anode")
        assert client.exists("/anode") is None
        client.stop()

    def test_failures(self, zkclient: KazooClient) -> None:
        client = zkclient
        client.start()
        with pytest.raises(NoNodeError):
            client.get("/none")
        client.stop()

    def test_data_watcher(self, zkclient: KazooClient) -> None:
        client = zkclient
        client.start()
        client.ensure_path("/some/node")
        from gevent.event import Event

        ev = Event()

        @client.DataWatch("/some/node")
        def changed(d: bytes | None, stat: ZnodeStat | None) -> bool | None:
            ev.set()
            return None

        ev.wait()
        ev.clear()
        client.set("/some/node", b"newvalue")
        ev.wait()
        client.stop()


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestGeventClient(test_client.TestClient):
    @pytest.fixture(autouse=True)
    def _skip_without_gevent(self) -> None:
        _require_gevent()

    def _makeOne(self, *args: Any) -> Any:
        return _make_gevent_handler()
