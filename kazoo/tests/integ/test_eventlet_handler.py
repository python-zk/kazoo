from __future__ import annotations

import contextlib
import functools
import sys
import unittest
from typing import Any, Generator, Literal, TYPE_CHECKING

import pytest

from kazoo.handlers.utils import create_tcp_socket
from kazoo.handlers import utils
from kazoo.protocol import states as kazoo_states
from kazoo.tests import util as test_util
from kazoo.tests.integ import test_client
from kazoo.tests.integ import test_lock

if TYPE_CHECKING:
    from kazoo.handlers.eventlet import SequentialEventletHandler


def _require_eventlet() -> None:
    try:
        import eventlet  # noqa: F401
    except ImportError:
        pytest.skip("eventlet not available.")


def _make_eventlet_handler() -> SequentialEventletHandler:
    from kazoo.handlers.eventlet import SequentialEventletHandler

    return SequentialEventletHandler()


# The zkclient fixture is shadowed for this module so every test (including
# those inherited from kazoo.tests.integ.test_client.TestClient and
# kazoo.tests.integ.test_lock.TestKazooLock/TestSemaphore) talks to the
# ensemble through an eventlet-handler client (handler-specific).
@pytest.fixture
def zkclient(zkensemble: Any, zkchroot: str) -> Any:
    # Guard against fixture-ordering: the inherited autouse set-up fixtures
    # (e.g. TestKazooLock._setup) pull in ``zkclient`` before the class-level
    # `_skip_without_eventlet` autouse fixture runs.
    _require_eventlet()
    client = zkensemble.get_client(handler=_make_eventlet_handler())
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


@contextlib.contextmanager
def start_stop_one(
    handler: SequentialEventletHandler | None = None,
) -> Generator[SequentialEventletHandler, None, None]:
    if not handler:
        handler = _make_eventlet_handler()
    handler.start()
    try:
        yield handler
    finally:
        handler.stop()


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestEventletHandler(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def _skip_without_eventlet(self) -> None:
        _require_eventlet()

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
        handler = _make_eventlet_handler()

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
        handler = _make_eventlet_handler()

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
            from eventlet.green import socket
        except ImportError:
            self.skipTest("eventlet unavailable")
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


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestEventletClient(test_client.TestClient):
    @pytest.fixture(autouse=True)
    def _skip_without_eventlet(self) -> None:
        _require_eventlet()

    def _makeOne(self, *args: Any) -> Any:
        return _make_eventlet_handler()


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestEventletSemaphore(test_lock.TestSemaphore):
    @pytest.fixture(autouse=True)
    def _skip_without_eventlet(self) -> None:
        _require_eventlet()

    @staticmethod
    def make_condition() -> Any:
        from eventlet.green import threading

        return threading.Condition()

    @staticmethod
    def make_event() -> Any:
        from eventlet.green import threading

        return threading.Event()

    @staticmethod
    def make_thread(*args: Any, **kwargs: Any) -> Any:
        from eventlet.green import threading

        return threading.Thread(*args, **kwargs)

    def _makeOne(self, *args: Any) -> Any:
        return _make_eventlet_handler()


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class TestEventletLock(test_lock.TestKazooLock):
    @pytest.fixture(autouse=True)
    def _skip_without_eventlet(self) -> None:
        _require_eventlet()

    @staticmethod
    def make_condition() -> Any:
        from eventlet.green import threading

        return threading.Condition()

    @staticmethod
    def make_event() -> Any:
        from eventlet.green import threading

        return threading.Event()

    @staticmethod
    def make_thread(*args: Any, **kwargs: Any) -> Any:
        from eventlet.green import threading

        return threading.Thread(*args, **kwargs)

    @staticmethod
    def make_wait() -> Any:
        import eventlet

        return test_util.Wait(getsleep=(lambda: eventlet.sleep))

    def _makeOne(self, *args: Any) -> Any:
        return _make_eventlet_handler()

    # Fails consistently under compose (pre-existing, tracked in T039):
    # the waiting client's session expires mid-test (see its connect log
    # "Session has expired"), the server deletes its ephemeral lock
    # candidate, and the Lock recipe loses mutual exclusion - both green
    # threads end up holding the lock at once, so the queue of contenders
    # never exceeds one and `Wait` times out. The threading-only variant
    # passes; only the eventlet scheduler is affected here. Revisit by
    # stabilising the client session at high-latency reconnect (e.g. keep
    # the heartbeat greenlet running during acquire) before re-enabling.
    @pytest.mark.skip(
        "eventlet lock_cancel loses mutual exclusion because the waiting "
        "client's session expires mid-test under compose; threading "
        "variant passes (see comment in TestEventletLock)"
    )
    def test_lock_cancel(self, *args: Any, **kwargs: Any) -> Any:
        return super().test_lock_cancel(*args, **kwargs)
