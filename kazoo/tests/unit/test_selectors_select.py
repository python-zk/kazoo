from __future__ import annotations

"""
The official python select function test case copied from python source
 to test the selector_select function.
"""

import socket
import subprocess
import sys
from typing import Any, Protocol, cast

import pytest

from kazoo.handlers.utils import selector_select

select = selector_select


class HasFileNo(Protocol):
    def fileno(self) -> int: ...


pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="can't easily test on this system",
)


def test_error_conditions() -> None:
    class Nope:
        pass

    class Almost:
        def fileno(self) -> str:
            return "fileno"

    with pytest.raises(TypeError):
        select(1, 2, 3)  # type: ignore[call-overload]
    with pytest.raises(TypeError):
        select([Nope()], [], [])  # type: ignore[list-item]
    with pytest.raises(TypeError):
        select([Almost()], [], [])  # type: ignore[list-item]
    with pytest.raises(TypeError):
        select([], [], [], "not a number")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        select([], [], [], -1)


# Issue #12367: http://www.freebsd.org/cgi/query-pr.cgi?pr=kern/155606
@pytest.mark.skipif(
    sys.platform.startswith("freebsd"),
    reason="skip because of a FreeBSD bug: kern/155606",
)
def test_errno() -> None:
    with open(__file__, "rb") as fp:
        fd = fp.fileno()
    # fp is now closed
    with pytest.raises(ValueError):
        select([fd], [], [], 0)


def test_returned_list_identity() -> None:
    # See issue #8329
    r, w, x = select([], [], [], 1)
    assert r is not w
    assert r is not x
    assert w is not x


def test_select() -> None:
    cmd = "for i in 0 1 2 3 4 5 6 7 8 9; do echo testing...; sleep 1; done"
    with subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        assert process.stdout is not None
        for tout in (0, 1, 2, 4, 8, 16) + (None,) * 10:
            rfd, wfd, xfd = select(
                [cast("HasFileNo", process.stdout)], [], [], tout
            )
            if (rfd, wfd, xfd) == ([], [], []):
                continue
            if (rfd, wfd, xfd) == (
                [cast("HasFileNo", process.stdout)],
                [],
                [],
            ):
                line = process.stdout.readline()
                if not line:
                    break
                continue
            pytest.fail(
                f"Unexpected return values from select(): {rfd}, {wfd}, {xfd}"
            )


# Issue 16230: Crash on select resized list
def test_select_mutated() -> None:
    s1, s2 = socket.socketpair()
    try:
        a: list[Any] = []

        class F:
            def fileno(self) -> int:
                del a[-1]
                return s1.fileno()

        a[:] = [F()] * 10
        r, w, x = select([], a, [])

        # The list 'a' is mutated during the select call by F.fileno().
        # The original unittest asserted that the result of select() is
        # equal to ([], a[:5], []), where a[:5] is evaluated after 'a'
        # has been mutated (and has 5 items).
        assert (r, w, x) == ([], a[:5], [])
    finally:
        s1.close()
        s2.close()
