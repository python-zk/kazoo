from __future__ import annotations

import os
from sys import platform
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from kazoo.client import KazooClient


class TestKazooInterrupt:
    def test_interrupted_systemcall(self, zkclient: KazooClient) -> None:
        """
        Make sure interrupted system calls don't break the world, since we
        can't control what all signals our connection thread will get
        """
        if "linux" not in platform:
            pytest.skip(
                "Unable to reproduce error case on non-linux platforms"
            )

        path = "interrupt_test"
        value = b"1"
        zkclient.create(path, value)

        # set the euid to the current process' euid.
        # glibc sends SIGRT to all children, which will interrupt the
        # system call
        os.seteuid(os.geteuid())

        # basic sanity test that it worked alright
        assert zkclient.get(path)[0] == value
