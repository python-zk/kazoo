from __future__ import annotations

##############################################################################
#
# Copyright Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import logging
import os
import time

from typing import Any, Callable, Type

CI = os.environ.get("CI", False)
CI_ZK_VERSION: tuple[int, ...] = tuple()
if CI:
    has_version = os.environ.get("ZOOKEEPER_VERSION", "")
    if has_version:
        if "-" in has_version:
            # Ignore pre-release markers like -alpha
            has_version = has_version.split("-")[0]
    CI_ZK_VERSION = tuple([int(n) for n in has_version.split(".")])


class Handler(logging.Handler):
    def __init__(self, *names: Any, **kw: Any):
        logging.Handler.__init__(self)
        self.names = names
        self.records: list[Any] = []
        self.setLoggerLevel(**kw)

    def setLoggerLevel(self, level: int = 1) -> None:
        self.level = level
        self.oldlevels: dict[str, int] = {}

    def emit(self, record: Any) -> None:
        self.records.append(record)

    def clear(self) -> None:
        del self.records[:]

    def install(self) -> None:
        for name in self.names:
            logger = logging.getLogger(name)
            self.oldlevels[name] = logger.level
            logger.setLevel(self.level)
            logger.addHandler(self)

    def uninstall(self) -> None:
        for name in self.names:
            logger = logging.getLogger(name)
            logger.setLevel(self.oldlevels[name])
            logger.removeHandler(self)

    def __str__(self) -> str:
        return "\n".join(
            [
                (
                    "%s %s\n  %s"
                    % (
                        record.name,
                        record.levelname,
                        "\n".join(
                            [
                                line
                                for line in record.getMessage().split("\n")
                                if line.strip()
                            ]
                        ),
                    )
                )
                for record in self.records
            ]
        )


class InstalledHandler(Handler):
    def __init__(self, *names: Any, **kw: Any):
        Handler.__init__(self, *names, **kw)
        self.install()


class Wait(object):
    class TimeOutWaitingFor(Exception):
        "A test condition timed out"

    timeout = 9
    wait = 0.01

    def __init__(
        self,
        timeout: int | None = None,
        wait: float | None = None,
        exception: Type[Exception] = TimeOutWaitingFor,
        getnow: Callable[[], Callable[[], float]] = (lambda: time.monotonic),
        getsleep: Callable[[], Callable[[float], None]] = (lambda: time.sleep),
    ):
        if timeout is not None:
            self.timeout = timeout

        if wait is not None:
            self.wait = wait

        self.exception = exception

        self.getnow = getnow
        self.getsleep = getsleep

    def __call__(
        self,
        func: Callable[[], Any],  # | None = None,
        timeout: float | None = None,
        wait: float | None = None,
        message: str | None = None,
    ) -> None:
        # if func is None:
        #    # Seriously WTF?
        #    return lambda func: self(func, timeout, wait, message)

        if func():
            return

        now = self.getnow()
        sleep = self.getsleep()
        if timeout is None:
            timeout = self.timeout
        if wait is None:
            wait = self.wait
        wait = float(wait)

        deadline = now() + timeout
        while 1:
            sleep(wait)
            if func():
                return
            if now() > deadline:
                # raise self.TimeOutWaitingFor(
                raise self.exception(message or func.__doc__ or func.__name__)


wait = Wait()
