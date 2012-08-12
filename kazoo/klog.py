"""Kazoo Logging for Zookeeper

Zookeeper logging redirects that fashion the appropriate logging setup based
on the handler used for the :class:`~kazoo.client.KazooClient`.

"""
import fcntl
import os
import logging

import zookeeper

from kazoo.handlers.util import get_realthread

_logging_setup = False
_logging_pipe = os.pipe()
log = logging.getLogger('ZooKeeper').log

# Define global levels
LEVELS = dict(ZOO_INFO=logging.INFO,
              ZOO_WARN=logging.WARNING,
              ZOO_ERROR=logging.ERROR,
              ZOO_DEBUG=logging.DEBUG,
              )


def setup_logging(use_gevent=False):
    global _logging_setup

    if _logging_setup:
        return

    if use_gevent:
        import gevent
        fcntl.fcntl(_logging_pipe[0], fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(_logging_pipe[1], fcntl.F_SETFL, os.O_NONBLOCK)
        gevent.spawn(_logging_greenlet)
    else:
        thread = get_realthread()
        thread.start_new_thread(_logging_thread, ())
    zookeeper.set_log_stream(os.fdopen(_logging_pipe[1], 'w'))
    _logging_setup = True


def _process_message(line):
    """Line processor used by all loggers"""
    try:
        if '@' in line:
            level, message = line.split('@', 1)
            level = LEVELS.get(level.split(':')[-1])

            if 'Exceeded deadline by' in line and level == logging.WARNING:
                level = logging.DEBUG

        else:
            level = None

        if level is None:
            log(logging.INFO, line)
        else:
            log(level, message)
    except Exception as v:
        logging.getLogger('ZooKeeper').exception("Logging error: %s", v)


def _logging_greenlet():
    """Zookeeper logging redirect

    This greenlet based logger waits for the pipe to get data, then reads
    lines off it and processes them.

    Used for gevent.

    """
    from gevent.socket import wait_read
    r, w = _logging_pipe
    while 1:
        wait_read(r)
        data = []
        char = os.read(r, 1)
        while char != '\n':
            data.append(char)
            char = os.read(r, 1)
        line = ''.join(data).strip()
        if not line:
            continue
        _process_message(line)


def _logging_thread():
    """Zookeeper logging redirect

    Zookeeper by default logs directly out. This thread handles reading
    off the pipe that the above `set_log_stream` call designates so
    that the Zookeeper logging output can be turned into Python logging
    statements under the `Zookeeper` name.

    Used for threading.

    """
    r, w = _logging_pipe
    f = os.fdopen(r)
    while 1:
        line = f.readline().strip()

        # Skip empty lines
        if not line:
            continue
        _process_message(line)
