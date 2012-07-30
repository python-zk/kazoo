.. _implementation_details:

======================
Implementation Details
======================

Under the hood, the Python Zookeeper library is a small binding to the
Zookeeper C library. This library creates an OS thread used for Zookeeper
session events and watches. Because the watch events are run in the same thread
used for session events, it can be error-prone writing watch callbacks that
might need to query Zookeeper. The most noticeable error is when a watch
function uses a blocking Zookeeper query and the connection is lost. Since the
blocking query is running in the same OS thread used for session events the
client will be unable to recognize when it has reconnected as all session
events are blocked.

Handlers
========

Both the Kazoo handlers run 3 separate queues to help alleviate deadlock issues
and ensure consistent execution order regardless of environment. The
:class:`~kazoo.handlers.gevent.SequentialGeventHandler` runs a separate
greenlet for each queue that processes the callbacks queued in order. The
:class:`~kazoo.handlers.threading.SequentialThreadingHandler` runs a separate
thread for each queue that processes the callbacks queued in order (thus the
naming scheme which notes they are sequential in anticipation that there could
be handlers shipped in the future which don't make this guarantee).

Callbacks are queued by type, the 3 types being:

1. Session events (State changes, registered listener functions)
2. Watch events (Watch callbacks, DataWatch, and ChildrenWatch functions)
3. Completion callbacks (Functions chained to
   :class:`~kazoo.interfaces.IAsyncResult` objects)

This ensures that calls can be made to Zookeeper from any callback *except* a
state listener without worrying that critical session events will be blocked.

.. warning::

    Its important to remember that if you write code that blocks in one of
    these functions then no queued functions of that type will be executed
    until the code stops blocking. If your code might block, it should run
    itself in a separate greenlet/thread so that the other callbacks can
    run.
