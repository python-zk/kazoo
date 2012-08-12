Changelog
=========

0.2 (8/12/2012)
---------------

Documentation
*************

- Fixed doc references to start_async using an AsyncResult object, it uses
  an Event object.

Bug Handling
************

- Issue #16 fixed: gevent zookeeper logging failed to handle a monkey patched
  logging setup. Logging is now setup such that a greenlet is used for logging
  messages under gevent, and the thread one is used otherwise.
- Fixed bug similar to #14 for ChildrenWatch on the session listener.
- Issue #14 fixed: DataWatch had inconsistent handling of the node it was
  watching not existing. DataWatch also properly spawns its _get_data function
  to avoid blocking session events.
- Issue #15 fixed: sleep_func for SequentialGeventHandler was not set on the
  class appropriately leading to additional arguments being passed to
  gevent.sleep.
- Issue #9 fixed: Threads/greenlets didn't gracefully shut down. Handler now
  has a start/stop that is used by the client when calling start and stop that
  shuts down the handler workers. This addresses errors and warnings that could
  be emitted upon process shutdown regarding a clean exit of the workers.
- Issue #12 fixed: gevent 0.13 doesn't use the same start_new_thread as gevent
  1.0 which resulted in a fully monkey-patched environment halting due to the
  wrong thread. Updated to use the older kazoo method of getting the real thread
  module object.

API Changes
***********

- The KazooClient handler is now officially exposed as KazooClient.handler
  so that the appropriate sync objects can be used by end-users.
- Refactored ChildrenWatcher used by SetPartitioner into a publicly exposed
  PatientChildrenWatch under recipe.watchers.

Deprecations
************

- connect/connect_async has been renamed to start/start_async to better match
  the stop to indicate connection handling. The prior names are aliased for
  the time being.

Recipes
*******

- Added Barrier and DoubleBarrier implementation.

0.2b1 (7/27/2012)
-----------------

Bug Handling
************

- ZOOKEEPER-1318: SystemError is caught and rethrown as the proper invalid
  state exception in older zookeeper python bindings where this issue is still
  valid.
- ZOOKEEPER-1431: Install the latest zc-zookeeper-static library or use the
  packaged ubuntu one for ubuntu 12.04 or later.
- ZOOKEEPER-553: State handling isn't checked via this method, we track it in
  a simpler manner with the watcher to ensure we know the right state.

Features
********

- Exponential backoff with jitter for retrying commands.
- Gevent 0.13 and 1.0b support.
- Lock, Party, SetPartitioner, and Election recipe implementations.
- Data and Children watching API's.
- State transition handling with listener registering to handle session state
  changes (choose to fatal the app on session expiration, etc.)
- Zookeeper logging stream redirected into Python logging channel under the
  name 'Zookeeper'.
- Base client library with handler support for threading and gevent async
  environments.
