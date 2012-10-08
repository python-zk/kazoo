Changelog
=========

0.7 (unreleased)
----------------

Features
********

- DataWatch now has a `allow_missing_node` setting that allows a watch to be
  set on a node that doesn't exist when the DataWatch is created.
- Add new Queue recipe, with optional priority support.
- Add new Counter recipe.

Bug Handling
************

- Issue #15 fixed again: Force sleep delay to be a float to appease gevent.
- Issue #29 fixed: DataWatch and ChildrenWatch properly re-register their
  watches on server disconnect.

0.6 (2012-09-27)
----------------

API Changes
***********

- Node paths are assumed to be Unicode objects. Under Python 2 pure-ascii
  strings will also be accepted. Node values are considered bytes. The byte
  type is an alias for `str` under Python 2.
- New KeeperState.CONNECTED_RO state for Zookeeper servers connected in
  read-only mode.
- New NotReadOnlyCallError exception when issuing a write change against a
  server thats currently read-only.

Features
********

- Add support for Python 3.2, 3.3 and PyPy (only for the threading handler).
- Handles connecting to Zookeeper 3.4+ read-only servers.
- Automatic background scanning for a Read/Write server when connected to a
  server in read-only mode.
- Add new Semaphore recipe.
- Add a new `retry_max_delay` argument to the client and by default limit the
  retry delay to at most an hour regardless of exponential backoff settings.
- Add new `randomize_hosts` argument to `KazooClient`, allowing one to disable
  host randomization.

Bug Handling
************

- Fix bug with locks not handling intermediary lock contenders disappearing.
- Fix bug with set_data type check failing to catch unicode values.
- Fix bug with gevent 0.13.x backport of peekable queue.
- Fix PatientChildrenWatch to use handler specific sleep function.

0.5 (2012-09-06)
----------------

Skipping a version to reflect the magnitude of the change. Kazoo is now a pure
Python client with no C bindings. This release should run without a problem
on alternate Python implementations such as PyPy and Jython. Porting to Python
3 in the future should also be much easier.

Documentation
*************

- Docs have been restructured to handle the new classes and locations of the
  methods from the pure Python refactor.

Bug Handling
************

This change may introduce new bugs, however there is no longer the possibility
of a complete Python segfault due to errors in the C library and/or the C
binding.

- Possible segfaults from the C lib are gone.
- Password mangling due to the C lib is gone.
- The party recipes didn't set their participating flag to False after
  leaving.

Features
********

- New `client.command` and `client.server_version` API, exposing Zookeeper's
  four letter commands and giving access to structured version information.
- Added 'include_data' option for get_children to include the node's Stat
  object.
- Substantial increase in logging data with debug mode. All correspondence with
  the Zookeeper server can now be seen to help in debugging.

API Changes
***********

- The testing helpers have been moved from `testing.__init__` into a
  `testing.harness` module. The official API's of `KazooTestCase` and
  `KazooTestHarness` can still be directly imported from `testing`.
- The kazoo.handlers.util module was removed.
- Backwards compatible exception class aliases are provided for now in kazoo
  exceptions for the prior C exception names.
- Unicode strings now work fine for node names and are properly converted to
  and from unicode objects.
- The data value argument for the create and create_async methods of the
  client was made optional and defaults to an empty byte string. The data
  value must be a byte string. Unicode values are no longer allowed and
  will raise a TypeError.


0.3 (2012-08-23)
----------------

API Changes
***********

- Handler interface now has an rlock_object for use by recipes.

Bug Handling
************

- Fixed password bug with updated zc-zookeeper-static release, which retains
  null bytes in the password properly.
- Fixed reconnect hammering, so that the reconnection follows retry jitter and
  retry backoff's.
- Fixed possible bug with using a threading.Condition in the set partitioner.
  Set partitioner uses new rlock_object handler API to get an appropriate RLock
  for gevent.
- Issue #17 fixed: Wrap timeout exceptions with staticmethod so they can be
  used directly as intended. Patch by Bob Van Zant.
- Fixed bug with client reconnection looping indefinitely using an expired
  session id.

0.2 (2012-08-12)
----------------

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

0.2b1 (2012-07-27)
------------------

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
