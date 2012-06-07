Changelog
=========

0.2 (unreleased)
----------------

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

- State transition handling with listener registering to handle session state
  changes (choose to fatal the app on session expiration, etc.)
- Zookeeper logging stream redirected into Python logging channel under the
  name 'Zookeeper'.
- Base client library with handler support for threading and gevent async
  environments.
