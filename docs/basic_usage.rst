.. _basic_usage:

===========
Basic Usage
===========

Connection Handling
===================

To begin using Kazoo, a :class:`~kazoo.client.KazooClient` object must be
created and a connection established:

.. code-block:: python

    from kazoo.client import KazooClient

    zk = KazooClient()
    zk.connect()

By default, the client will connect to a local Zookeeper server on the default
port (2181). You should make sure Zookeeper is actually running there first,
or the ``connect`` command will be waiting until its default timeout.

Once connected, the client will attempt to stay connected regardless of
intermittent connection loss or Zookeeper session expiration. The client can be
instructed to drop a connection by calling `stop`:

.. code-block:: python

    zk.stop()

This command is named `stop` rather than `disconnect` because the client could
be already disconnected and attempting to reconnect when this is called.

Listening for Connection Events
-------------------------------

It can be useful to know when the connection has been dropped, restored, or
when the Zookeeper session has expired. To simplify this process Kazoo uses a
state system and lets you register listener functions to be called when the
state changes.

.. code-block:: python

    from kazoo.client import KazooState

    def my_listener(state):
        if state == KazooState.LOST:
            # Register somewhere that the session was lost
        elif state == KazooState.SUSPENDED
            # Handle being disconnected from Zookeeper
        else:
            # Handle being connected/reconnected to Zookeeper

    zk.add_listener(my_listener)

When using the :class:`kazoo.recipe.lock.Lock` or creating ephemeral nodes, its
highly recommended to add a state listener so that your program can properly
deal with connection interruptions or a Zookeeper session loss.

Zookeeper CRUD
==============

Zookeeper includes several functions for creating, reading, updating, and
deleting Zookeeper nodes (called znodes or nodes here). Kazoo adds several
convenience methods and a more Pythonic API.

Creating Nodes
--------------

Methods:

* :meth:`~kazoo.client.KazooClient.ensure_path`
* :meth:`~kazoo.client.KazooClient.create`

:meth:`~kazoo.client.KazooClient.ensure_path` will recursively create the node
and any nodes in the path necessary along the way, but can not set the data for
the node, only the ACL.

:meth:`~kazoo.client.KazooClient.create` creates a node and can set the data on
the node along with a watch function. It requires the path to it to exist
first, unless the `makepath` option is set to `True`.

.. code-block:: python

    # Ensure a path, create if necessary
    zk.ensure_path("/my/favorite")

    # Create a node with data
    zk.create("/my/favorite/node", "a value")

Reading Data
------------

Methods:

* :meth:`~kazoo.client.KazooClient.exists`
* :meth:`~kazoo.client.KazooClient.get`
* :meth:`~kazoo.client.KazooClient.get_children`

:meth:`~kazoo.client.KazooClient.exists` checks to see if a node exists.

:meth:`~kazoo.client.KazooClient.get` fetches the data of the node along with
detailed node information in a :class:`~kazoo.client.ZnodeStat` structure.

:meth:`~kazoo.client.KazooClient.get_children` gets a list of the children of
a given node.

.. code-block:: python

    # Determine if a node exists
    if zk.exists("/my/favorite"):
        # Do something

    # Print the version of a node and its data
    data, stat = zk.get("/my/favorite")
    print "Version is %s, data is %s" % (stat.version, data)

    # List the children
    children = zk.get_children("/my/favorite")
    print "There are %s children with names %s" % (len(children), children)

Updating Data
-------------

Methods:

* :meth:`~kazoo.client.KazooClient.set`

:meth:`~kazoo.client.KazooClient.set` updates the data for a given node. A
version for the node can be supplied, which will be required to match before
updating the data, or a :exc:`~kazoo.exceptions.BadVersionException` will be
raised instead of updating.

.. code-block:: python

    zk.set("/my/favorite", "some data")

Deleting Nodes
--------------

Methods:

* :meth:`~kazoo.client.KazooClient.delete`

:meth:`~kazoo.client.KazooClient.delete` deletes a node, and can optionally
recursively delete the entire path up to the node as well. A version can be
supplied when deleting a node which will be required to match the version of
the node before deleting it or a :exc:`~kazoo.exceptions.BadVersionException`
will be raised instead of deleting.

.. code-block:: python

    zk.delete("/my/favorite/node", recursive=True)

Retrying Commands
=================

Connections to Zookeeper may get interrupted if the Zookeeper server goes down
or becomes unreachable. By default, kazoo does not retry commands so these
failures will result in an exception being raised. To assist with failures
kazoo comes with a :meth:`~kazoo.client.KazooClient.retry` helper that will
retry a function should one of the Zookeeper connection exceptions get raised.

Example:

.. code-block:: python

    result = zk.retry(zk.get, "/path/to/node")

Some commands may have unique behavior that doesn't warrant automatic retries
on a per command basis. For example, if one creates a node a connection might
be lost before the command returns successfully but the node actually got
created. This results in a :exc:`kazoo.exceptions.NodeExistsException` being
raised when it runs again.

A similar unique situation arises when a node is created with ephemeral and
sequence options set, `documented here on the Zookeeper site <http://zookeeper.
apache.org/doc/trunk/recipes.html#sc_recipes_errorHandlingNote>`_. Since the
:meth:`~kazoo.client.KazooClient.retry` method takes a function to call and
its arguments, a function that runs multiple Zookeeper commands could be
passed to it so that the entire function will be retried if the connection is
lost.

This snippet from the lock implementation shows how it uses retry to re-run the
function acquiring a lock, and checks to see if it was already created to
handle this condition:

.. code-block:: python

    # kazoo.recipe.lock snippet

    def acquire(self):
        """Acquire the mutex, blocking until it is obtained"""
        try:
            self.client.retry(self._inner_acquire)
            self.is_acquired = True
        except Exception:
            # if we did ultimately fail, attempt to clean up
            self._best_effort_cleanup()
            self.cancelled = False
            raise

    def _inner_acquire(self):
        self.wake_event.clear()

        # make sure our election parent node exists
        if not self.assured_path:
            self.client.ensure_path(self.path)

        node = None
        if self.create_tried:
            node = self._find_node()
        else:
            self.create_tried = True

        if not node:
            node = self.client.create(self.create_path, self.data,
                ephemeral=True, sequence=True)
            # strip off path to node
            node = node[len(self.path) + 1:]

`create_tried` records whether it has tried to create the node already in the
event the connection is lost before the node name is returned.

Watchers
========

Kazoo can set watch functions on a node that can be triggered either when the
node has changed or when the children of the node change. This change to the
node or children can also be the node or its children being deleted.

Watchers can be set in two different ways, the first is the style that
Zookeeper supports by default for one-time watch events. These watch functions
will be called once by kazoo, and do *not* receive session events, unlike the
native Zookeeper watches. Using this style requires the watch function to be
passed to one of these methods:

* :meth:`~kazoo.client.KazooClient.get`
* :meth:`~kazoo.client.KazooClient.get_children`
* :meth:`~kazoo.client.KazooClient.exists`

A watch function passed to :meth:`~kazoo.client.KazooClient.get` or
:meth:`~kazoo.client.KazooClient.exists` will be called when the data on the
node changes or the node itself is deleted. It will be passed a
:class:`~kazoo.client.WatchedEvent` instance.

.. code-block:: python

    def my_func(event):
        # check to see what the children are now

    # Call my_func when the children change
    children = zk.get_children("/my/favorite/node", watch=my_func)

Kazoo includes a higher level API that watches for data and children
modifications that's easier to use as it doesn't require re-setting the watch
every time the event is triggered. It also passes in the data and
:class:`~kazoo.client.ZnodeStat` when watching a node or the list of children
when watching a nodes children. Watch functions registered with this API will
be called immediately and every time there's a change, or until the function
returns False. If `allow_session_lost` is set to `True`, then the function will
no longer be called if the session is lost.

The following methods provide this functionality:

* :class:`~kazoo.recipe.watchers.ChildrenWatch`
* :class:`~kazoo.recipe.watchers.DataWatch`

These classes are available directly on the :class:`~kazoo.client.KazooClient`
instance and don't require the client object to be passed in when used in this
manner. The instance returned by instantiating either of the classes can be
called directly allowing them to be used as decorators:

.. code-block:: python

    @zk.ChildrenWatch("/my/favorite/node")
    def watch_children(children):
        print "Children are now: %s" % children
    # Above function called immediately, and from then on

    @zk.DataWatch("/my/favorite")
    def watch_node(data, stat):
        print "Version is %s, data is %s" % (stat.version, data)
