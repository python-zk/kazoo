.. _async_usage:

==================
Asynchronous Usage
==================

The asynchronous Kazoo API relies on the
:class:`~kazoo.interfaces.IAsyncResult` object which is returned by all the
asynchronous methods. Callbacks can be added with the
:meth:`~kazoo.interfaces.IAsyncResult.rawlink` method which works in a
consistent manner whether threads or an asynchronous framework like gevent is
used.

Kazoo utilizes a pluggable :class:`~kazoo.interfaces.IHandler` interface which
abstracts the callback system to ensure it works consistently.

Connection Handling
===================

Creating a connection:

.. code-block:: python

    from kazoo.client import KazooClient
    from kazoo.handlers.gevent import SequentialGeventHandler

    zk = KazooClient(handler=SequentialGeventHandler)

    # returns immediately
    async_obj = zk.connect_async()

    # Wait for the connection to be established, this will throw
    # an exception if there's one during the connection attempt
    async_obj.get()

In this example, the :meth:`~kazoo.interfaces.IAsyncResult.get` method is used
to wait and get the value which is set when a connection is established. In the
event the connection has failed than an exception is raised. The
:class:`~kazoo.handlers.gevent.SequentialGeventHandler` is used when you want
to use gevent, kazoo doesn't rely on gevents monkey patching and requires that
you pass in the appropriate handler.

Chaining a connection callback:

.. code-block:: python

    import sys

    from kazoo.exceptions import ConnectionLossException
    from kazoo.exceptions import NoAuthException

    def my_handler(async_obj):
        try:
            async_obj.get()
            do_something()
        except (ConnectionLossException, NoAuthException):
            sys.exit(1)

    # Both of these statements return immediately
    async_obj = zk.connect_async()
    async_obj.rawlink(my_handler)

For the :meth:`~kazoo.client.KazooClient.connect_async` method, the resulting
value when the connection succeeds isn't relevant. For other asynchronous
methods, the value returned will be identical to what would've been returned
with the synchronous version.

Zookeeper CRUD
==============

The following CRUD methods all work the same as their synchronous counterparts
except that they return an :class:`~kazoo.interfaces.IAsyncResult` object.

Creating Method:

* :meth:`~kazoo.client.KazooClient.create_async`

Reading Methods:

* :meth:`~kazoo.client.KazooClient.exists_async`
* :meth:`~kazoo.client.KazooClient.get_async`
* :meth:`~kazoo.client.KazooClient.get_children_async`

Updating Methods:

* :meth:`~kazoo.client.KazooClient.set_async`

Deleting Methods:

* :meth:`~kazoo.client.KazooClient.delete_async`

The :meth:`~kazoo.client.KazooClient.ensure_path` has no asynchronous
counterpart at the moment nor can the
:meth:`~kazoo.client.KazooClient.delete_async` method do recursive deletes.
