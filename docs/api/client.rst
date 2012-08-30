.. _client_module:

:mod:`kazoo.client`
----------------------------

.. automodule:: kazoo.client

Public API
++++++++++

    .. autoclass:: KazooClient()
        :members:
        :member-order: bysource

        .. automethod:: __init__

        .. attribute:: handler

        The :class:`~kazoo.interfaces.IHandler` strategy used by this client.
        Gives access to appropriate synchronization objects.

        .. attribute:: retry

        An instance of :class:`~kazoo.retry.KazooRetry` acting as a wrapper
        to retry other methods, See :ref:`retrying_commands`.
