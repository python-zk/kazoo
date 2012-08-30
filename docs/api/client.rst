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

        .. attribute:: retry

        An instance of :class:`~kazoo.retry.KazooRetry` acting as a wrapper
        to retry other methods, See :ref:`retrying_commands`.
