.. _retry_module:

:mod:`kazoo.retry`
----------------------------

.. automodule:: kazoo.retry

Public API
++++++++++

    .. autoclass:: RetrySleeper
        :members:
        :member-order: bysource

        .. automethod:: __init__

    .. autoclass:: KazooRetry
        :members:
        :member-order: bysource

        .. automethod:: __init__

        .. automethod:: __call__

    .. autoexception:: ForceRetryError

    .. autoexception:: RetryFailedError
