.. _testing:

=======
Testing
=======

Kazoo has several test harnesses used internally for its own tests that are
exposed as public API's for use in your own tests for common Zookeeper cluster
management and session testing. They can be mixed in with your own `unittest`
or `nose` tests along with a `mock` object that allows you to force specific
`KazooClient` command to fail in various ways.

Kazoo Test Harness
==================

The :class:`~kazoo.testing.KazooTestHarness` can be used directly or mixed in
with your test code.

Example:

.. code-block:: python

    import unittest

    from kazoo.testing import KazooTestHarness

    class MyTest(unittest.TestCase, KazooTestHarness):
        def setUp(self):
            self.setup_zookeeper()

        def tearDown(self):
            self.teardown_zookeeper()

        def testmycode(self):
            self.client.ensure_path('/test/path')
            result = self.client.get('/test/path')
            ...


Kazoo Test Case
===============

The :class:`~kazoo.testing.KazooTestCase` is complete test case that is
equivalent to the mixin setup of :class:`~kazoo.testing.KazooTestHarness`. An
equivalent test to the one above:

.. code-block:: python

    from kazoo.testing import KazooTestCase

    class MyTest(KazooTestCase):
        def testmycode(self):
            self.client.ensure_path('/test/path')
            result = self.client.get('/test/path')
            ...

Faking Zookeeper Results
========================

It can be useful to simulate errors or a connection loss when running test code
to ensure that your program functions in a robust manner. Kazoo provides a
:meth:`~kazoo.testing.KazooTestHarness.add_errors` method that can be passed
an error structure composed of :class:`~kazoo.testing.ZooError` that will be
used for the underlying Python `zookeeper` library calls.

Example:

.. code-block:: python

    from kazoo.testing import KazooTestCase
    from kazoo.testing import ZooError

    class MyTest(KazooTestCase):
        def testmycode(self):
            errors = dict(
                acreate=[
                    ZooError('completion', zookeeper.CONNECTIONLOSS, False),
                    True,
                    ZooError('call', SystemError(), False)
                ]
            )

            self.client.add_errors(errors)

            self.client.ensure_path('/test/path')
            result = self.client.get('/test/path')
