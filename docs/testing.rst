.. _testing:

=======
Testing
=======

Kazoo has several test harnesses used internally for its own tests that are
exposed as public API's for use in your own tests for common Zookeeper cluster
management and session testing. They can be mixed in with your own `unittest`
or `nose` tests along with a `mock` object that allows you to force specific
`KazooClient` commands to fail in various ways.

The test harness needs to be able to find the Zookeeper Java libraries. You
need to specify an environment variable called `ZOOKEEPER_PATH` and point it
to their location, for example `/usr/share/java`. The directory should contain
a `zookeeper-*.jar` and a `lib` directory containing at least a `log4j-*.jar`.

If your Java setup is complex, you may also override our classpath mechanism
completely by specifying an environment variable called `ZOOKEEPER_CLASSPATH`.
If provided, it will be used unmodified as the Java classpath for Zookeeper.


Kazoo Test Harness
==================

The :class:`~kazoo.testing.harness.KazooTestHarness` can be used directly or
mixed in with your test code.

Example:

.. code-block:: python

    from kazoo.testing import KazooTestHarness

    class MyTest(KazooTestHarness):
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

The :class:`~kazoo.testing.harness.KazooTestCase` is complete test case that
is equivalent to the mixin setup of
:class:`~kazoo.testing.harness.KazooTestHarness`. An equivalent test to the
one above:

.. code-block:: python

    from kazoo.testing import KazooTestCase

    class MyTest(KazooTestCase):
        def testmycode(self):
            self.client.ensure_path('/test/path')
            result = self.client.get('/test/path')
            ...
