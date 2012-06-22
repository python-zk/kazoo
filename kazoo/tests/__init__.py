import atexit
import logging
import os
import unittest
import uuid
from collections import namedtuple

import zookeeper
from kazoo.client import Callback
from kazoo.client import KazooClient
from kazoo.tests.common import ZookeeperCluster

log = logging.getLogger(__name__)

ZK_HOME = os.environ.get("ZOOKEEPER_PATH")
assert ZK_HOME, (
    "ZOOKEEPER_PATH environment variable must be defined.\n "
    "For deb package installations this is /usr/share/java")

CLUSTER = ZookeeperCluster(ZK_HOME)
atexit.register(lambda cluster: cluster.terminate(), CLUSTER)


class ZooError(namedtuple('ZooError', ('when', 'exception', 'allow'))):
    """A Zookeeper Error to throw instead of or in addition to executing the
    zookeeper command

    Since the :class:`KazooClient` implements most of the zookeeper commands
    using the async calls, the exception could occur during the call itself
    (which is rare), or on the completion.

    .. attribute:: when

        When the exception should be tossed, can be set to 'call' or
        'completion'.

    .. attribute:: exception

        The exception instance or zookeeper exception code (an int).

    .. attribute:: allow

        Boolean indicating if the zookeeper function should be called
        even though an exception is being returned. This is useful for
        reproducing rare events in Zookeeper where a connection is lost
        before a response is returned but the command ran successfully.

    """


class ZookeeperErrors(object):
    """A Zookeeper proxy with errors"""
    def __init__(self, errors, handler):
        """Create a :class:`ZookeeperErrors` object that throws the
        desired errors upon calls

        :param errors: A dict structure keyed by the zookeeper function
                       to intercept, with the value being a list of
                       :class:`ZooError`s to throw. When the list is empty
                       calls will proceed as normal, include ``True`` in a
                       place to allow a call to pass if desired as well.
        :param handler: The handler instance used by the KazooClient.

        Example::

            errors = dict(
                acreate=[
                    ZooError('completion', zookeeper.CONNECTIONLOSS, False),
                    True,
                    ZooError('call', SystemError(), False)
                ]
            )

        .. warning::

            Care should be taken when simulating errors that your test code
            executes the zookeeper commands in a reliably deterministic manner.

        """
        for k, v in errors.items():
            v.reverse()
        self.errors = errors
        self.handler = handler

    def _intercept_completion(self):
        # Completion callback is always at the end
        asy = self.handler.async_result()

        def new_completion(handle, code, *args):
            log.info("Intercept completion, handle: %s, "
                     "code: %s, args: %s", handle, code, args)
            asy.set()
        return asy, new_completion

    def _intercept_watcher(self):
        def new_watcher(*args):
            log.info("Intercept watcher, args: %s", args)
        return new_watcher

    def _call_exception(self, err, name, func, *args, **kwargs):
        if err.allow:
            # Strip completion/watch callbacks, replace with dummies
            asy = False
            if callable(args[-1]):
                asy, args[-1] = self._intercept_completion()
            if callable(args[-2]):
                args[-2] = self._intercept_watcher()
            log.debug("Calling actual function: %s", name)
            func(*args, **kwargs)
            if asy:
                asy.wait()
            log.debug("Raising desired exception: %s", err.exception)
            raise err.exception
        else:
            log.debug("Raising desired exception on call: %s",
                      err.exception)
            raise err.exception

    def _completion_exception(self, err, name, func, *args, **kwargs):
        if callable(args[-1]):
            # Replace completion callback with one that returns the
            # exception
            old_complete = args[-1]
            args[-1] = lambda handle, code, *new_args: old_complete(
                handle, err.exception, *new_args)
        if callable(args[-2]):
            # Strip the watch callback
            args[-2] = lambda *args: 1
        # Call it if we're supposed to
        if err.allow:
            func(*args, **kwargs)
        else:
            # Make sure the completion callback is called
            self.handler.dispatch_callback(
                Callback('completion', func, (None, err.exception))
            )
        return zookeeper.OK

    def __getattr__(self, name):
        func = getattr(zookeeper, name)
        err = self.errors.get(name)
        if err:
            err = err.pop()
        else:
            return func

        def func_wrapper(*args, **kwargs):
            if err.when == 'call':
                return self._call_exception(err, name, func, *args, **kwargs)
            else:
                return self._completion_exception(err, name, func, *args,
                                                  **kwargs)
        return func_wrapper


class KazooTestCase(unittest.TestCase):
    @property
    def cluster(self):
        return CLUSTER

    @property
    def servers(self):
        return ",".join([s.address for s in self.cluster])

    def _get_nonchroot_client(self):
        return KazooClient(self.servers)

    def _get_client(self, **kwargs):
        return KazooClient(self.hosts, **kwargs)

    def expire_session(self, client_id=None):
        client_id = client_id or self.client.client_id
        client = KazooClient(self.cluster[1].address, client_id=client_id)
        client.connect()
        client.stop()

    def setUp(self):
        zookeeper.deterministic_conn_order(True)
        if not self.cluster[0].running:
            self.cluster.start()
        namespace = "/kazootests" + uuid.uuid4().hex
        self.hosts = self.servers + namespace

        self.client = self._get_client()

    def tearDown(self):
        if not self.cluster[0].running:
            self.cluster.start()

        if self.client.connected:
            self.client.stop()

        client = self._get_client()
        client.connect()
        client.recursive_delete('/')
        client.stop()

    def add_errors(self, errors):
        self.client.zookeeper = ZookeeperErrors(errors, self.client._handler)
