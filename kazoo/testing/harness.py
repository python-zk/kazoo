"""Kazoo testing harnesses"""
from __future__ import annotations

import atexit
import logging
import os
import uuid
import unittest

from typing import Any, Callable, Literal, cast, TYPE_CHECKING

if TYPE_CHECKING:
    import kazoo.interfaces

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from kazoo.protocol.connection import _CONNECTION_DROP, _SESSION_EXPIRED
from kazoo.protocol.states import KazooState
from kazoo.testing.common import ZookeeperCluster

log = logging.getLogger(__name__)

CLUSTER: ZookeeperCluster | None = None
CLUSTER_CONF: dict[str, Any] | None = None
CLUSTER_DEFAULTS = {
    "ZOOKEEPER_PORT_OFFSET": 20000,
    "ZOOKEEPER_CLUSTER_SIZE": 3,
    "ZOOKEEPER_OBSERVER_START_ID": -1,
    "ZOOKEEPER_LOCAL_SESSION_RO": "false",
}
MAX_INIT_TRIES = 5


# FIXME use a typeddict for cluster conf and cluster defaults
def get_global_cluster() -> ZookeeperCluster:
    global CLUSTER, CLUSTER_CONF
    cluster_conf = {
        k: os.environ.get(k, CLUSTER_DEFAULTS.get(k))
        for k in [
            "ZOOKEEPER_PATH",
            "ZOOKEEPER_CLASSPATH",
            "ZOOKEEPER_PORT_OFFSET",
            "ZOOKEEPER_CLUSTER_SIZE",
            "ZOOKEEPER_VERSION",
            "ZOOKEEPER_OBSERVER_START_ID",
            "ZOOKEEPER_JAAS_AUTH",
            "ZOOKEEPER_LOCAL_SESSION_RO",
        ]
    }
    if CLUSTER is not None:
        if CLUSTER_CONF == cluster_conf:
            return CLUSTER
        else:
            log.info("Config change detected. Reconfiguring cluster...")
            CLUSTER.terminate()
            CLUSTER = None
    # Create a new cluster
    ZK_HOME = cast(str, cluster_conf.get("ZOOKEEPER_PATH"))
    ZK_CLASSPATH = cast(str, cluster_conf.get("ZOOKEEPER_CLASSPATH"))
    ZK_PORT_OFFSET = int(  # type: ignore[call-overload]
        cluster_conf.get("ZOOKEEPER_PORT_OFFSET")
    )
    ZK_CLUSTER_SIZE = int(  # type: ignore[call-overload]
        cluster_conf.get("ZOOKEEPER_CLUSTER_SIZE")
    )
    ZK_VERSION_STR = cast(str, cluster_conf.get("ZOOKEEPER_VERSION"))
    if "-" in ZK_VERSION_STR:
        # Ignore pre-release markers like -alpha
        ZK_VERSION_STR = ZK_VERSION_STR.split("-")[0]
    ZK_VERSION = tuple([int(n) for n in ZK_VERSION_STR.split(".")])
    ZK_OBSERVER_START_ID = int(  # type: ignore[call-overload]
        cluster_conf.get("ZOOKEEPER_OBSERVER_START_ID")
    )

    assert ZK_HOME or ZK_CLASSPATH or ZK_VERSION, (
        "Either ZOOKEEPER_PATH or ZOOKEEPER_CLASSPATH or "
        "ZOOKEEPER_VERSION environment variable must be defined.\n"
        "For deb package installations this is /usr/share/java"
    )

    if ZK_VERSION >= (3, 5):
        ZOOKEEPER_LOCAL_SESSION_RO = cast(
            str, cluster_conf.get("ZOOKEEPER_LOCAL_SESSION_RO")
        )
        additional_configuration_entries = [
            "4lw.commands.whitelist=*",
            "reconfigEnabled=true",
            # required to avoid session validation error
            # in read only test
            "localSessionsEnabled=" + ZOOKEEPER_LOCAL_SESSION_RO,
            "localSessionsUpgradingEnabled=" + ZOOKEEPER_LOCAL_SESSION_RO,
        ]
        # If defined, this sets the superuser password to "test"
        additional_java_system_properties = [
            "-Dzookeeper.DigestAuthenticationProvider.superDigest="
            "super:D/InIHSb7yEEbrWz8b9l71RjZJU="
        ]
    else:
        additional_configuration_entries = []
        additional_java_system_properties = []
    ZOOKEEPER_JAAS_AUTH = cluster_conf.get("ZOOKEEPER_JAAS_AUTH")
    if ZOOKEEPER_JAAS_AUTH == "digest":
        jaas_config = """
Server {
    org.apache.zookeeper.server.auth.DigestLoginModule required
    user_super="super_secret"
    user_jaasuser="jaas_password";
};"""
    elif ZOOKEEPER_JAAS_AUTH == "gssapi":
        # Configure Zookeeper to use our test KDC.
        additional_java_system_properties += [
            "-Djava.security.krb5.conf=%s"
            % os.path.expandvars("${KRB5_CONFIG}"),
            "-Dsun.security.krb5.debug=true",
        ]
        jaas_config = """
Server {
  com.sun.security.auth.module.Krb5LoginModule required
  debug=true
  isInitiator=false
  useKeyTab=true
  keyTab="%s"
  storeKey=true
  useTicketCache=false
  principal="zookeeper/127.0.0.1@KAZOOTEST.ORG";
};""" % os.path.expandvars(
            "${KRB5_TEST_ENV}/server.keytab"
        )
    else:
        jaas_config = None

    CLUSTER = ZookeeperCluster(
        install_path=ZK_HOME,
        classpath=ZK_CLASSPATH,
        port_offset=ZK_PORT_OFFSET,
        size=ZK_CLUSTER_SIZE,
        observer_start_id=ZK_OBSERVER_START_ID,
        configuration_entries=additional_configuration_entries,
        java_system_properties=additional_java_system_properties,
        jaas_config=jaas_config,
    )
    CLUSTER_CONF = cluster_conf
    atexit.register(lambda cluster: cluster.terminate(), CLUSTER)
    return CLUSTER


class KazooTestHarness(unittest.TestCase):
    """Harness for testing code that uses Kazoo

    This object can be used directly or as a mixin. It supports starting
    and stopping a complete ZooKeeper cluster locally and provides an
    API for simulating errors and expiring sessions.

    Example::

        class MyTestCase(KazooTestHarness):
            def setUp(self) -> None:
                self.setup_zookeeper()

                # additional test setup

            def tearDown(self)-> None:
                self.teardown_zookeeper()

            def test_something(self) -> None:
                something_that_needs_a_kazoo_client(self.client)

            def test_something_else(self) -> None:
                something_that_needs_zk_servers(self.servers)

    """

    DEFAULT_CLIENT_TIMEOUT = 15

    def __init__(self, *args: Any, **kw: Any):
        super(KazooTestHarness, self).__init__(*args, **kw)
        self._client: KazooClient | None = None
        self._clients: list[KazooClient] = []

    @property
    def cluster(self) -> ZookeeperCluster:
        return get_global_cluster()

    @property
    def client(self) -> KazooClient:
        assert self._client is not None
        return self._client

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        log.log(level, msg, *args, **kwargs)

    @property
    def servers(self) -> str:
        return ",".join([s.address for s in self.cluster])

    @property
    def secure_servers(self) -> str:
        return ",".join([s.secure_address for s in self.cluster])

    def _get_nonchroot_client(self) -> KazooClient:
        c = KazooClient(self.servers)
        self._clients.append(c)
        return c

    def _get_client(self, **client_options: Any) -> KazooClient:
        if "timeout" not in client_options:
            client_options["timeout"] = self.DEFAULT_CLIENT_TIMEOUT
        c = KazooClient(self.hosts, **client_options)
        self._clients.append(c)
        return c

    def lose_connection(
        self, event_factory: Callable[[], kazoo.interfaces.Event]
    ) -> None:
        """Force client to lose connection with server"""
        self.__break_connection(
            _CONNECTION_DROP, KazooState.SUSPENDED, event_factory
        )

    def expire_session(
        self, event_factory: Callable[[], kazoo.interfaces.Event]
    ) -> None:
        """Force ZK to expire a client session"""
        self.__break_connection(
            _SESSION_EXPIRED, KazooState.LOST, event_factory
        )

    def setup_zookeeper(self, **client_options: Any) -> None:
        """Create a ZK cluster and chrooted :class:`KazooClient`

        The cluster will only be created on the first invocation and won't be
        fully torn down until exit.
        """
        do_start = False
        for s in self.cluster:
            if not s.running:
                do_start = True
        if do_start:
            self.cluster.start()
        namespace = "/kazootests" + uuid.uuid4().hex
        self.hosts = self.servers + namespace

        tries = 0
        while True:
            try:
                client_cluster_health = self._get_client()
                client_cluster_health.start()
                client_cluster_health.ensure_path("/")
                client_cluster_health.stop()
                self.log(logging.INFO, "cluster looks ready to go")
                break
            except Exception:
                tries += 1
                if tries >= MAX_INIT_TRIES:
                    raise
                if tries > 0 and tries % 2 == 0:
                    self.log(
                        logging.WARNING,
                        "nuking current cluster and making another one",
                    )
                    self.cluster.terminate()
                    self.cluster.start()
                continue
        if client_options.get("use_ssl"):
            self.hosts = self.secure_servers + namespace
        else:
            self.hosts = self.servers + namespace
        self._client = self._get_client(**client_options)
        self.client.start()
        self.client.ensure_path("/")

    def teardown_zookeeper(self) -> None:
        """Reset and cleanup the zookeeper cluster that was started."""
        while self._clients:
            c = self._clients.pop()
            try:
                c.stop()
            except KazooException:
                log.exception("Failed stopping client %s", c)
            finally:
                c.close()
        self._client = None

    def __break_connection(
        self,
        break_event: object,
        expected_state: KazooState,
        event_factory: Callable[[], kazoo.interfaces.Event],
    ) -> None:
        """Break ZooKeeper connection using the specified event."""

        lost = event_factory()
        safe = event_factory()

        def watch_loss(state: KazooState) -> Literal[True] | None:
            if state == expected_state:
                lost.set()
            elif lost.is_set() and state == KazooState.CONNECTED:
                safe.set()
                return True
            return None

        self.client.add_listener(watch_loss)
        self.client._call(break_event, None)  # type: ignore[arg-type]

        lost.wait(5)
        if not lost.is_set():
            raise Exception("Failed to get notified of broken connection.")

        safe.wait(15)
        if not safe.is_set():
            raise Exception("Failed to see client reconnect.")

        self.client.retry(self.client.get_async, "/")


class KazooTestCase(KazooTestHarness):
    def setUp(self) -> None:
        self.setup_zookeeper()

    def tearDown(self) -> None:
        self.teardown_zookeeper()

    @classmethod
    def tearDownClass(cls) -> None:
        cluster = get_global_cluster()
        if cluster is not None:
            cluster.terminate()
