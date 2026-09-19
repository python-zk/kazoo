"""Thin pytest glue for the kazoo ZooKeeper integration harness.

This module holds the pytest-facing fixture and hook *definitions* so that
pytest can discover them through the integration ``conftest``. All harness
logic — axis resolution, ensemble and client plumbing, Docker availability and
bind-mount translation, capture / keylog / Kerberos assembly, and marker
evaluation — lives in :mod:`kazoo.testing.common`, which stays importable
without pytest or a Docker engine.
"""

from __future__ import annotations

import functools
import os
import pathlib
import uuid
from importlib import resources
from typing import TYPE_CHECKING

import pytest

from kazoo.testing import common

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any

    from testcontainers.compose import DockerCompose

    from kazoo.client import KazooClient

__all__ = [
    "docker_compose",
    "docker_compose_config",
    "docker_env",
    "zkchroot",
    "zkclient",
    "zkensemble",
    "zksuperadmin_client",
]


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register CLI options for the three testing axes."""
    parser.addoption(
        "--zk-version",
        action="store",
        default=None,
        help=(
            "ZooKeeper server version (e.g. 3.7, 3.8, 3.9). "
            "Defaults to $KAZOO_TESTING_ZK_VERSION or $ZK_VERSION or '3.9.5'."
        ),
    )
    parser.addoption(
        "--zk-auth",
        action="store",
        default=None,
        choices=[mode.value for mode in common.ZKAuthMode],
        help=(
            "ZooKeeper authentication flavor: plain, digest, sasl_digest, "
            "sasl_gssapi, tls. Defaults to $KAZOO_TESTING_ZK_AUTH or "
            "$ZK_AUTH or 'plain'."
        ),
    )
    parser.addoption(
        "--zk-features",
        action="store",
        default=None,
        help=(
            "Comma-separated ZooKeeper feature set: standard, ttl, readonly, "
            "reconfig, capture. Defaults to $KAZOO_TESTING_ZK_FEATURES or "
            "$ZK_FEATURES or 'standard'."
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register our custom markers so pytest knows about them."""
    config.addinivalue_line(
        "markers",
        "zk_version(spec): Run only when the active ZK version matches "
        "the PEP 440 SpecifierSet.",
    )
    config.addinivalue_line(
        "markers",
        "zk_auth(modes): Run only when the active ZK auth mode is one "
        "of the specified modes.",
    )
    config.addinivalue_line(
        "markers",
        "zk_features(features): Run only when all specified features "
        "are active on the ensemble.",
    )


def pytest_collection_modifyitems(
    session: pytest.Session,
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Apply collection-time skip evaluation for the axis markers.

    Incompatible tests are skipped before any client/ensemble is spun up, so
    they never attempt connections.
    """
    version, auth, features = common._resolve_axis_options(config)
    for item in items:
        reason = common._evaluate_axis_markers(item, version, auth, features)
        if reason is not None:
            item.add_marker(pytest.mark.skip(reason=reason))


@pytest.fixture(scope="session", autouse=True)
def docker_env(
    pytestconfig: pytest.Config,
    tmp_path_factory: pytest.TempPathFactory,
) -> "Iterator[common.KazooZkEnv]":
    tmp_path: pathlib.Path = tmp_path_factory.getbasetemp()
    old_environ = dict(os.environ)
    try:
        # Compose interpolates ${KAZOO_TESTING_ZK_WORK_DIR} into bind-mount
        # sources. Host-side file ops below keep the native Path; the env var
        # is what compose hands to the daemon, so it may need translation to
        # a daemon-visible mount path on Windows-remotes (see
        # _daemon_mount_path).
        os.environ["KAZOO_TESTING_ZK_WORK_DIR"] = common._daemon_mount_path(
            tmp_path
        )
        # Unique per-session compose project name keeps parallel test runs (and
        # any stray stacks from other projects) isolated from each other.
        os.environ["COMPOSE_PROJECT_NAME"] = f"kazoo-{uuid.uuid4().hex[:8]}"
        version, auth, features = common._resolve_axis_options(pytestconfig)
        yield common.KazooZkEnv(
            version=version,
            workdir=tmp_path,
            auth=auth,
            features=features,
        )
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


@pytest.fixture(scope="session")
def docker_compose_config(
    docker_env: "common.KazooZkEnv",
) -> dict[str, Any]:
    """Resolve the docker-compose overlay files for the active axis.

    The base file (``docker-compose.base.yml``) is always included. For any
    non-plain authentication flavor an overlay file
    (``docker-compose.auth-<auth>.yml``) is layered on top via docker-compose
    multi-file support. The capture feature layers
    ``docker-compose.features-capture.yml`` over whatever base/auth is active.

    Interpolation variables (KAZOO_TESTING_ZK_VERSION,
    KAZOO_TESTING_ZK_FEATURES_JVMFLAGS, KAZOO_TESTING_ZK_WORK_DIR,
    COMPOSE_PROJECT_NAME) are exported to the process environment by
    :func:`~kazoo.testing.common._resolve_axis_options` (via ``docker_env``)
    and this fixture before ``docker_compose`` runs.
    """
    auth = docker_env.auth
    features = docker_env.features
    compose_files = common.resolve_compose_files(auth, features)
    jvm_flags = []
    for feature in features:
        for prop in common.FEATURE_JVM_PROPERTIES.get(feature, ()):
            jvm_flags.append(prop)
    os.environ["KAZOO_TESTING_ZK_FEATURES_JVMFLAGS"] = " ".join(jvm_flags)
    return {
        "version": docker_env.version,
        "auth": auth,
        "features": features,
        "compose_files": compose_files,
    }


@pytest.fixture(scope="session")
def docker_compose(
    request: pytest.FixtureRequest,
    docker_compose_config: dict[str, Any],
    docker_env: "common.KazooZkEnv",
) -> "Iterator[DockerCompose]":
    """Start the ZooKeeper ensemble stack via docker-compose (testcontainers).

    Session-scoped: the ensemble is brought up once before the first test and
    torn down (including volumes) after the last test. Individual ensemble
    members are controlled per-test through :meth:`ZkEnsemble.stop` /
    :meth:`ZkEnsemble.start`.

    The ``testcontainers.compose.DockerCompose`` driver is imported lazily so
    that ``kazoo.testing`` stays importable in environments where the
    test-only dependency is not installed.
    """
    from testcontainers.compose import DockerCompose

    # Compose files and the resources they reference (jaas/, dockerfiles/)
    # live in the kazoo.testing package. Locate the directory via
    # importlib.resources so discovery does not depend on __file__ (it
    # resolves to the real on-disk dir for any filesystem-backed install).
    with resources.as_file(resources.files("kazoo.testing")) as context_path:
        context = str(context_path)
        # Relative bind-mount sources in the compose overlays (./jaas/...) are
        # interpolated through ${KAZOO_TESTING_ZK_COMPOSE_DIR} so they can be
        # translated to a daemon-visible mount path on Windows-remote setups,
        # exactly like ${KAZOO_TESTING_ZK_WORK_DIR} above.
        os.environ["KAZOO_TESTING_ZK_COMPOSE_DIR"] = common._daemon_mount_path(
            context_path
        )
        common._ensure_docker_available(context)

        compose = DockerCompose(
            context=context,
            compose_file_name=docker_compose_config["compose_files"],
        )

        # Capture preflight: when `capture` is active, build the in-repo image
        # declared by the capture overlay (dockerfiles/capture) *before* `up`,
        # so a build failure aborts the session with an actionable message
        # instead of failing opaquely mid-`up` (a network/registry outage for
        # `apk` tshark is reported here).
        if common.ZKFeature.CAPTURE in docker_compose_config["features"]:
            common._build_capture_images(compose, context)

        try:
            compose.start()
            common.set_compose_handle(compose)
            # Belt-and-suspenders beyond `up --wait`: fail fast with a precise
            # message if any ensemble member's ZK JVM is not actually healthy.
            # The healthcheck lives on the -service services (the netns holders
            # zoo1/zoo2/zoo3 run no JVM and carry no healthcheck).
            for node in ("zoo1-service", "zoo2-service", "zoo3-service"):
                container = compose.get_container(node)
                if container.Health != "healthy":
                    raise RuntimeError(
                        f"{node} did not become healthy after `docker compose "
                        f"up --wait` (state={container.State!r}, "
                        f"health={container.Health!r})"
                    )
            yield compose
        finally:
            # Runs even when `start()` itself raised partway (e.g. one node
            # never became healthy), so `down --volumes` cleans up the stack.
            if request.session.testsfailed:
                common.dump_ensemble_logs()
            # Assemble the TLS keylog + context certs before the stack
            # goes down, so the decryption material for the pcapng artifacts
            # is available after teardown. No-op on non-tls/non-capture runs.
            emitted = common._assemble_tls_keylog(
                docker_env.workdir, docker_env.auth, docker_env.features
            )
            if emitted:
                paths = ", ".join(map(str, emitted))
                print(f"[kazoo] capture keylog artifacts: {paths}")
            # Teardown never deletes capture artifacts: `down --volumes`
            # removes only *named compose volumes* (the tmpfs zooN data
            # volumes), never the bound directories under
            # ${KAZOO_TESTING_ZK_WORK_DIR} (captures/, logs/, certs/, agent/),
            # so the pcapngs + decryption material survive unchanged and remain
            # on disk after the session for analysis.
            common.set_compose_handle(None)
            compose.stop()


@pytest.fixture(scope="function")
def zkensemble(
    docker_compose: "DockerCompose",
    docker_env: "common.KazooZkEnv",
) -> "common.ZkEnsemble":
    """Provide a per-test handle on the running ZooKeeper ensemble.

    Unlike a session-scoped handle, this fixture is created fresh for every
    test so that each test can create its own clients and control individual
    ensemble members (e.g. stop/start via :meth:`ZkEnsemble.stop`).
    """

    # TLS-transport axes (tls, sasl_gssapi) expose the client port only on the
    # secureClientPort (2281, published as an ephemeral host port); plain,
    # digest and sasl_digest talk to the plain client port (2181).
    client_port = (
        2281
        if docker_env.auth
        in (
            common.ZKAuthMode.TLS,
            common.ZKAuthMode.SASL_GSSAPI,
        )
        else 2181
    )

    # The ensemble exposes its client ports on ephemeral host ports; resolve
    # the actual host address/ports via the running compose stack.
    p1 = docker_compose.get_service_port("zoo1", client_port)
    p2 = docker_compose.get_service_port("zoo2", client_port)
    p3 = docker_compose.get_service_port("zoo3", client_port)
    if p1 is None or p2 is None or p3 is None:
        raise RuntimeError(
            "Failed to resolve ZooKeeper ensemble service ports"
        )
    zk1_port = int(p1)
    zk2_port = int(p2)
    zk3_port = int(p3)

    if docker_env.auth is common.ZKAuthMode.SASL_GSSAPI:
        common._export_krb5_client_env(docker_env, docker_compose)

    # ``get_service_host`` returns the publisher's bind address (``0.0.0.0`` /
    # ``::`` on macOS/Linux; testcontainers only rewrites those to 127.0.0.1 on
    # Windows). Clients must connect over the loopback interface where the
    # published ports actually listen, and the GSSAPI service principal for
    # sasl_gssapi is derived from the connect host (``zookeeper@<host>``), so a
    # wildcard host there yields ``zookeeper@0.0.0.0`` and a PROCESS_TGS error.
    zk_ip = str(docker_compose.get_service_host("zoo1", client_port))
    if not zk_ip or zk_ip in ("0.0.0.0", "::", "::1", "localhost"):
        zk_ip = "127.0.0.1"

    return common.ZkEnsemble(
        zk_ip=zk_ip,
        zk1_port=zk1_port,
        zk2_port=zk2_port,
        zk3_port=zk3_port,
        version=docker_env.version,
        workdir=docker_env.workdir,
        auth=docker_env.auth,
        features=docker_env.features,
        compose=docker_compose,
    )


@pytest.fixture(scope="function")
def zkchroot(request: pytest.FixtureRequest) -> str:
    """Unique per-test chroot path within the active ensemble."""
    return f"/{os.path.basename(request.node.nodeid)}-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="function")
def zkclient(
    zkensemble: "common.ZkEnsemble",
    zkchroot: str,
) -> "Iterator[KazooClient]":
    """Create a KazooClient instance connected to the ensemble."""
    client = zkensemble.get_client()
    setattr(
        client,
        "harness_expire_session",
        functools.partial(
            zkensemble.expire_session,
            client=client,
            event_factory=client.handler.event_object,
        ),
    )
    client.start()
    client.ensure_path(zkchroot)
    client.chroot = zkchroot
    yield client
    client.stop()
    client.close()


@pytest.fixture(scope="function")
def zksuperadmin_client(
    zkensemble: "common.ZkEnsemble",
    zkchroot: str,
) -> "Iterator[KazooClient]":
    """Create a KazooClient connected as superadmin to the ensemble."""
    client = zkensemble.get_client(superadmin=True)
    client.start()
    client.ensure_path(zkchroot)
    client.chroot = zkchroot
    yield client
    client.stop()
    client.close()
