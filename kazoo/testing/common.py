"""Business logic for the kazoo integration-test zoo harness.

This module holds the harness logic that is independent of pytest fixtures:
the testing axes (version / auth / features) and their JVM-flag mappings, the
ensemble and client helpers, the Docker availability and bind-mount path
translation helpers, the capture / keylog / Kerberos environment assembly, and
the marker-evaluation and axis-resolution functions that drive skip decisions.

Pure helpers take their environment-dependent inputs as parameters so they can
be unit-tested without a Docker engine or a live ZooKeeper; the thin
pytest-facing wrappers and fixtures live in :mod:`kazoo.testing.fixtures`.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
from typing import TYPE_CHECKING, Any, Callable, cast

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum

import pytest
from packaging import (
    specifiers,
    version,
)

import kazoo.client
from kazoo.interfaces import Event
from kazoo.protocol.connection import (
    _CONNECTION_DROP,
    _SESSION_EXPIRED,
)
from kazoo.protocol.states import KazooState

if TYPE_CHECKING:
    from testcontainers.compose import DockerCompose

    from kazoo.client import KazooClient
else:
    # Runtime stand-ins so sphinx-autodoc can resolve the quoted forward
    # references above when it evaluates annotations: kazoo.testing must not
    # import kazoo.client at runtime, and the docs env ships no testcontainers.
    # They are never used at runtime; type checkers only see the TYPE_CHECKING
    # imports.
    class DockerCompose:
        pass

    class KazooClient:
        pass


# The three testing axes. The "auth" axis selects the docker-compose flavor
# (and therefore which client-side connection options make sense), while the
# "features" axis controls ZooKeeper JVM/system flags.
class ZKAuthMode(StrEnum):
    PLAIN = "plain"
    DIGEST = "digest"
    SASL_DIGEST = "sasl_digest"
    SASL_GSSAPI = "sasl_gssapi"
    TLS = "tls"


class ZKFeature(StrEnum):
    STANDARD = "standard"
    TTL = "ttl"
    READONLY = "readonly"
    RECONFIG = "reconfig"
    # Harness-level feature: adds the capture sidecar to the compose stack.
    # Deliberately absent from FEATURE_JVM_PROPERTIES below — capture is a
    # harness observation feature, not a ZooKeeper server feature, so it must
    # contribute no server JVM flags.
    CAPTURE = "capture"


ZK_DEFAULT_VERSION = "3.9.5"

# feature -> JVM/system properties (injected into the server environment)
FEATURE_JVM_PROPERTIES: dict[ZKFeature, tuple[str, ...]] = {
    ZKFeature.STANDARD: (),
    ZKFeature.TTL: ("-Dzookeeper.extendedTypesEnabled=true",),
    # Note: readonlymode.enabled is a JVM system property
    # (-Dreadonlymode.enabled=true), not a zoo.cfg property.
    ZKFeature.READONLY: ("-Dreadonlymode.enabled=true",),
    ZKFeature.RECONFIG: ("-Dzookeeper.reconfigEnabled=true",),
}

# auth -> JVM/system properties (injected into the server environment).
# These are exported to the compose environment as ZK_AUTH_JVMFLAGS and
# interpolated into SERVER_JVMFLAGS by the base compose file.
AUTH_JVM_FLAGS: dict[ZKAuthMode, str] = {
    ZKAuthMode.PLAIN: "",
    ZKAuthMode.DIGEST: "",
    ZKAuthMode.SASL_DIGEST: "",
    ZKAuthMode.SASL_GSSAPI: "",
    ZKAuthMode.TLS: "",
}

#: Compose overlay file names, keyed by the compose-file basename.
_COMPOSE_BASE = "docker-compose.base.yml"
_COMPOSE_CAPTURE = "docker-compose.features-capture.yml"


def resolve_compose_files(
    auth: ZKAuthMode,
    features: tuple[ZKFeature, ...],
) -> list[str]:
    """Return the ordered compose overlay files for the active axis.

    The base file is always included. Every non-plain auth flavor layers its
    ``docker-compose.auth-<auth>.yml`` overlay (the flavor value's underscore
    maps to a hyphen in the file name); the capture feature layers
    ``docker-compose.features-capture.yml`` on top of whatever is active.
    """
    compose_files = [_COMPOSE_BASE]
    if auth is not ZKAuthMode.PLAIN:
        overlay = auth.value.replace("_", "-")
        compose_files.append(f"docker-compose.auth-{overlay}.yml")
    if ZKFeature.CAPTURE in features:
        compose_files.append(_COMPOSE_CAPTURE)
    return compose_files


def resolve_axis_options(
    version_opt: str | None,
    auth_opt: str | None,
    features_opt: str | None,
    environ: dict[str, str],
) -> tuple[
    str,
    ZKAuthMode,
    tuple[ZKFeature, ...],
    dict[str, str],
]:
    """Resolve the three axes from CLI options and environment variables.

    Returns the resolved (version, auth, features) triple together with the
    environment variables that interpolation of the compose files reads:
    ``KAZOO_TESTING_ZK_VERSION``, ``KAZOO_TESTING_ZK_AUTH``,
    ``KAZOO_TESTING_ZK_FEATURES``, ``KAZOO_TESTING_ZK_AUTH_JVMFLAGS``,
    ``KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS``, and ``KAZOO_TESTING_ZK_CFG_EXTRA``.
    """
    version_value = (
        version_opt
        or environ.get("KAZOO_TESTING_ZK_VERSION")
        or environ.get("ZK_VERSION", ZK_DEFAULT_VERSION)
    )
    auth = ZKAuthMode(
        auth_opt
        or environ.get("KAZOO_TESTING_ZK_AUTH")
        or environ.get("ZK_AUTH", ZKAuthMode.PLAIN.value)
    )
    features = tuple(
        ZKFeature(f.strip())
        for f in (
            features_opt
            or environ.get("KAZOO_TESTING_ZK_FEATURES")
            or environ.get("ZK_FEATURES", ZKFeature.STANDARD.value)
        ).split(",")
        if f.strip()
    )
    cfg_extra = []
    if ZKFeature.RECONFIG in features:
        cfg_extra.append("reconfigEnabled=true")
    if ZKFeature.READONLY in features:
        # Read-only mode (-Dreadonlymode.enabled=true) allows a partitioned ZK
        # node to accept read connections. However, a partitioned node cannot
        # issue or validate global sessions without a leader/quorum.
        # Enabling localSessionsEnabled and localSessionsUpgradingEnabled
        # in zoo.cfg allows partitioned nodes to issue node-local sessions so
        # clients requesting read_only=True can establish a CONNECTED_RO state.
        cfg_extra.append("localSessionsEnabled=true")
        cfg_extra.append("localSessionsUpgradingEnabled=true")
    if ZKFeature.TTL in features:
        cfg_extra.append("extendedTypesEnabled=true")

    env_updates = {
        "KAZOO_TESTING_ZK_VERSION": version_value,
        "KAZOO_TESTING_ZK_AUTH": auth.value,
        "KAZOO_TESTING_ZK_FEATURES": ",".join(f.value for f in features),
        "KAZOO_TESTING_ZK_AUTH_JVMFLAGS": AUTH_JVM_FLAGS.get(auth, ""),
        "KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS": "",
        "KAZOO_TESTING_ZK_CFG_EXTRA": "\n".join(cfg_extra),
    }

    if ZKFeature.CAPTURE in features and auth is ZKAuthMode.TLS:
        env_updates["KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS"] = (
            "-javaagent:/agent/extract-tls-secrets.jar=/logs/tls-secrets.log"
        )

    return version_value, auth, features, env_updates


def _resolve_axis_options(
    pytestconfig: pytest.Config,
) -> tuple[str, ZKAuthMode, tuple[ZKFeature, ...]]:
    """Resolve the three axes from pytest options, falling back to env vars."""
    version, auth, features, env_updates = resolve_axis_options(
        pytestconfig.getoption("--zk-version"),
        pytestconfig.getoption("--zk-auth"),
        pytestconfig.getoption("--zk-features"),
        dict(os.environ),
    )
    os.environ.update(env_updates)
    return version, auth, features


def _evaluate_axis_markers(
    item: pytest.Item,
    zk_version: str,
    auth: ZKAuthMode,
    features: tuple[ZKFeature, ...],
) -> str | None:
    """Evaluate the zk_version/zk_auth/zk_features markers on a test item.

    Returns an actionable skip reason string, or ``None`` when the item is
    compatible with the active run configuration.
    """
    reasons: list[str] = []

    marker = item.get_closest_marker("zk_version")
    if marker:
        spec = marker.args[0]
        if version.Version(zk_version) not in specifiers.SpecifierSet(spec):
            reasons.append(f"Requires ZK {spec} (active: {zk_version})")

    marker = item.get_closest_marker("zk_auth")
    if marker:
        # Marker args are plain strings (e.g. @pytest.mark.zk_auth("digest"));
        # StrEnum members compare equal to their value, so membership tests
        # against those strings work unchanged.
        allowed = marker.args or ()
        skip = marker.kwargs.get("skip") or ()
        if allowed and auth.value not in allowed:
            reasons.append(
                f"Requires auth in {sorted(allowed)} (active: {auth.value})"
            )
        if auth.value in skip:
            reasons.append(f"Incompatible with auth {auth.value}")

    marker = item.get_closest_marker("zk_features")
    if marker:
        require = marker.kwargs.get("require") or ()
        skip_features = marker.kwargs.get("skip") or ()
        active = {f.value for f in features}
        missing = [f for f in require if f not in active]
        if missing:
            reasons.append(f"Missing required feature(s): {missing}")
        incompatible = [f for f in skip_features if f in active]
        if incompatible:
            reasons.append(
                f"Incompatible with active feature(s): {incompatible}"
            )

    return "; ".join(reasons) if reasons else None


@dataclass(frozen=True)
class KazooZkEnv:
    """Resolved session configuration: version, work dir, auth, features."""

    version: str
    workdir: pathlib.Path
    auth: ZKAuthMode = ZKAuthMode.PLAIN
    features: tuple[ZKFeature, ...] = (ZKFeature.STANDARD,)


@dataclass(frozen=True)
class ZkEnsemble:
    """A running compose-backed ZooKeeper ensemble.

    Carries the resolved client host/ports and the compose handle, and exposes
    client creation, connection-loss/session-expiry helpers, and per-member
    stop/start for failure-injection tests.
    """

    zk_ip: str
    zk1_port: int
    zk2_port: int
    zk3_port: int
    version: str
    compose: "DockerCompose"
    workdir: pathlib.Path
    auth: ZKAuthMode = ZKAuthMode.PLAIN
    features: tuple[ZKFeature, ...] = (ZKFeature.STANDARD,)
    handler: Any = None

    def get_hosts(self) -> str:
        """Return the comma-joined client host:port list for all members."""
        client_hosts = ",".join(
            [
                f"{self.zk_ip}:{port}"
                for port in [self.zk1_port, self.zk2_port, self.zk3_port]
            ]
        )
        return client_hosts

    def _client_implied_options(self) -> dict[str, Any]:
        """Connection options implied by the active auth axis.

        Each implied option is returned under its KazooClient kwarg name and
        is applied independently (see ``get_client``) so that, for example, a
        superadmin client (which supplies its own ``auth_data``) still gets the
        ``sasl_options``/``use_ssl`` implied by a SASL or TLS axis.
        """
        opts: dict[str, Any] = {}
        if self.auth is ZKAuthMode.SASL_DIGEST:
            opts["sasl_options"] = {
                "mechanism": "DIGEST-MD5",
                # DigestServerCallback in the server JAAS config only accepts
                # the hardcoded test users (see jaas/sasl-digest.conf).
                "username": "jaasuser",
                "password": "jaas_password",
            }
        elif self.auth in (ZKAuthMode.TLS, ZKAuthMode.SASL_GSSAPI):
            # TLS transport: client cert + CA produced by the certgen sidecar
            # (see dockerfiles/certgen; sasl_gssapi tunnels GSSAPI over TLS).
            # The bundle carries the key followed by the certificate, so it
            # serves as both certfile and keyfile.
            certs = self.workdir / "certs" / "client"
            opts["use_ssl"] = True
            opts["certfile"] = str(certs / "client.pem")
            opts["keyfile"] = str(certs / "client.pem")
            opts["ca"] = str(certs / "cacert.pem")
            if self.auth is ZKAuthMode.SASL_GSSAPI:
                opts["sasl_options"] = {"mechanism": "GSSAPI"}
        return opts

    def _apply_superadmin_auth(self, kwargs: dict[str, Any]) -> None:
        """Merge the superadmin digest credentials into ``kwargs`` in place."""
        auth_data = kwargs.pop("auth_data", None)
        if auth_data is None:
            kwargs["auth_data"] = [("digest", "super:super_secret")]
        else:
            if isinstance(auth_data, list):
                auth_data.append(("digest", "super:super_secret"))
                kwargs["auth_data"] = auth_data
            else:
                raise ValueError(
                    "Existing 'auth_data' in kwargs must be a list of "
                    "(scheme, credentials) tuples if 'superadmin' is True."
                )

    def get_client(
        self, /, superadmin: bool = False, **kwargs: Any
    ) -> "KazooClient":
        if "hosts" in kwargs:
            client_hosts = kwargs.pop("hosts")
        else:
            client_hosts = self.get_hosts()

        if superadmin:
            # For superadmin, the ZooKeeper server is configured with digest
            # authentication via
            # -Dzookeeper.DigestAuthenticationProvider.superDigest="super:D/InIHSb7yEEbrWz8b9l71RjZJU="
            # in the server JVM flags. The client then authenticates with the
            # cleartext password "super_secret".
            self._apply_superadmin_auth(kwargs)

        # Apply connection options implied by the active auth axis. Each option
        # is set only if the caller did not already provide it explicitly, so
        # an explicit override always wins and no implied option silently
        # clobbers another (e.g. superadmin's auth_data coexists with the
        # sasl_options a SASL axis requires).
        for key, value in self._client_implied_options().items():
            kwargs.setdefault(key, value)

        client = kazoo.client.KazooClient(
            hosts=client_hosts,
            **kwargs,
        )
        object.__setattr__(self, "handler", client.handler)
        return client

    def lose_connection(
        self,
        client: "KazooClient",
        event_factory: Callable[[], Event] | None = None,
    ) -> None:
        """Force client to lose connection with server."""
        factory: Callable[[], Event] = (
            client.handler.event_object
            if event_factory is None
            else event_factory
        )
        self.__break_connection(
            client, _CONNECTION_DROP, KazooState.SUSPENDED, factory
        )

    def expire_session(
        self,
        client: "KazooClient",
        event_factory: Callable[[], Event] | None = None,
    ) -> None:
        """Force ZK to expire a client session."""
        factory: Callable[[], Event] = (
            client.handler.event_object
            if event_factory is None
            else event_factory
        )
        self.__break_connection(
            client, _SESSION_EXPIRED, KazooState.LOST, factory
        )

    def __break_connection(
        self,
        client: "KazooClient",
        break_event: object,
        expected_state: KazooState,
        event_factory: Callable[[], Event],
    ) -> None:
        """Break ZooKeeper connection using the specified event."""

        assert break_event in (_CONNECTION_DROP, _SESSION_EXPIRED)

        lost = event_factory()
        safe = event_factory()

        def watch_loss(state: KazooState) -> bool | None:
            if state == expected_state:
                lost.set()
            elif lost.is_set() and state == KazooState.CONNECTED:
                safe.set()
                return True
            return None

        client.add_listener(watch_loss)
        client._call(break_event, None)  # type: ignore[arg-type]

        lost.wait(5)
        if not lost.is_set():
            raise Exception("Failed to get notified of broken connection.")

        safe.wait(15)
        if not safe.is_set():
            raise Exception("Failed to see client reconnect.")

        client.retry(client.get_async, "/")

    def _run_compose(self, *args: str, handler: Any = None) -> None:
        """Run a ``docker compose`` command against this ensemble's stack."""
        h = handler if handler is not None else self.handler
        _run_cooperative_subprocess(
            [*self.compose.compose_command_property, *args],
            cwd=self.compose.context,
            check=True,
            handler=h,
        )

    def _wait_service_exited(
        self, service: str, timeout: float = 30.0, handler: Any = None
    ) -> None:
        """Wait until the specified compose service reaches 'exited' state."""
        if self.compose is None or not hasattr(self.compose, "get_container"):
            return
        h = handler if handler is not None else self.handler
        sleep_fn = (
            h.sleep_func
            if h is not None and hasattr(h, "sleep_func")
            else time.sleep
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                container = self.compose.get_container(
                    service, include_all=True
                )
                if getattr(container, "State", "").lower() == "exited":
                    return
            except Exception:
                pass
            sleep_fn(0.2)
        raise RuntimeError(
            f"Service '{service}' did not exit within {timeout} seconds."
        )

    def _wait_service_healthy(
        self, service: str, timeout: float = 60.0, handler: Any = None
    ) -> None:
        """Wait until the specified compose service reaches 'healthy' state.

        NOTE: This explicit polling logic is temporary until ``docker compose
        start --wait`` becomes widely supported across Compose releases.
        While recent versions (e.g. Compose v5.x on Docker Desktop) support
        ``--wait`` and ``--wait-timeout`` on ``start``, standard Docker Compose
        v2 (such as on Linux CI runners) only supports ``--wait`` on
        ``docker compose up``.
        """
        if self.compose is None or not hasattr(self.compose, "get_container"):
            return
        h = handler if handler is not None else self.handler
        sleep_fn = (
            h.sleep_func
            if h is not None and hasattr(h, "sleep_func")
            else time.sleep
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                container = self.compose.get_container(service)
                if getattr(container, "Health", "") == "healthy":
                    return
            except Exception:
                pass
            sleep_fn(0.2)
        raise RuntimeError(
            f"Service '{service}' did not reach 'healthy' state within "
            f"{timeout} seconds."
        )

    @staticmethod
    def _process_service(name: str) -> str:
        """Map a member name to the compose service running its ZK JVM.

        Under the network-holder split (docker-compose.base.yml), the compose
        service ``zooN`` is only a netns-holding container while the actual
        ZooKeeper process lives in ``zooN-service``. Failure-injection tests
        stop/start a member's ZooKeeper *process*, so any member name must be
        translated to its ``-service`` twin here; the holder itself is never
        stopped (it keeps the member's network namespace — and the capture
        sidecar's tap — alive across member restarts).
        """
        if name in {"zoo1", "zoo2", "zoo3"}:
            return f"{name}-service"
        return name

    def stop(self, name: str, handler: Any = None) -> None:
        """Stop the specified ZK node's ZooKeeper process and wait until
        exited."""
        service = self._process_service(name)
        self._run_compose("stop", service, handler=handler)
        self._wait_service_exited(service, handler=handler)

    def start(self, name: str, handler: Any = None) -> None:
        """Start the specified ZK node's ZooKeeper process and wait until
        healthy."""
        service = self._process_service(name)
        self._run_compose("start", service, handler=handler)
        self._wait_service_healthy(service, handler=handler)


def _run_cooperative_subprocess(
    cmd: list[str] | tuple[str, ...],
    cwd: str | os.PathLike[str] | None = None,
    check: bool = True,
    handler: Any = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run a subprocess command cooperatively, selecting the subprocess
    implementation based on the active KazooClient handler."""
    target_cwd = str(cwd) if cwd is not None else None
    handler_name = getattr(handler, "name", None)

    if handler_name == "sequential_gevent_handler":
        import gevent.subprocess as gevent_subprocess  # type: ignore[import]

        return gevent_subprocess.run(
            cmd, cwd=target_cwd, check=check, **kwargs
        )
    elif handler_name == "sequential_eventlet_handler":
        from eventlet.green import (  # type: ignore[import]
            subprocess as eventlet_subprocess,
        )

        return cast(
            subprocess.CompletedProcess[Any],
            eventlet_subprocess.run(  # type: ignore[attr-defined]
                cmd, cwd=target_cwd, check=check, **kwargs
            ),
        )

    return subprocess.run(cmd, cwd=target_cwd, check=check, **kwargs)


#: Module-global handle on the running compose stack, set by
#: :func:`kazoo.testing.fixtures.docker_compose` and consumed by
#: :func:`dump_ensemble_logs` while the stack is still up.
_COMPOSE_HANDLE: "DockerCompose | None" = None


def set_compose_handle(compose: "DockerCompose | None") -> None:
    """Record (or clear) the running compose stack for dump_ensemble_logs."""
    global _COMPOSE_HANDLE
    _COMPOSE_HANDLE = compose


def _ensure_docker_available(context: str) -> None:
    """Fail fast if docker compose is unavailable."""
    try:
        subprocess.run(
            ["docker", "compose", "version"],
            cwd=context,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        raise RuntimeError(
            "The 'docker' CLI was not found on PATH; the kazoo integration "
            "tests require a Docker Engine with the Compose v2 plugin "
            "(see https://docs.docker.com/compose/install/)."
        ) from None
    except subprocess.CalledProcessError:
        raise RuntimeError(
            "`docker compose version` failed; the Compose v2 plugin is "
            "required (Compose v2.12+ for `up --wait`)."
        ) from None

    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        raise RuntimeError(
            "`docker info` failed; is the Docker daemon running? The kazoo "
            "integration tests require a running Docker Engine."
        ) from None
    _ensure_linux_docker_backend()


def _daemon_mount_path(
    path: pathlib.Path,
    os_name: str = os.name,
    docker_host: str = os.environ.get("DOCKER_HOST", ""),
) -> str:
    """Return ``path`` as a bind-mount source the docker daemon can see.

    Bind mounts are resolved by the *daemon* host, so a Windows client that
    talks to a remote Linux engine over TCP (``DOCKER_HOST=tcp://...`` — e.g.
    a WSL2-hosted dockerd on the GitHub Windows runner) must expose its drives
    at the daemon's `/mnt/<drive>` mount points rather than as Windows-style
    paths (``D:/a/b``). Docker Desktop's own WSL2 backend uses the same
    ``/mnt`` layout, so this stays valid there too. When the client targets a
    native engine (Docker Desktop, local Linux), the host path is passed
    through unchanged.
    """
    posix = path.as_posix()
    host = docker_host
    if os_name == "nt" and host.startswith(("tcp://", "http://")):
        drive = re.match(r"^([A-Za-z]):(/.+)$", posix)
        if drive:
            return f"/mnt/{drive.group(1).lower()}{drive.group(2)}"
    return posix


def _ensure_linux_docker_backend() -> None:
    """Skip (never fail) when the docker daemon is not a Linux backend.

    The official ZooKeeper image is published for Linux only, so a
    Windows-container daemon (e.g. the GitHub-hosted ``windows-latest``
    runner, whose Moby engine serves Windows containers) cannot pull it and
    ``compose up`` dies with ``no matching manifest for
    windows(...)/amd64``. Detecting the daemon OS up front turns that into a
    clean skip of the whole ensemble suite with an actionable reason instead
    of a wall of per-test errors (a real Windows host with Docker Desktop's
    Linux backend passes normally).
    """
    try:
        ostype = subprocess.run(
            ["docker", "info", "--format", "{{.OSType}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        # The daemon is reachable (checked above); an unusual driver just
        # means this probe is unsupported, so do not hard-fail on it.
        return
    if ostype and ostype.lower() != "linux":
        pytest.skip(
            f"docker engine OSType is {ostype!r}, not 'linux': the "
            "ZooKeeper official image is linux-only, so the ensemble suite "
            "cannot run against a Windows-container docker backend. "
            "Run these tests with Docker Desktop (Linux containers) or a "
            "Linux Docker Engine."
        )


def _build_capture_images(compose: "DockerCompose", context: str) -> None:
    """Build the in-repo capture image before the stack starts.

    ``docker compose up --wait`` would normally build the image declared by
    the overlay, but a failure there surfaces as an opaque error partway into
    ``start()``. Building explicitly first converts any build-time problem
    (Docker network / registry outage for ``apk`` tshark) into a single,
    actionable ``RuntimeError`` raised before the ensemble is even started.
    The session fixture's ``finally`` teardown still runs, so nothing is left
    behind.
    """
    # Reuse the same compose project/context/overlay list the stack will start
    # with; `build` is a bool flag on the driver that only modifies `up`, so
    # the build subcommand is invoked here directly.
    cmd = [*compose.docker_compose_command(), "build"]
    try:
        subprocess.run(
            cmd,
            cwd=context,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        details = exc.stderr.decode("utf-8", "replace").strip() or (
            exc.stdout.decode("utf-8", "replace").strip()
        )
        raise RuntimeError(
            "capture: in-repo image build failed before the stack started "
            f"({details or exc}). Check Docker network/registry reachability "
            "for dockerfiles/capture (apk tshark)."
        ) from exc


def dump_ensemble_logs() -> None:
    """Dump stdout/stderr of every ensemble member to aid failure diagnosis.

    Best-effort only: a stack that is mid-teardown or already removed will
    simply log nothing. Called while the compose stack is still running.
    """
    compose = _COMPOSE_HANDLE
    if compose is None:
        return

    def _print_logs(service: str) -> None:
        try:
            stdout, stderr = compose.get_logs(service)
        except Exception as exc:  # noqa: BLE001 - best-effort log dump
            print(f"\n[kazoo] failed to fetch logs for {service}: {exc!r}")
            return
        for label, stream in (("stdout", stdout), ("stderr", stderr)):
            text = stream if isinstance(stream, str) else str(stream)
            print(f"\n===== {service} {label} =====")
            print(text)

    # Logs from the ZK JVM processes; the network-ns holders (zoo1..zoo3)
    # run no JVM, so their stdout/stderr carry nothing of interest.
    for service in ("zoo1-service", "zoo2-service", "zoo3-service"):
        _print_logs(service)


def _assemble_tls_keylog(
    workdir: pathlib.Path,
    auth: ZKAuthMode,
    features: tuple[ZKFeature, ...],
) -> list[pathlib.Path] | None:
    """Concatenate per-node TLS keylogs + context certs into ``captures/tls/``.

    On ``tls``+``capture`` runs the ensemble JVMs are launched with the
    ``extract-tls-secrets`` agent, which writes an SSLKEYLOGFILE-format
    secrets file into each node's ``/logs`` bind mount. This assembles those
    per-node keylogs (``logs/zk1|zk2|zk3/tls-secrets.log``) into
    ``captures/tls/zk-secrets.log`` and copies the server + CA certificates
    for context, so the pcapng artifacts can be decrypted. No private key is
    exported, and the keylog contents are never printed.

    Returns the emitted artifact paths, or ``None`` when this run has no
    keylog (capture inactive or auth is not ``tls``).
    """
    if ZKFeature.CAPTURE not in features or auth is not ZKAuthMode.TLS:
        return None

    tls_dir = workdir / "captures" / "tls"
    tls_dir.mkdir(parents=True, exist_ok=True)

    keylog = tls_dir / "zk-secrets.log"
    with keylog.open("wb") as out:
        for log_dir in ("zk1", "zk2", "zk3"):
            node_log = workdir / "logs" / log_dir / "tls-secrets.log"
            if node_log.is_file() and node_log.stat().st_size:
                out.write(node_log.read_bytes())
                out.write(b"\n")

    emitted: list[pathlib.Path] = []
    copies = {
        workdir / "certs" / "server" / "server.pem": "server-cert.pem",
        workdir / "certs" / "cacert.pem": "ca.pem",
    }
    for source, name in copies.items():
        if source.is_file():
            destination = tls_dir / name
            shutil.copyfile(source, destination)
            emitted.append(destination)

    if keylog.stat().st_size:
        emitted.insert(0, keylog)
    if emitted:
        return emitted
    return None


def _write_host_krb5_conf(
    workdir: pathlib.Path,
    kdc_host: str,
    kdc_port: int,
) -> pathlib.Path:
    """Write a host-view ``krb5.conf`` pointing at the published KDC port.

    The KDC sidecar writes a *server-view* config advertising
    ``kdc = kdc:1088``, resolvable only on the compose network. Host-side
    client processes cannot resolve the ``kdc`` compose service name, so this
    writes a config that points at the published host address/port instead.
    Returns the written file path.
    """
    host_krb5 = workdir / "krb5.client.conf"
    host_krb5.write_text(
        f"[libdefaults]\n"
        f" default_realm = EXAMPLE.ORG\n"
        f" dns_lookup_realm = false\n"
        f" rdns = false\n"
        f"[realms]\n"
        f" EXAMPLE.ORG = {{\n"
        f"  kdc = {kdc_host}:{kdc_port}\n"
        f" }}\n",
        encoding="utf-8",
    )
    return host_krb5


def _export_krb5_client_env(
    docker_env: KazooZkEnv,
    docker_compose: "DockerCompose",
) -> None:
    """Export the client-side Kerberos environment for the sasl_gssapi axis.

    The KDC publishes both TCP and UDP (``0:1088`` + ``0:1088/udp``) on the
    same host port; Docker Compose assigns both transports the same ephemeral
    port, so those bind a single published port. ``normalize().URL`` returns
    the publisher's bind address (``0.0.0.0`` / ``::`` on macOS/Linux), which
    host-side kinit cannot reach; clients must target the loopback interface
    where Docker publishes the port, so wildcard addresses fall back to
    ``127.0.0.1`` — exactly like the ensemble host resolution.

    The client is pointed at a *fresh per-run* FILE credential cache so a
    previous KDC instance's TGT/service ticket is never reused (each compose
    stack runs its own realm with new keys). ``KRB5_CLIENT_KTNAME`` points at
    the client keytab produced by the KDC sidecar, and ``KRB5_CONFIG`` at the
    host-view config written by :func:`_write_host_krb5_conf`.
    """
    from testcontainers.compose import PublishedPortModel

    container = docker_compose.get_container("kdc")
    publishers: list[PublishedPortModel] = (
        container.Publishers  # type: ignore[assignment]
    )
    tcp = [p for p in publishers if (p.Protocol or "").lower() == "tcp"]
    if not tcp:
        raise RuntimeError(
            "sasl_gssapi: no TCP publisher found for the KDC service; "
            "is docker-compose.auth-sasl-gssapi.yml being used?"
        )
    kdc_port = tcp[0].PublishedPort
    if kdc_port is None:
        raise RuntimeError(
            "sasl_gssapi: KDC published port was not found on TCP endpoint"
        )
    kdc_host = tcp[0].normalize().URL
    if not kdc_host or kdc_host in ("0.0.0.0", "::", "::1", "localhost"):
        kdc_host = "127.0.0.1"

    host_krb5 = _write_host_krb5_conf(
        docker_env.workdir, kdc_host, int(kdc_port)
    )
    os.environ["KRB5_CONFIG"] = str(host_krb5)
    os.environ["KRB5_CLIENT_KTNAME"] = str(
        docker_env.workdir / "keytabs" / "client.keytab"
    )

    # Fresh per-run FILE credential cache for the client. KRB5CCNAME must be a
    # FILE cache for a kinit -c target; do not inherit a stale API: cache
    # location or the default macOS shared cache.
    ccache = docker_env.workdir / f"krb5cc-{os.getpid()}"
    ccache.unlink(missing_ok=True)
    kinit_env = dict(os.environ)
    kinit_env.pop("KRB5CCNAME", None)
    kinit_rc = subprocess.run(
        [
            "kinit",
            "-c",
            str(ccache),
            "-kt",
            os.environ["KRB5_CLIENT_KTNAME"],
            "client@EXAMPLE.ORG",
        ],
        capture_output=True,
        text=True,
        env=kinit_env,
    ).returncode
    if kinit_rc != 0:
        raise RuntimeError(
            "sasl_gssapi: host-side kinit failed; KDC unreachable from "
            f"client context (rc={kinit_rc}). See {host_krb5} and the "
            "transport-format note in the common module docstring."
        )
    os.environ["KRB5CCNAME"] = f"FILE:{ccache}"
