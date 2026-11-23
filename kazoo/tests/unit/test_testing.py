"""Unit tests for the kazoo.testing harness modules.

Two test groups live here:

* ``TestImportSurface`` -- the module-layout contract: the names the
  integration suite imports must resolve from ``kazoo.testing.common`` and
  ``kazoo.testing.fixtures``, and the replaced modules must no longer be
  importable.
* Harness logic tests (axis resolution, marker evaluation, mount paths,
  ensemble helpers, compose-overlay selection, keylog assembly, capture
  probing) -- these exercise the pure functions in ``kazoo.testing.common``
  and never require a Docker engine or a live ZooKeeper.

The pure-function groups aim for 100% branch coverage of
``kazoo.testing.common``.
"""

from __future__ import annotations

import argparse
import importlib
import os
import pathlib
import subprocess
import sys
import threading
import types

import pytest

from kazoo.testing import common, fixtures


class TestImportSurface:
    """The ``kazoo.testing`` module layout contract.

    The integration suite imports fixtures, hooks, and a few helpers from the
    harness. Those names must stay importable from the two split modules, and
    the modules they replaced must be gone.
    """

    @pytest.mark.parametrize(
        "name",
        [
            "docker_env",
            "docker_compose",
            "zkensemble",
            "zkchroot",
            "zkclient",
            "zksuperadmin_client",
            "docker_compose_config",
            "pytest_addoption",
            "pytest_configure",
            "pytest_collection_modifyitems",
        ],
    )
    def test_fixtures_exports(self, name: str) -> None:
        module = importlib.import_module("kazoo.testing.fixtures")
        assert hasattr(module, name)

    @pytest.mark.parametrize(
        "name",
        [
            "ZKAuthMode",
            "ZKFeature",
            "ZK_DEFAULT_VERSION",
            "FEATURE_JVM_PROPERTIES",
            "AUTH_JVM_FLAGS",
            "KazooZkEnv",
            "ZkEnsemble",
            "_assemble_tls_keylog",
            "_evaluate_axis_markers",
        ],
    )
    def test_common_exports(self, name: str) -> None:
        module = importlib.import_module("kazoo.testing.common")
        assert hasattr(module, name)

    def test_harness_module_removed(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("kazoo.testing.harness")

    def test_kazoo_tests_conftest_removed(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("kazoo.tests.conftest")


class _FakeMarker:
    """Minimal stand-in for a pytest marker."""

    def __init__(self, args=(), kwargs=None):
        self.args = tuple(args)
        self.kwargs = dict(kwargs or {})


class _FakeItem:
    """Minimal item stand-in exposing get_closest_marker."""

    def __init__(self, markers=None):
        self._markers = markers or {}

    def get_closest_marker(self, name):
        return self._markers.get(name)


def _make_ensemble(
    auth: common.ZKAuthMode = common.ZKAuthMode.PLAIN,
    features: tuple = (common.ZKFeature.STANDARD,),
    workdir=None,
):
    return common.ZkEnsemble(
        zk_ip="127.0.0.1",
        zk1_port=2181,
        zk2_port=2182,
        zk3_port=2183,
        version="3.9.5",
        compose=None,
        workdir=workdir if workdir is not None else pathlib.Path("/tmp"),
        auth=auth,
        features=features,
    )


class TestResolveAxisOptions:
    """resolve_axis_options: env defaults, CLI overrides, parsing (T023)."""

    def test_env_defaults(self):
        version, auth, features, env = common.resolve_axis_options(
            None, None, None, {}
        )
        assert version == common.ZK_DEFAULT_VERSION
        assert auth is common.ZKAuthMode.PLAIN
        assert features == (common.ZKFeature.STANDARD,)
        assert env["KAZOO_TESTING_ZK_VERSION"] == common.ZK_DEFAULT_VERSION
        assert env["KAZOO_TESTING_ZK_AUTH"] == "plain"
        assert env["KAZOO_TESTING_ZK_FEATURES"] == "standard"
        assert env["KAZOO_TESTING_ZK_AUTH_JVMFLAGS"] == ""
        assert env["KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS"] == ""

    def test_environment_values(self):
        version, auth, features, env = common.resolve_axis_options(
            None,
            None,
            None,
            {
                "KAZOO_TESTING_ZK_VERSION": "3.8.6",
                "KAZOO_TESTING_ZK_AUTH": "digest",
                "KAZOO_TESTING_ZK_FEATURES": "ttl, reconfig",
            },
        )
        assert version == "3.8.6"
        assert auth is common.ZKAuthMode.DIGEST
        assert features == (common.ZKFeature.TTL, common.ZKFeature.RECONFIG)
        assert (
            env["KAZOO_TESTING_ZK_AUTH_JVMFLAGS"]
            == common.AUTH_JVM_FLAGS[common.ZKAuthMode.DIGEST]
        )

    def test_environment_unprefixed_fallback(self):
        version, auth, features, env = common.resolve_axis_options(
            None,
            None,
            None,
            {
                "ZK_VERSION": "3.8.6",
                "ZK_AUTH": "digest",
                "ZK_FEATURES": "ttl, reconfig",
            },
        )
        assert version == "3.8.6"
        assert auth is common.ZKAuthMode.DIGEST
        assert features == (common.ZKFeature.TTL, common.ZKFeature.RECONFIG)
        assert env["KAZOO_TESTING_ZK_VERSION"] == "3.8.6"
        assert env["KAZOO_TESTING_ZK_AUTH"] == "digest"
        assert env["KAZOO_TESTING_ZK_FEATURES"] == "ttl,reconfig"
        assert (
            env["KAZOO_TESTING_ZK_AUTH_JVMFLAGS"]
            == common.AUTH_JVM_FLAGS[common.ZKAuthMode.DIGEST]
        )

    def test_options_override_environment(self):
        version, auth, features, env = common.resolve_axis_options(
            "3.7.2",
            "tls",
            "capture",
            {
                "KAZOO_TESTING_ZK_VERSION": "3.9.5",
                "KAZOO_TESTING_ZK_AUTH": "plain",
                "KAZOO_TESTING_ZK_FEATURES": "standard",
            },
        )
        assert version == "3.7.2"
        assert auth is common.ZKAuthMode.TLS
        assert features == (common.ZKFeature.CAPTURE,)
        assert env["KAZOO_TESTING_ZK_VERSION"] == "3.7.2"
        assert env["KAZOO_TESTING_ZK_AUTH"] == "tls"
        assert env["KAZOO_TESTING_ZK_FEATURES"] == "capture"

    def test_empty_feature_segments_are_filtered(self):
        _version, _auth, features, env = common.resolve_axis_options(
            None, None, "ttl,,reconfig", {}
        )
        assert features == (common.ZKFeature.TTL, common.ZKFeature.RECONFIG)
        assert env["KAZOO_TESTING_ZK_FEATURES"] == "ttl,reconfig"

    def test_capture_jvmflags_only_for_tls_capture(self):
        _v, _a, features, env = common.resolve_axis_options(
            None, "tls", "capture", {}
        )
        assert features == (common.ZKFeature.CAPTURE,)
        assert env["KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS"] == (
            "-javaagent:/agent/extract-tls-secrets.jar=/logs/tls-secrets.log"
        )

    def test_capture_jvmflags_empty_without_tls(self):
        _v, _a, _features, env = common.resolve_axis_options(
            None, "plain", "capture", {}
        )
        assert env["KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS"] == ""


class _FakeConfig:
    """Stand-in pytest config exposing getoption."""

    def __init__(self, options=None):
        self._options = dict(options or {})

    def getoption(self, name):
        return self._options.get(name)


class TestResolveAxisOptionsWrapper:
    """_resolve_axis_options: pytest-option plumbing (T023a)."""

    _ENV_KEYS = (
        "KAZOO_TESTING_ZK_VERSION",
        "KAZOO_TESTING_ZK_AUTH",
        "KAZOO_TESTING_ZK_FEATURES",
        "KAZOO_TESTING_ZK_AUTH_JVMFLAGS",
        "KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS",
        "KAZOO_TESTING_ZK_CFG_EXTRA",
        "ZK_VERSION",
        "ZK_AUTH",
        "ZK_FEATURES",
    )

    def _env_snapshot(self):
        return {k: os.environ.get(k) for k in self._ENV_KEYS}

    def _env_restore(self, snapshot):
        for key, value in snapshot.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_options_wired_through(self):
        snapshot = self._env_snapshot()
        try:
            config = _FakeConfig(
                {
                    "--zk-version": "3.8.6",
                    "--zk-auth": "digest",
                    "--zk-features": "ttl,reconfig",
                }
            )
            version_value, auth, features = common._resolve_axis_options(
                config
            )
            assert version_value == "3.8.6"
            assert auth is common.ZKAuthMode.DIGEST
            assert features == (
                common.ZKFeature.TTL,
                common.ZKFeature.RECONFIG,
            )
            assert os.environ["KAZOO_TESTING_ZK_VERSION"] == "3.8.6"
            assert os.environ["KAZOO_TESTING_ZK_AUTH"] == "digest"
            assert os.environ["KAZOO_TESTING_ZK_FEATURES"] == "ttl,reconfig"
        finally:
            self._env_restore(snapshot)

    def test_env_fallback_when_options_absent(self, monkeypatch):
        snapshot = self._env_snapshot()
        try:
            monkeypatch.setenv("KAZOO_TESTING_ZK_VERSION", "3.6.4")
            monkeypatch.setenv("KAZOO_TESTING_ZK_AUTH", "tls")
            monkeypatch.setenv("KAZOO_TESTING_ZK_FEATURES", "capture")
            version_value, auth, features = common._resolve_axis_options(
                _FakeConfig()
            )
            assert version_value == "3.6.4"
            assert auth is common.ZKAuthMode.TLS
            assert features == (common.ZKFeature.CAPTURE,)
            assert (
                os.environ["KAZOO_TESTING_ZK_AUTH_JVMFLAGS"]
                == common.AUTH_JVM_FLAGS[common.ZKAuthMode.TLS]
            )
            assert os.environ["KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS"] != ""
        finally:
            self._env_restore(snapshot)


class TestMarkerEvaluation:
    """Axis-marker evaluation (T024)."""

    def _eval(self, item):
        return common._evaluate_axis_markers(
            item,
            "3.9.5",
            common.ZKAuthMode.DIGEST,
            (common.ZKFeature.TTL,),
        )

    def test_no_markers_returns_none(self):
        assert self._eval(_FakeItem()) is None

    def test_version_marker_hit_and_miss(self):
        item = _FakeItem({"zk_version": _FakeMarker(("<3.8",))})
        assert self._eval(item) == "Requires ZK <3.8 (active: 3.9.5)"
        item = _FakeItem({"zk_version": _FakeMarker(("<3.10",))})
        assert self._eval(item) is None

    def test_auth_allowed_and_skip(self):
        ok = _FakeItem({"zk_auth": _FakeMarker(("digest",))})
        assert self._eval(ok) is None
        forbidden = _FakeItem({"zk_auth": _FakeMarker(("tls",))})
        assert self._eval(forbidden) == (
            "Requires auth in ['tls'] (active: digest)"
        )
        skip_digest = _FakeItem(
            {"zk_auth": _FakeMarker(kwargs={"skip": ("digest",)})}
        )
        assert self._eval(skip_digest) == "Incompatible with auth digest"

    def test_features_require_and_skip(self):
        require_meta = {"require": ["ttl"]}
        item = _FakeItem({"zk_features": _FakeMarker(kwargs=require_meta)})
        assert self._eval(item) is None
        missing = {"require": ["readonly"]}
        item = _FakeItem({"zk_features": _FakeMarker(kwargs=missing)})
        assert self._eval(item) == "Missing required feature(s): ['readonly']"
        skip_meta = {"skip": ["ttl"]}
        item = _FakeItem({"zk_features": _FakeMarker(kwargs=skip_meta)})
        assert (
            self._eval(item) == "Incompatible with active feature(s): ['ttl']"
        )

    def test_multiple_reasons_joined(self):
        item = _FakeItem(
            {
                "zk_version": _FakeMarker((">=3.10",)),
                "zk_auth": _FakeMarker(("tls",)),
            }
        )
        assert self._eval(item) == (
            "Requires ZK >=3.10 (active: 3.9.5); "
            "Requires auth in ['tls'] (active: digest)"
        )


class TestDaemonMountPath:
    """Bind-mount path translation for remote daemons (T025)."""

    def test_posix_passthrough(self):
        path = pathlib.Path("/tmp/kazoo/work")
        assert (
            common._daemon_mount_path(path, os_name="posix", docker_host="")
            == "/tmp/kazoo/work"
        )

    def test_windows_tcp_drive_rewrite(self):
        path = pathlib.Path("D:/kazoo/work")
        out = common._daemon_mount_path(
            path, os_name="nt", docker_host="tcp://localhost:2375"
        )
        assert out == "/mnt/d/kazoo/work"

    def test_windows_http_drive_rewrite(self):
        path = pathlib.Path("C:/work")
        out = common._daemon_mount_path(
            path, os_name="nt", docker_host="http://engine:2375"
        )
        assert out == "/mnt/c/work"

    def test_windows_tcp_non_drive_passthrough(self):
        path = pathlib.Path("/mnt/c/x")
        out = common._daemon_mount_path(
            path, os_name="nt", docker_host="tcp://localhost:2375"
        )
        assert out == "/mnt/c/x"

    def test_windows_without_remote_host_passthrough(self):
        path = pathlib.Path("D:/kazoo/work")
        assert (
            common._daemon_mount_path(path, os_name="nt", docker_host="")
            == "D:/kazoo/work"
        )

    @pytest.mark.parametrize(
        "name,expected",
        [("zoo1", "zoo1-service"), ("zoo2", "zoo2-service")],
    )
    def test_process_service_members(self, name, expected):
        assert common.ZkEnsemble._process_service(name) == expected

    def test_process_service_passthrough(self):
        assert (
            common.ZkEnsemble._process_service("zoo1-service")
            == "zoo1-service"
        )


class TestZkEnsemble:
    """Ensemble client plumbing (T026)."""

    def test_get_hosts(self):
        ensemble = _make_ensemble()
        assert (
            ensemble.get_hosts()
            == "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
        )

    def test_implied_options_plain(self):
        assert (
            _make_ensemble(common.ZKAuthMode.PLAIN)._client_implied_options()
            == {}
        )

    def test_implied_options_digest(self):
        opts = _make_ensemble(
            common.ZKAuthMode.DIGEST
        )._client_implied_options()
        assert opts == {}

    def test_implied_options_sasl_digest(self):
        opts = _make_ensemble(
            common.ZKAuthMode.SASL_DIGEST
        )._client_implied_options()
        assert opts["sasl_options"]["mechanism"] == "DIGEST-MD5"

    def test_implied_options_tls(self, tmp_path):
        opts = _make_ensemble(
            common.ZKAuthMode.TLS, workdir=tmp_path
        )._client_implied_options()
        assert opts["use_ssl"] is True
        assert (
            str(tmp_path / "certs" / "client" / "client.pem") in opts.values()
        )
        assert "sasl_options" not in opts

    def test_implied_options_sasl_gssapi(self, tmp_path):
        opts = _make_ensemble(
            common.ZKAuthMode.SASL_GSSAPI, workdir=tmp_path
        )._client_implied_options()
        assert opts["use_ssl"] is True
        assert opts["sasl_options"] == {"mechanism": "GSSAPI"}

    def test_superadmin_auth_added(self):
        kwargs: dict = {}
        _make_ensemble()._apply_superadmin_auth(kwargs)
        assert kwargs == {"auth_data": [("digest", "super:super_secret")]}

    def test_superadmin_auth_appended(self):
        existing = [("digest", "other")]
        kwargs: dict = {"auth_data": existing}
        _make_ensemble()._apply_superadmin_auth(kwargs)
        assert kwargs["auth_data"] == [
            ("digest", "other"),
            ("digest", "super:super_secret"),
        ]

    def test_superadmin_auth_rejects_non_list(self):
        kwargs: dict = {"auth_data": "not-a-list"}
        with pytest.raises(ValueError):
            _make_ensemble()._apply_superadmin_auth(kwargs)

    def test_get_client_hosts_kwarg_wins(self):
        client = _make_ensemble().get_client(hosts="1.2.3.4:9999")
        assert client.hosts == [("1.2.3.4", 9999)]

    def test_get_client_default_hosts_and_implied_options(self):
        client = _make_ensemble(common.ZKAuthMode.DIGEST).get_client()
        assert client.hosts == [
            ("127.0.0.1", 2181),
            ("127.0.0.1", 2182),
            ("127.0.0.1", 2183),
        ]
        assert client.auth_data == set()

    def test_get_client_superadmin(self):
        client = _make_ensemble().get_client(superadmin=True)
        assert ("digest", "super:super_secret") in client.auth_data

    def test_set_compose_handle_roundtrip(self):
        common.set_compose_handle("fake")
        assert common._COMPOSE_HANDLE == "fake"
        common.set_compose_handle(None)
        assert common._COMPOSE_HANDLE is None


class TestResolveComposeFiles:
    """Compose overlay selection and mapping consistency (T027)."""

    _BASE = "docker-compose.base.yml"
    _CAPTURE = "docker-compose.features-capture.yml"

    def test_plain(self):
        assert common.resolve_compose_files(
            common.ZKAuthMode.PLAIN, (common.ZKFeature.STANDARD,)
        ) == [self._BASE]

    @pytest.mark.parametrize(
        "auth,overlay",
        [
            (common.ZKAuthMode.DIGEST, "docker-compose.auth-digest.yml"),
            (
                common.ZKAuthMode.SASL_DIGEST,
                "docker-compose.auth-sasl-digest.yml",
            ),
            (
                common.ZKAuthMode.SASL_GSSAPI,
                "docker-compose.auth-sasl-gssapi.yml",
            ),
            (common.ZKAuthMode.TLS, "docker-compose.auth-tls.yml"),
        ],
    )
    def test_auth_overlays(self, auth, overlay):
        assert common.resolve_compose_files(
            auth, (common.ZKFeature.STANDARD,)
        ) == [self._BASE, overlay]

    def test_capture_overlay(self):
        assert common.resolve_compose_files(
            common.ZKAuthMode.PLAIN, (common.ZKFeature.CAPTURE,)
        ) == [self._BASE, self._CAPTURE]

    def test_auth_and_capture_combo(self):
        assert common.resolve_compose_files(
            common.ZKAuthMode.TLS,
            (common.ZKFeature.STANDARD, common.ZKFeature.CAPTURE),
        ) == [self._BASE, "docker-compose.auth-tls.yml", self._CAPTURE]

    def test_capture_not_in_feature_jvm_properties(self):
        assert common.ZKFeature.CAPTURE not in common.FEATURE_JVM_PROPERTIES

    def test_auth_jvm_flags_cover_all_modes(self):
        assert set(common.AUTH_JVM_FLAGS) == set(common.ZKAuthMode)


class TestTlsKeylogAssembly:
    """Teardown keylog assembly (T028)."""

    def test_no_capture_returns_none(self, tmp_path):
        assert (
            common._assemble_tls_keylog(
                tmp_path,
                common.ZKAuthMode.TLS,
                (common.ZKFeature.STANDARD,),
            )
            is None
        )

    def test_capture_non_tls_returns_none(self, tmp_path):
        assert (
            common._assemble_tls_keylog(
                tmp_path,
                common.ZKAuthMode.PLAIN,
                (common.ZKFeature.CAPTURE,),
            )
            is None
        )

    def test_assembles_keylog_and_certs(self, tmp_path):
        workdir = tmp_path / "work"
        (workdir / "logs" / "zk1").mkdir(parents=True)
        (workdir / "logs" / "zk2").mkdir(parents=True)
        (workdir / "logs" / "zk1" / "tls-secrets.log").write_bytes(
            b"CLIENT_HANDSHAKE_TRAFFIC_SECRET 1\n"
        )
        (workdir / "logs" / "zk2" / "tls-secrets.log").write_bytes(b"")
        certs = workdir / "certs"
        (certs / "server").mkdir(parents=True)
        (certs / "server" / "server.pem").write_bytes(b"SERVER")
        (certs / "cacert.pem").write_bytes(b"CA")

        emitted = common._assemble_tls_keylog(
            workdir, common.ZKAuthMode.TLS, (common.ZKFeature.CAPTURE,)
        )
        assert emitted is not None
        keylog = workdir / "captures" / "tls" / "zk-secrets.log"
        assert keylog.exists()
        assert b"CLIENT_HANDSHAKE_TRAFFIC_SECRET" in keylog.read_bytes()
        assert (workdir / "captures" / "tls" / "server-cert.pem").is_file()
        assert (workdir / "captures" / "tls" / "ca.pem").is_file()
        assert emitted[0] == keylog
        assert len(emitted) == 3

    def test_empty_keylog_no_certs_returns_none(self, tmp_path):
        workdir = tmp_path / "work"
        workdir.mkdir()
        assert (
            common._assemble_tls_keylog(
                workdir, common.ZKAuthMode.TLS, (common.ZKFeature.CAPTURE,)
            )
            is None
        )


class TestKrb5Conf:
    """Host-view krb5.conf generation (T029)."""

    def test_writes_kdc_line(self, tmp_path):
        conf = common._write_host_krb5_conf(tmp_path, "127.0.0.1", 16888)
        assert conf == tmp_path / "krb5.client.conf"
        content = conf.read_text(encoding="utf-8")
        assert "default_realm = EXAMPLE.ORG" in content
        assert "kdc = 127.0.0.1:16888" in content


class TestBreakConnection:
    """lose_connection / expire_session / __break_connection (T030)."""

    def _fake_client(self, states):
        class _FakeHandler:
            event_object = threading.Event

        class _FakeClient:
            def __init__(self):
                self.handler = _FakeHandler()
                self.listener = None
                self.retried = False
                self.get_async = None

            def add_listener(self, fn):
                self.listener = fn

            def _call(self, event, arg):
                for state in states:
                    self.listener(state)

            def retry(self, fn, *args, **kwargs):
                self.retried = True

        return _FakeClient()

    @pytest.mark.parametrize(
        "method,states",
        [
            (
                "lose_connection",
                (
                    common.KazooState.CONNECTED,
                    common.KazooState.SUSPENDED,
                    common.KazooState.CONNECTED,
                ),
            ),
            (
                "expire_session",
                (
                    common.KazooState.CONNECTED,
                    common.KazooState.LOST,
                    common.KazooState.CONNECTED,
                ),
            ),
        ],
    )
    def test_happy_path(self, method, states):
        client = self._fake_client(states)
        getattr(_make_ensemble(), method)(client)
        assert client.retried is True

    @pytest.mark.parametrize("method", ["lose_connection", "expire_session"])
    def test_explicit_event_factory(self, method):
        expected = {
            "lose_connection": common.KazooState.SUSPENDED,
            "expire_session": common.KazooState.LOST,
        }[method]
        states = (
            common.KazooState.CONNECTED,
            expected,
            common.KazooState.CONNECTED,
        )
        client = self._fake_client(states)
        getattr(_make_ensemble(), method)(
            client, event_factory=threading.Event
        )
        assert client.retried is True


class _ImmediateEvent(threading.Event):
    def wait(self, timeout=None):
        return self.is_set()


class TestBreakConnectionTimeouts:
    """Timeout paths in __break_connection (T030)."""

    def _client(self, states):
        class _FakeClient:
            def __init__(self):
                self.listener = None
                self.get_async = None

            def add_listener(self, fn):
                self.listener = fn

            def _call(self, event, arg):
                for state in states:
                    self.listener(state)

        return _FakeClient()

    def test_lost_notification_timeout(self):
        client = self._client(())
        with pytest.raises(Exception, match="Failed to get notified"):
            _make_ensemble().lose_connection(
                client, event_factory=lambda: _ImmediateEvent()
            )

    def test_reconnect_timeout(self):
        client = self._client((common.KazooState.SUSPENDED,))
        with pytest.raises(Exception, match="Failed to see client reconnect"):
            _make_ensemble().lose_connection(
                client, event_factory=lambda: _ImmediateEvent()
            )


class _ComposeCommand:
    def __init__(self):
        self.compose_command_property = ["docker", "compose"]
        self.context = "/tmp/compose"

    def docker_compose_command(self):
        return ["docker", "compose"]


class TestRunCompose:
    """_run_compose / stop / start subprocess plumbing (T031)."""

    def _ensemble(self, compose=None):
        return common.ZkEnsemble(
            zk_ip="127.0.0.1",
            zk1_port=2181,
            zk2_port=2182,
            zk3_port=2183,
            version="3.9.5",
            compose=compose if compose is not None else _ComposeCommand(),
            workdir=pathlib.Path("/tmp"),
            auth=common.ZKAuthMode.PLAIN,
            features=(common.ZKFeature.STANDARD,),
        )

    def test_stop_start(self, monkeypatch):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append((cmd, kwargs.get("cwd")))

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        monkeypatch.setattr(
            common.ZkEnsemble, "_wait_service_exited", lambda *a, **kw: None
        )
        monkeypatch.setattr(
            common.ZkEnsemble, "_wait_service_healthy", lambda *a, **kw: None
        )
        ensemble = self._ensemble()
        ensemble.stop("zoo1")
        ensemble.start("zoo2")
        assert calls == [
            (["docker", "compose", "stop", "zoo1-service"], "/tmp/compose"),
            (["docker", "compose", "start", "zoo2-service"], "/tmp/compose"),
        ]

    def test_run_cooperative_subprocess_success(self):
        res = common._run_cooperative_subprocess(
            ["python3", "-c", "print('hello')"],
            capture_output=True,
            text=True,
            check=True,
        )
        assert res.returncode == 0
        assert res.stdout.strip() == "hello"

    def test_run_cooperative_subprocess_called_process_error(self):
        with pytest.raises(common.subprocess.CalledProcessError):
            common._run_cooperative_subprocess(
                ["python3", "-c", "import sys; sys.exit(2)"],
                check=True,
            )

    def test_run_cooperative_subprocess_gevent_handler(self, monkeypatch):
        class _FakeGeventHandler:
            name = "sequential_gevent_handler"

        called = []
        fake_gevent_subprocess = types.ModuleType("gevent.subprocess")

        def fake_run(cmd, cwd=None, check=True, **kwargs):
            called.append((cmd, cwd, check))
            return "gevent_result"

        fake_gevent_subprocess.run = fake_run  # type: ignore[attr-defined]
        fake_gevent_pkg = sys.modules.get("gevent", types.ModuleType("gevent"))
        monkeypatch.setattr(
            fake_gevent_pkg,
            "subprocess",
            fake_gevent_subprocess,
            raising=False,
        )
        monkeypatch.setitem(sys.modules, "gevent", fake_gevent_pkg)
        monkeypatch.setitem(
            sys.modules, "gevent.subprocess", fake_gevent_subprocess
        )

        res = common._run_cooperative_subprocess(
            ["echo", "hi"],
            cwd="/tmp",
            check=True,
            handler=_FakeGeventHandler(),
        )
        assert res == "gevent_result"
        assert called == [(["echo", "hi"], "/tmp", True)]

    def test_run_cooperative_subprocess_eventlet_handler(self, monkeypatch):
        class _FakeEventletHandler:
            name = "sequential_eventlet_handler"

        called = []
        fake_eventlet_subprocess = types.ModuleType(
            "eventlet.green.subprocess"
        )

        def fake_run(cmd, cwd=None, check=True, **kwargs):
            called.append((cmd, cwd, check))
            return "eventlet_result"

        fake_eventlet_subprocess.run = fake_run  # type: ignore[attr-defined]
        fake_eventlet_pkg = sys.modules.get(
            "eventlet", types.ModuleType("eventlet")
        )
        fake_green_pkg = sys.modules.get(
            "eventlet.green", types.ModuleType("eventlet.green")
        )
        monkeypatch.setattr(
            fake_green_pkg,
            "subprocess",
            fake_eventlet_subprocess,
            raising=False,
        )
        monkeypatch.setattr(
            fake_eventlet_pkg, "green", fake_green_pkg, raising=False
        )
        monkeypatch.setitem(sys.modules, "eventlet", fake_eventlet_pkg)
        monkeypatch.setitem(sys.modules, "eventlet.green", fake_green_pkg)
        monkeypatch.setitem(
            sys.modules,
            "eventlet.green.subprocess",
            fake_eventlet_subprocess,
        )

        res = common._run_cooperative_subprocess(
            ["echo", "hi"],
            cwd="/tmp",
            check=True,
            handler=_FakeEventletHandler(),
        )
        assert res == "eventlet_result"
        assert called == [(["echo", "hi"], "/tmp", True)]

    def test_wait_service_healthy(self):
        class _FakeContainer:
            def __init__(self):
                self.polls = 0

            @property
            def Health(self):
                self.polls += 1
                return "healthy" if self.polls >= 2 else "starting"

        class _FakeCompose:
            def __init__(self):
                self.container = _FakeContainer()

            def get_container(self, service):
                return self.container

        compose = _FakeCompose()
        ensemble = self._ensemble(compose=compose)
        sleep_calls = []

        class _FakeHandler:
            name = "fake"

            @staticmethod
            def sleep_func(duration):
                sleep_calls.append(duration)

        ensemble._wait_service_healthy("zoo1-service", handler=_FakeHandler())
        assert compose.container.polls == 2
        assert sleep_calls == [0.2]

    def test_wait_service_healthy_timeout(self):
        class _FakeContainer:
            @property
            def Health(self):
                return "starting"

        class _FakeCompose:
            def get_container(self, service):
                return _FakeContainer()

        compose = _FakeCompose()
        ensemble = self._ensemble(compose=compose)
        with pytest.raises(
            RuntimeError, match="did not reach 'healthy' state"
        ):
            ensemble._wait_service_healthy("zoo1-service", timeout=0.01)

    def test_wait_service_exited(self):
        class _FakeContainer:
            def __init__(self):
                self.polls = 0

            @property
            def State(self):
                self.polls += 1
                return "exited" if self.polls >= 2 else "running"

        class _FakeCompose:
            def __init__(self):
                self.container = _FakeContainer()

            def get_container(self, service, include_all=False):
                return self.container

        compose = _FakeCompose()
        ensemble = self._ensemble(compose=compose)
        sleep_calls = []

        class _FakeHandler:
            name = "fake"

            @staticmethod
            def sleep_func(duration):
                sleep_calls.append(duration)

        ensemble._wait_service_exited("zoo1-service", handler=_FakeHandler())
        assert compose.container.polls == 2
        assert sleep_calls == [0.2]

    def test_wait_service_exited_timeout(self):
        class _FakeContainer:
            @property
            def State(self):
                return "running"

        class _FakeCompose:
            def get_container(self, service, include_all=False):
                return _FakeContainer()

        compose = _FakeCompose()
        ensemble = self._ensemble(compose=compose)
        with pytest.raises(RuntimeError, match="did not exit"):
            ensemble._wait_service_exited("zoo1-service", timeout=0.01)


class _Proc:
    def __init__(self, stdout=""):
        self.stdout = stdout


class TestEnsureDockerAvailable:
    """Docker preflight checks and their failure modes (T032)."""

    def test_available(self, monkeypatch):
        def fake_run(*args, **kwargs):
            return _Proc("linux\n")

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        common._ensure_docker_available("/tmp")

    def test_missing_docker_cli(self, monkeypatch):
        def fake_run(*args, **kwargs):
            raise FileNotFoundError

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        with pytest.raises(RuntimeError, match="docker. CLI was not found"):
            common._ensure_docker_available("/tmp")

    def test_compose_plugin_missing(self, monkeypatch):
        def fake_run(*args, **kwargs):
            raise subprocess.CalledProcessError(1, ["docker", "compose"])

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        with pytest.raises(RuntimeError, match="Compose v2 plugin"):
            common._ensure_docker_available("/tmp")

    def test_daemon_not_running(self, monkeypatch):
        calls = []

        def fake_run(*args, **kwargs):
            calls.append(args[0])
            if len(calls) == 1:
                return _Proc("linux\n")
            raise FileNotFoundError

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        with pytest.raises(RuntimeError, match="daemon"):
            common._ensure_docker_available("/tmp")

    def test_windows_backend_skips(self, monkeypatch):
        def fake_run(*args, **kwargs):
            return _Proc("windows\n")

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        with pytest.raises(pytest.skip.Exception):
            common._ensure_linux_docker_backend()

    def test_ostype_probe_failure_tolerated(self, monkeypatch):
        def fake_run(*args, **kwargs):
            raise FileNotFoundError

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        common._ensure_linux_docker_backend()


class TestBuildCaptureImages:
    """In-repo capture image build preflight (T033)."""

    def test_success(self, monkeypatch):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        common._build_capture_images(_ComposeCommand(), "/tmp")
        assert calls == [["docker", "compose", "build"]]

    def test_failure_uses_stderr(self, monkeypatch):
        def fake_run(cmd, **kwargs):
            raise subprocess.CalledProcessError(1, cmd, stderr=b"boom\n")

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        with pytest.raises(RuntimeError, match="boom"):
            common._build_capture_images(_ComposeCommand(), "/tmp")

    def test_failure_falls_back_to_stdout(self, monkeypatch):
        def fake_run(cmd, **kwargs):
            raise subprocess.CalledProcessError(
                1, cmd, b"stdout-detail\n", b""
            )

        monkeypatch.setattr(common.subprocess, "run", fake_run)
        with pytest.raises(RuntimeError, match="stdout-detail"):
            common._build_capture_images(_ComposeCommand(), "/tmp")


class TestDumpEnsembleLogs:
    """Best-effort member log dump (T034)."""

    def test_no_handle_noop(self):
        common.set_compose_handle(None)
        common.dump_ensemble_logs()

    def test_bytes_and_str_streams(self, capsys):
        class _Fake:
            def get_logs(self, service):
                return (b"out bytes\n", "err text")

        common.set_compose_handle(_Fake())
        try:
            common.dump_ensemble_logs()
        finally:
            common.set_compose_handle(None)
        captured = capsys.readouterr()
        assert "zoo1-service stdout" in captured.out
        assert "out bytes" in captured.out
        assert "zoo1-service stderr" in captured.out
        assert "err text" in captured.out

    def test_get_logs_exception(self, capsys):
        class _Fake:
            def get_logs(self, service):
                raise RuntimeError("boom")

        common.set_compose_handle(_Fake())
        try:
            common.dump_ensemble_logs()
        finally:
            common.set_compose_handle(None)
        captured = capsys.readouterr()
        assert "failed to fetch logs" in captured.out


def _call_fixture(fixture, *args, **kwargs):
    """Invoke a @pytest.fixture-decorated function body directly."""
    return fixture.__wrapped__(*args, **kwargs)


class _MarkerConfig:
    def __init__(self):
        self.lines = []

    def addinivalue_line(self, group, line):
        self.lines.append((group, line))


class _RecordItem(_FakeItem):
    def __init__(self, markers=None):
        super().__init__(markers)
        self.added = []

    def add_marker(self, marker):
        self.added.append(marker)


class _TmpPathFactory:
    def __init__(self, path):
        self._path = path

    def getbasetemp(self):
        return self._path


class _NodeRequest:
    def __init__(self, nodeid):
        self.node = type("N", (), {"nodeid": nodeid})()


def _env_snapshot(keys):
    return {k: os.environ.get(k) for k in keys}


def _env_restore(snapshot):
    for key, value in snapshot.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


class TestFixtureHooks:
    """Pytest glue hooks (T035)."""

    def test_addoption_registers_axes(self):
        parser = pytest.Parser()
        fixtures.pytest_addoption(parser)
        namespace = argparse.Namespace()
        parser.parse_known_args(
            [
                "--zk-version",
                "3.6.4",
                "--zk-auth",
                "digest",
                "--zk-features",
                "ttl,reconfig",
            ],
            namespace,
        )
        assert namespace.zk_version == "3.6.4"
        assert namespace.zk_auth == "digest"
        assert namespace.zk_features == "ttl,reconfig"

    def test_auth_option_restricts_choices(self):
        parser = pytest.Parser()
        fixtures.pytest_addoption(parser)
        with pytest.raises((pytest.UsageError, SystemExit)):
            parser.parse_known_args(
                ["--zk-auth", "bogus"], argparse.Namespace()
            )

    def test_axis_options_absent_by_default(self):
        parser = pytest.Parser()
        fixtures.pytest_addoption(parser)
        namespace = argparse.Namespace()
        parser.parse_known_args([], namespace)
        assert namespace.zk_version is None
        assert namespace.zk_auth is None
        assert namespace.zk_features is None

    def test_configure_registers_markers(self):
        config = _MarkerConfig()
        fixtures.pytest_configure(config)
        groups = {group for group, _line in config.lines}
        assert groups == {"markers"}
        joined = "\n".join(line for _group, line in config.lines)
        for marker in (
            "zk_version(",
            "zk_auth(",
            "zk_features(",
        ):
            assert marker in joined

    def _axis_config(self):
        return _FakeConfig(
            {
                "--zk-version": "3.9.5",
                "--zk-auth": "digest",
                "--zk-features": "standard",
            }
        )

    def test_collection_modifyitems_skips_incompatible(self, monkeypatch):
        keys = (
            "KAZOO_TESTING_ZK_VERSION",
            "KAZOO_TESTING_ZK_AUTH",
            "KAZOO_TESTING_ZK_FEATURES",
            "KAZOO_TESTING_ZK_AUTH_JVMFLAGS",
            "KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS",
            "ZK_VERSION",
            "ZK_AUTH",
            "ZK_FEATURES",
        )
        snapshot = _env_snapshot(keys)
        try:
            incompatible = _RecordItem({"zk_version": _FakeMarker(("<3.8",))})
            compatible = _RecordItem()
            fixtures.pytest_collection_modifyitems(
                None, self._axis_config(), [incompatible, compatible]
            )
            assert len(incompatible.added) == 1
            assert compatible.added == []
        finally:
            _env_restore(snapshot)


class TestDockerEnvFixture:
    """Session env-var wiring (T036)."""

    _KEYS = (
        "KAZOO_TESTING_ZK_WORK_DIR",
        "KAZOO_TESTING_ZK_VERSION",
        "KAZOO_TESTING_ZK_AUTH",
        "KAZOO_TESTING_ZK_FEATURES",
        "KAZOO_TESTING_ZK_AUTH_JVMFLAGS",
        "KAZOO_TESTING_ZK_CAPTURE_JVMFLAGS",
        "COMPOSE_PROJECT_NAME",
    )

    def test_sets_environment_and_axis(self, tmp_path):
        snapshot = _env_snapshot(self._KEYS)
        try:
            config = _FakeConfig(
                {
                    "--zk-version": "3.8.6",
                    "--zk-auth": "digest",
                    "--zk-features": "ttl",
                }
            )
            gen = fixtures.docker_env.__wrapped__(
                config, _TmpPathFactory(tmp_path)
            )
            env = next(gen)
            assert isinstance(env, common.KazooZkEnv)
            assert env.version == "3.8.6"
            assert env.auth is common.ZKAuthMode.DIGEST
            assert env.features == (common.ZKFeature.TTL,)
            assert (
                os.environ["KAZOO_TESTING_ZK_WORK_DIR"] == tmp_path.as_posix()
            )
            assert os.environ["COMPOSE_PROJECT_NAME"].startswith("kazoo-")
            with pytest.raises(StopIteration):
                next(gen)
        finally:
            _env_restore(snapshot)

    def test_restores_environment_on_teardown(self, tmp_path):
        before = dict(os.environ)
        key = "KAZOO_TESTING_ZK_SENTINEL_TEST"
        os.environ[key] = "sentinel_val"
        before[key] = "sentinel_val"
        try:
            config = _FakeConfig(
                {
                    "--zk-version": "3.8.6",
                    "--zk-auth": "digest",
                    "--zk-features": "ttl",
                }
            )
            gen = fixtures.docker_env.__wrapped__(
                config, _TmpPathFactory(tmp_path)
            )
            next(gen)
            # Verify in-flight mutation occurred
            assert os.environ["KAZOO_TESTING_ZK_VERSION"] == "3.8.6"
            # Trigger teardown by advancing generator to completion
            with pytest.raises(StopIteration):
                next(gen)
            # Verify environment was exactly restored to its state before gen
            assert os.environ == before
        finally:
            os.environ.pop(key, None)


class TestDockerComposeConfigFixture:
    """Overlay + JVM-flags resolution (T037)."""

    def test_resolves_files_and_jvmflags(self, tmp_path, monkeypatch):
        env = common.KazooZkEnv(
            version="3.9.5",
            workdir=tmp_path,
            auth=common.ZKAuthMode.TLS,
            features=(common.ZKFeature.RECONFIG,),
        )
        monkeypatch.delenv("KAZOO_TESTING_ZK_FEATURES_JVMFLAGS", raising=False)
        result = _call_fixture(fixtures.docker_compose_config, env)
        assert result["version"] == "3.9.5"
        assert result["auth"] is common.ZKAuthMode.TLS
        assert result["features"] == (common.ZKFeature.RECONFIG,)
        assert result["compose_files"] == [
            "docker-compose.base.yml",
            "docker-compose.auth-tls.yml",
        ]
        assert os.environ["KAZOO_TESTING_ZK_FEATURES_JVMFLAGS"] == (
            "-Dzookeeper.reconfigEnabled=true"
        )


class TestZkChrootFixture:
    """Per-test chroot generation (T038)."""

    def test_unique_per_nodeid(self):
        chroot = _call_fixture(
            fixtures.zkchroot, _NodeRequest("tests/test_x/test_y")
        )
        assert chroot.startswith("/test_y-")
        assert len(chroot) == len("/test_y-") + 8


class _FakeClient:
    def __init__(self):
        self.started = False
        self.stopped = False
        self.closed = False
        self.paths: list[str] = []
        self.chroot: str | None = None
        self.handler = type("H", (), {"event_object": lambda: None})()

    def start(self):
        self.started = True

    def ensure_path(self, path):
        self.paths.append(path)

    def stop(self):
        self.stopped = True

    def close(self):
        self.closed = True


class _FakeEnsemble:
    def __init__(self):
        self.clients: list[_FakeClient] = []
        self.get_client_calls: list[dict[str, object]] = []

    def get_client(self, **kwargs):
        self.get_client_calls.append(kwargs)
        client = _FakeClient()
        self.clients.append(client)
        return client

    def expire_session(self, *args, **kwargs):
        pass


class TestZkClientFixtures:
    """Ensemble client fixtures lifecycle and chroot isolation."""

    def test_zkclient_lifecycle_and_chroot(self):
        ensemble = _FakeEnsemble()
        gen = _call_fixture(fixtures.zkclient, ensemble, "/test-chroot")
        client = next(gen)
        assert ensemble.get_client_calls == [{}]
        assert client.started is True
        assert client.paths == ["/test-chroot"]
        assert client.chroot == "/test-chroot"
        assert hasattr(client, "harness_expire_session")
        assert client.stopped is False
        assert client.closed is False
        with pytest.raises(StopIteration):
            next(gen)
        assert client.stopped is True
        assert client.closed is True

    def test_zksuperadmin_client_lifecycle_and_shared_chroot(self):
        ensemble = _FakeEnsemble()
        gen = _call_fixture(
            fixtures.zksuperadmin_client, ensemble, "/test-chroot"
        )
        client = next(gen)
        assert ensemble.get_client_calls == [{"superadmin": True}]
        assert client.started is True
        assert client.paths == ["/test-chroot"]
        assert client.chroot == "/test-chroot"
        assert client.stopped is False
        assert client.closed is False
        with pytest.raises(StopIteration):
            next(gen)
        assert client.stopped is True
        assert client.closed is True


class _FakePublisher:
    def __init__(self, protocol, port, url):
        self.Protocol = protocol
        self.PublishedPort = port
        self._url = url

    def normalize(self):
        return type("N", (), {"URL": self._url})()


class _FakeKdcCompose:
    def __init__(self, publishers):
        self._publishers = publishers

    def get_container(self, name):
        assert name == "kdc"
        return type("C", (), {"Publishers": self._publishers})()


class TestExportKrb5ClientEnv:
    """KDC host resolution + host-side kinit (T040)."""

    _KEYS = ("KRB5_CONFIG", "KRB5_CLIENT_KTNAME", "KRB5CCNAME")

    def _env(self, workdir):
        return common.KazooZkEnv(
            version="3.9.5",
            workdir=workdir,
            auth=common.ZKAuthMode.SASL_GSSAPI,
            features=(common.ZKFeature.STANDARD,),
        )

    def _kinit(self, rc=0):
        def fake_run(cmd, **kwargs):
            assert cmd[0] == "kinit"
            return type("P", (), {"returncode": rc})()

        return fake_run

    def test_success_wildcard_host_rewritten(self, tmp_path, monkeypatch):
        snapshot = _env_snapshot(self._KEYS)
        try:
            publisher = _FakePublisher("tcp", 16888, "0.0.0.0")
            monkeypatch.setattr(common.subprocess, "run", self._kinit(0))
            common._export_krb5_client_env(
                self._env(tmp_path), _FakeKdcCompose([publisher])
            )
            assert os.environ["KRB5_CONFIG"] == str(
                tmp_path / "krb5.client.conf"
            )
            assert os.environ["KRB5_CLIENT_KTNAME"] == str(
                tmp_path / "keytabs" / "client.keytab"
            )
            assert os.environ["KRB5CCNAME"] == (
                f"FILE:{tmp_path / ('krb5cc-' + str(os.getpid()))}"
            )
            conf = (tmp_path / "krb5.client.conf").read_text()
            assert "kdc = 127.0.0.1:16888" in conf
        finally:
            _env_restore(snapshot)

    def test_success_explicit_host_kept(self, tmp_path, monkeypatch):
        snapshot = _env_snapshot(self._KEYS)
        try:
            publisher = _FakePublisher("tcp", 16888, "kdc.local")
            monkeypatch.setattr(common.subprocess, "run", self._kinit(0))
            common._export_krb5_client_env(
                self._env(tmp_path), _FakeKdcCompose([publisher])
            )
            conf = (tmp_path / "krb5.client.conf").read_text()
            assert "kdc = kdc.local:16888" in conf
        finally:
            _env_restore(snapshot)

    def test_missing_protocol_is_non_tcp(self, tmp_path, monkeypatch):
        snapshot = _env_snapshot(self._KEYS)
        try:
            publisher = _FakePublisher(None, 16888, "0.0.0.0")
            monkeypatch.setattr(common.subprocess, "run", self._kinit(0))
            with pytest.raises(RuntimeError, match="no TCP publisher"):
                common._export_krb5_client_env(
                    self._env(tmp_path), _FakeKdcCompose([publisher])
                )
        finally:
            _env_restore(snapshot)

    def test_only_udp_publisher_raises(self, tmp_path, monkeypatch):
        snapshot = _env_snapshot(self._KEYS)
        try:
            publisher = _FakePublisher("udp", 16888, "0.0.0.0")
            monkeypatch.setattr(common.subprocess, "run", self._kinit(0))
            with pytest.raises(RuntimeError, match="no TCP publisher"):
                common._export_krb5_client_env(
                    self._env(tmp_path), _FakeKdcCompose([publisher])
                )
        finally:
            _env_restore(snapshot)

    def test_kinit_failure_raises(self, tmp_path, monkeypatch):
        snapshot = _env_snapshot(self._KEYS)
        try:
            publisher = _FakePublisher("tcp", 16888, "127.0.0.1")
            monkeypatch.setattr(common.subprocess, "run", self._kinit(1))
            with pytest.raises(RuntimeError, match="kinit failed"):
                common._export_krb5_client_env(
                    self._env(tmp_path), _FakeKdcCompose([publisher])
                )
        finally:
            _env_restore(snapshot)
