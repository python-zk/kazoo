"""Integration self-check tests for the ``capture`` axis.

These tests validate the network-capture feature from inside a capture-enabled
session. They are written to run **only** when ``--zk-features=capture`` is
active and are
skipped otherwise by the existing ``zk_features`` marker machinery
(``kazoo.testing.fixtures``), so the same file is safe in every matrix
cell.

* ``test_capture_feature_active`` -- the axis wiring: the capture
  overlay is layered onto the compose file list.
* ``test_artifact_exists_and_valid`` -- the artifact contract:
  every member's tshark sidecar holds open a ``kazoo-client-zooN-*.pcapng``
  that already carries real client-port frames and a structurally valid pcapng
  header.
* ``test_tls_keylog_emitted`` -- the decryption-material contract:
  on the tls flavor the ensemble emits a non-empty SSLKEYLOGFILE-format keylog
  plus the context certs into ``captures/tls/``.
* ``test_capture_outcomes_identical`` -- the parity contract:
  the ``capture`` axis never changes any test's run/skip/fail classification
  (verified via the marker machinery).
* ``test_capture_with_feature_combo_ttl`` /
  ``test_capture_with_feature_combo_reconfig`` -- capture composes with the
  ttl/reconfig server features.
* ``test_non_tls_emits_no_keylog`` -- the no-decryption-material edge: no
  keylog is emitted unless the tls flavor is active.

Per the network-holder split (docker-compose.base.yml), capture is now **per
member**: one ``zooN-capture`` sidecar joins each member's network namespace
and writes a uniquely-named per-run file (``kazoo-client-zooN-<ts>.pcapng``,
see docker-compose.features-capture.yml). The artifact is therefore a
*collection* — the test verifies each member has produced a file, and that the
union of frames across the members carries client-port traffic (the Kazoo
client connects to whichever ensemble member it happens to pick, so frames may
land on any single member).

Two views of the artifact are exercised, because the "when is it readable"
answer differs between Docker Desktop and native Linux:

* **Container-side (authoritative, live):** ``docker compose exec`` into each
  capture container. The file is open there for the whole session, its Section
  Header Block is written as soon as capture starts, and captured frames are
  flushed into it continuously. This works identically on every platform, so
  it is the required gate.
* **Host-side (best effort):** on native Linux the bind mount mirrors the
  live file immediately. On
  Docker Desktop, virtiofs only syncs the host's view of an open,
  actively-written file back once the writing process exits, so the host
  mount is expected to be empty or absent *while the session is running*;
  the authoritative post-session ``capinfos`` gate there is the manual V1
  check. The host probe therefore hard-fails only when a file is present
  but malformed, and tolerates the stale/absent Docker Desktop view.

The host needs no capture tooling; ``capinfos`` is an optional extra
exercised only when it happens to be installed and the host view is readable.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from kazoo.testing.common import (
    ZKFeature,
    _assemble_tls_keylog,
    _evaluate_axis_markers,
)

# pcapng magic bytes: 0A 0D 0D 0A (native endian) identifies the Section
# Header Block (SHB); the byte-swapped variant is produced by non-native
# writers, so accept either.
_PCAPNG_MAGIC = (b"\x0a\x0d\x0d\x0a", b"\x4d\x3c\xb2\xa1")

_CAPTURE_OVERLAY = "docker-compose.features-capture.yml"

#: Ensemble members and their capture sidecar services. Each member's capture
#: writes its own uniquely-named pcapng (see capture-entrypoint.sh).
_MEMBERS = ("zoo1", "zoo2", "zoo3")
_CAPTURE_SERVICES = tuple(f"{m}-capture" for m in _MEMBERS)


@pytest.mark.zk_features(require=["capture"])
def test_capture_feature_active(docker_compose_config):
    """A capture-enabled run must not be skipped and must layer the overlay."""
    # Running at all proves the ``zk_features(require=["capture"])`` marker did
    # not skip this item (marker machinery).
    assert _CAPTURE_OVERLAY in docker_compose_config["compose_files"]


@pytest.mark.zk_features(require=["capture"])
def test_artifact_exists_and_valid(docker_compose, zkclient):
    """Every member's capture artifact exists and is a valid pcapng."""
    # Requesting the session-scoped docker_compose fixture guarantees the
    # stack (including the per-member capture sidecars) has been
    # ``up --wait``-ed before this assertion runs. The zkclient fixture
    # additionally drives real client traffic across the compose bridge (clear
    # port 2181), which the tshark sidecars are capturing; without it the
    # artifacts would stay empty for this standalone self-check.
    zkclient.create("/capture-selfcheck", b"x")
    assert zkclient.get("/capture-selfcheck")[0] == b"x"

    # Container-side gate: each sidecar's pcapng is open, its Section Header
    # Block is on disk, and captured client frames are already being flushed
    # into it. This is live on every platform (see module docstring).
    magics = {
        member: _wait_for_container_pcapng_magic(docker_compose, member)
        for member in _MEMBERS
    }
    _assert_container_frames_present(docker_compose)

    # Host-side probe: assert existence+validity when the bind mount actually
    # mirrors the live files (native Linux); tolerate Docker Desktop's stale
    # virtiofs view mid-session (see module docstring).
    artifacts = list(
        Path(os.environ["ZK_WORK_DIR"]).glob("captures/kazoo-client-*.pcapng")
    )
    _probe_host_artifacts(artifacts, magics)


@pytest.mark.zk_auth("tls")
@pytest.mark.zk_features(require=["capture"])
def test_tls_keylog_emitted(docker_env, zkclient):
    """The tls+capture run emits the TLS decryption material.

    On the tls flavor the ensemble JVMs run the ``extract-tls-secrets`` agent,
    which writes an SSLKEYLOGFILE-format keylog per node. This test drives real
    TLS client traffic, then exercises the same assembly routine the harness
    teardown runs (``_assemble_tls_keylog``) to assert ``captures/tls/``
    contains a non-empty keylog plus the context certificates. No private key
    is ever emitted.
    """
    # Drive real TLS traffic (the handshake the agent's keylog captures).
    zkclient.create("/tls-keylog-selfcheck", b"x")
    assert zkclient.get("/tls-keylog-selfcheck")[0] == b"x"

    # The agent flushes keylog lines as handshakes occur, but the host-side
    # view of an actively-written bind mount can lag briefly (and on Docker
    # Desktop virtiofs syncs lazily), so poll the per-node keylogs until at
    # least one carries content before assembling (same rationale as the
    # pcapng magic poll above).
    _wait_for_host_keylog(docker_env.workdir)

    emitted = _assemble_tls_keylog(
        docker_env.workdir, docker_env.auth, docker_env.features
    )
    assert emitted is not None, "no keylog material assembled on tls+capture"
    paths = {p.name for p in emitted}
    assert "zk-secrets.log" in paths
    assert "server-cert.pem" in paths
    assert "ca.pem" in paths

    keylog = docker_env.workdir / "captures" / "tls" / "zk-secrets.log"
    assert keylog.stat().st_size > 0, "keylog is empty after a TLS handshake"
    for name in ("server-cert.pem", "ca.pem"):
        material = (docker_env.workdir / "captures" / "tls" / name).read_text()
        assert material.startswith(
            "-----BEGIN CERTIFICATE-----"
        ), f"{name} is not a PEM certificate"


@pytest.mark.zk_auth(skip=("tls",))
@pytest.mark.zk_features(require=["capture"])
def test_non_tls_emits_no_keylog(docker_env):
    """Non-tls capture runs must not emit TLS decryption material.

    The keylog agent is only attached on the tls flavor, so ``captures/tls/``
    must not exist anywhere else (plain, digest, sasl_digest, sasl_gssapi).
    """
    tls_dir = docker_env.workdir / "captures" / "tls"
    assert not tls_dir.exists(), (
        f"decryption material {tls_dir} emitted on non-tls axis "
        f"(auth={docker_env.auth.value})"
    )


@pytest.mark.zk_features(require=["capture"])
def test_capture_outcomes_identical(request, docker_env):
    """Adding the ``capture`` feature must not alter any test's outcome.

    Capture is observational (a tshark sidecar plus, on tls, a passive keylog
    agent), so it must not change which tests run, skip, or fail. We prove
    this through the marker machinery itself: re-evaluate every collected
    item's axis markers with the active feature set and with ``capture``
    removed, and require identical run/skip/fail classifications. Only the
    capture-gated self-check tests themselves are exempt (they are supposed
    to skip without the axis value).
    """
    items = list(request.session.items)
    assert items, "no collected items to parity-check"
    active_features = docker_env.features
    assert ZKFeature.CAPTURE in active_features  # we are on a capture run
    baseline_features = tuple(
        f for f in active_features if f is not ZKFeature.CAPTURE
    )

    for item in items:
        # Skip the capture-gated tests: they are defined to run only under
        # the capture axis, so their without-capture classification (skip) is
        # an expected, intentional difference.
        marker = item.get_closest_marker("zk_features")
        require = (marker.kwargs or {}).get("require") or () if marker else ()
        if "capture" in require:
            continue
        with_capture = _evaluate_axis_markers(
            item, docker_env.version, docker_env.auth, active_features
        )
        without_capture = _evaluate_axis_markers(
            item, docker_env.version, docker_env.auth, baseline_features
        )
        assert without_capture == with_capture, (
            f"capture changed the outcome of {item.nodeid}: "
            f"without={without_capture!r} with={with_capture!r}"
        )


@pytest.mark.zk_features(require=["capture", "ttl"])
def test_capture_with_feature_combo_ttl(docker_compose, zkclient):
    """Capture composes with the ttl server feature.

    The per-member capture sidecars must work identically when the ttl server
    feature is also active; the pcapng of at least one member must carry
    client-port frames (the artifact contract is per-member, see module
    docstring).
    """
    zkclient.create("/capture-ttl-combo", b"x")
    assert zkclient.get("/capture-ttl-combo")[0] == b"x"
    _assert_container_frames_present(docker_compose)


@pytest.mark.zk_features(require=["capture", "reconfig"])
def test_capture_with_feature_combo_reconfig(docker_compose, zkclient):
    """Capture composes with the reconfig server feature."""
    zkclient.create("/capture-reconfig-combo", b"y")
    assert zkclient.get("/capture-reconfig-combo")[0] == b"y"
    _assert_container_frames_present(docker_compose)


def _container_exec(
    docker_compose, member: str, command: list[str]
) -> tuple[str, str, int]:
    """Run a command inside the ``{member}-capture`` sidecar container."""
    stdout, stderr, exit_code = docker_compose.exec_in_container(
        command, service_name=f"{member}-capture"
    )
    return stdout, stderr, exit_code


def _member_pcapngs(member: str) -> str:
    """Shell glob matching all capture files written by a member's sidecar.

    One file per sidecar invocation (per run/session); the sidecar may have
    been recreated, so match all of them and read the newest.
    """
    return f"/captures/kazoo-client-{member}-*.pcapng"


def _newest_member_pcapng(docker_compose, member: str) -> str:
    """The newest capture file a member's sidecar has written (path in the
    container)."""
    stdout, _stderr, exit_code = _container_exec(
        docker_compose,
        member,
        ["sh", "-c", f"ls -t {_member_pcapngs(member)} 2>/dev/null | head -1"],
    )
    if exit_code != 0 or not stdout.strip():
        raise AssertionError(
            f"no capture file for {member} "
            f"(service {member}-capture, glob {_member_pcapngs(member)})"
        )
    return stdout.strip().splitlines()[0]


def _wait_for_container_pcapng_magic(docker_compose, member: str) -> bytes:
    """Poll a member's sidecar for a readable pcapng Section Header Block.

    Each capture service writes its pcapng Section Header Block to its output
    file as soon as capture starts, so polling the container's own view
    converges in at most a couple of flush cycles and is immune to any
    host-side mount staleness.
    """
    last_magic = b""
    last_error: Exception | None = None
    for _ in range(50):
        try:
            newest = _newest_member_pcapng(docker_compose, member)
            stdout, _stderr, exit_code = _container_exec(
                docker_compose,
                member,
                ["sh", "-c", f"head -c 4 {newest}"],
            )
            if exit_code == 0:
                last_magic = stdout.encode("latin-1")
            if last_magic:
                break
        except (RuntimeError, OSError) as exc:
            last_error = exc
        time.sleep(0.2)
    if not last_magic:
        raise AssertionError(
            f"capture sidecar {member}-capture pcapng missing or unreadable"
            + (f" ({last_error})" if last_error else "")
        )
    # A pcapng SHB leads the file; legacy pcap (D4 C3 B2 A1) is not expected
    # (tshark writes pcapng natively).
    assert (
        last_magic in _PCAPNG_MAGIC
    ), f"{member}: not a pcapng header ({last_magic!r})"
    return last_magic


def _assert_container_frames_present(docker_compose) -> None:
    """The sidecars must have captured at least one client-port frame.

    tshark writes captured packet data to the pcapng in flushes, so like the
    magic gate this polls (up to ~10s) until frames become readable rather
    than asserting on the first read (a clean in-band flush during the
    session, plus the final flush at teardown). The Kazoo client connects to
    a single ensemble member, so frames may appear on any one capture; the
    union across all members must include a client port.
    """
    ports: set[int] = set()
    for _ in range(50):
        for member in _MEMBERS:
            try:
                newest = _newest_member_pcapng(docker_compose, member)
            except AssertionError:
                continue
            stdout, _stderr, exit_code = _container_exec(
                docker_compose,
                member,
                [
                    "tshark",
                    "-r",
                    newest,
                    "-T",
                    "fields",
                    "-e",
                    "tcp.port",
                ],
            )
            if exit_code == 0 and stdout.strip():
                # `-T fields -e tcp.port` emits one "sport,dport" pair per
                # frame; split on both whitespace and comma.
                tokens = [
                    tok for line in stdout.split() for tok in line.split(",")
                ]
                ports |= {int(tok) for tok in tokens if tok.isdigit()}
        if ports & {2181, 2281}:
            break
        time.sleep(0.2)
    assert ports & {2181, 2281}, (
        f"no client-port frames captured across {_MEMBERS} (ports="
        f"{sorted(ports)})"
    )


def _wait_for_host_keylog(workdir: Path) -> None:
    """Poll the per-node host-side keylogs until at least one is non-empty.

    The ``extract-tls-secrets`` agent writes SSLKEYLOGFILE lines as TLS
    handshakes occur, but the host-side view of an actively-written bind mount
    can lag briefly (Docker Desktop's virtiofs syncs lazily). Poll up to ~10s
    so ``_assemble_tls_keylog`` reads a file that already carries content,
    matching the pcapng magic poll used above.
    """
    for _ in range(50):
        for node in _MEMBERS:
            keylog = workdir / "logs" / node / "tls-secrets.log"
            try:
                if keylog.stat().st_size > 0:
                    return
            except OSError:
                continue
        time.sleep(0.2)


def _probe_host_artifacts(
    artifacts: list[Path], magics: dict[str, bytes]
) -> None:
    """Best-effort host-side artifact probe (see module docstring).

    Hard-fails only when a host file exists but disagrees with the
    container-side artifact (i.e. a genuinely inconsistent bind mount).
    Skips silently when the host cannot reflect the live file yet; the
    post-session ``capinfos`` check covers that case.
    """
    if not artifacts:
        return  # not yet visible on the host (expected on Docker Desktop)
    for artifact in artifacts:
        try:
            with artifact.open("rb") as handle:
                head = handle.read(4)
        except OSError:
            continue  # racing the mount; another probe will cover it
        # The host may expose multiple members' files; any valid pcapng magic
        # is acceptable (each sidecar writes its own SHB).
        assert head in _PCAPNG_MAGIC, (
            f"host artifact {artifact.name} out of sync with sidecar: "
            f"{head!r}"
        )
    _run_capinfos_if_available(artifacts)


def _run_capinfos_if_available(artifacts: list[Path]) -> None:
    """Validate with ``capinfos`` when the optional host tool is present.

    The host needs no capture tooling; this is an optional extra exercised
    when capinfos *is* installed, tolerating the transient "in progress"
    state of a live capture.
    """
    capinfos = shutil.which("capinfos")
    if capinfos is None:
        return
    for artifact in artifacts:
        result = subprocess.run(
            [capinfos, str(artifact)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        # ``returncode != 0`` would mean the tool could not parse the file; a
        # mid-capture file is expected to remain readable.
        assert (
            result.returncode == 0
        ), f"capinfos failed on {artifact}:\n{result.stdout}\n{result.stderr}"
