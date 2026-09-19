"""Authentication integration tests for the auth axis.

Each auth flavor (digest, sasl_digest, tls, sasl_gssapi) is exercised with:

* a *positive* case: valid credentials authenticate and the client can create
  and read a node;
* a *negative* case: invalid credentials are rejected by the server.

Tests are gated with the ``zk_auth`` marker so they only run under the flavor
they exercise; incompatible runs are skipped at collection time.

Notes on negative SASL assertions:

* ZooKeeper enforces SASL authentication via ``enforce.auth.enabled=true`` +
  ``enforce.auth.schemes=sasl`` (the legacy ``requireClientAuthScheme`` key is
  not recognized by ZK 3.7+). When the server rejects a client it returns the
  -124 error which kazoo maps to :class:`SessionClosedRequireSaslError`.
* ``client.start()`` may return before the SASL failure is processed (the
  session is marked CONNECTED before the SASL exchange completes), so the
  negative tests wait for the session to become unusable instead of asserting
  exclusively on ``start()`` raising.
"""

from __future__ import annotations

import time

import pytest

from kazoo.exceptions import (
    AuthFailedError,
    ConnectionClosedError,
    ConnectionLoss,
    NoAuthError,
    SessionClosedRequireSaslError,
)
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import KazooState
from kazoo.security import make_digest_acl


def _require_puresasl():
    """Skip unless the pure-sasl library (client-side SASL) is installed."""
    try:
        import puresasl  # noqa: F401
    except ImportError:
        pytest.skip("pure-sasl not installed; SASL mechanisms unavailable")


def _require_kerberos():
    """Skip unless the pykerberos module (GSSAPI mechanism) is installed."""
    try:
        import kerberos  # noqa: F401
    except ImportError:
        pytest.skip("pykerberos not installed; GSSAPI unavailable")


def _wait_until_unusable(client, timeout=10.0):
    """Wait until a client's session is no longer usable.

    Returns once the client is either not connected or in a LOST state, or
    raises ``AssertionError`` after ``timeout`` seconds. Used by the negative
    tests because ``client.start()`` can return before an authentication
    rejection is processed by the connection loop.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not client.connected or client.state == KazooState.LOST:
            return
        time.sleep(0.1)
    raise AssertionError(
        f"client did not become unusable within {timeout}s "
        f"(connected={client.connected}, state={client.state})"
    )


class TestDigestAuthentication:
    """Positive/negative coverage for the digest flavor."""

    @pytest.mark.zk_auth("digest")
    def test_valid_credentials_authenticate(self, zksuperadmin_client):
        zksuperadmin_client.create("/digest-valid", b"data")
        data, _ = zksuperadmin_client.get("/digest-valid")
        assert data == b"data"

    @pytest.mark.zk_auth("digest")
    def test_superadmin_bypasses_acl(self, zkclient, zksuperadmin_client):
        acl = make_digest_acl("owner", "owner_secret", all=True)
        zksuperadmin_client.create("/digest-protected", b"secret", acl=(acl,))
        with pytest.raises(NoAuthError):
            zkclient.get("/digest-protected")
        data, _ = zksuperadmin_client.get("/digest-protected")
        assert data == b"secret"

    @pytest.mark.zk_auth("digest")
    def test_invalid_credentials_rejected(self, zkensemble, zkchroot):
        # Digest credentials are validated lazily on ACL checks: a client with
        # wrong credentials connects, but cannot access a node protected by a
        # digest ACL it does not satisfy.
        acl = make_digest_acl("owner", "owner_secret", all=True)
        owner = zkensemble.get_client(
            auth_data=[("digest", "owner:owner_secret")]
        )
        bad = zkensemble.get_client(auth_data=[("digest", "bad:bad")])
        owner.start()
        bad.start()
        try:
            owner.ensure_path(zkchroot)
            path = f"{zkchroot}/digest-protected"
            owner.create(path, b"secret", acl=(acl,))
            # The owner (correct credentials) can read the protected node.
            data, _ = owner.get(path)
            assert data == b"secret"
            # The imposter (wrong credentials) is rejected on ACL check.
            with pytest.raises(NoAuthError):
                bad.get(path)
        finally:
            owner.stop()
            owner.close()
            bad.stop()
            bad.close()


class TestSASLDigestAuthentication:
    """Positive/negative coverage for the sasl_digest flavor."""

    @pytest.mark.zk_auth("sasl_digest")
    def test_valid_credentials_authenticate(self, zkensemble, zkchroot):
        _require_puresasl()
        client = zkensemble.get_client()  # implied sasl_options (jaasuser)
        client.start()
        try:
            client.ensure_path(zkchroot)
            path = f"{zkchroot}/sasl-valid"
            client.create(path, b"data")
            data, _ = client.get(path)
            assert data == b"data"
        finally:
            client.stop()
            client.close()

    @pytest.mark.zk_auth("sasl_digest")
    def test_invalid_credentials_rejected(self, zkensemble):
        _require_puresasl()
        client = zkensemble.get_client(
            sasl_options={
                "mechanism": "DIGEST-MD5",
                "username": "baduser",
                "password": "badpassword",
            }
        )
        try:
            client.start(timeout=5)
        except (
            AuthFailedError,
            SessionClosedRequireSaslError,
            KazooTimeoutError,
        ):
            # The rejection surfaced synchronously from start().
            client.stop()
            client.close()
            return

        # start() returned before the SASL failure was processed; the session
        # must nevertheless not be usable.
        try:
            _wait_until_unusable(client)
            with pytest.raises(
                (AuthFailedError, ConnectionClosedError, ConnectionLoss)
            ):
                client.get("/")
        finally:
            client.stop()
            client.close()


class TestTLSAuthentication:
    """Positive/negative coverage for the tls flavor."""

    @pytest.mark.zk_auth("tls")
    def test_valid_credentials_authenticate(self, zkensemble, zkchroot):
        client = zkensemble.get_client()  # implied use_ssl + client certs
        client.start()
        try:
            client.ensure_path(zkchroot)
            path = f"{zkchroot}/tls-valid"
            client.create(path, b"data")
            data, _ = client.get(path)
            assert data == b"data"
        finally:
            client.stop()
            client.close()

    @pytest.mark.zk_auth("tls")
    def test_invalid_certificate_rejected(self, zkensemble):
        # Connect over the TLS port without a client certificate: the server
        # requires mutual TLS (ssl.clientAuth=need) and must refuse the
        # handshake, so the session can never be established.
        client = zkensemble.get_client(
            use_ssl=True,
            certfile=None,
            keyfile=None,
            ca=None,
        )
        try:
            client.start(timeout=5)
        except (ConnectionLoss, KazooTimeoutError, ConnectionClosedError):
            # Handshake refused as expected.
            client.stop()
            client.close()
            return

        try:
            _wait_until_unusable(client)
            with pytest.raises(
                (ConnectionLoss, ConnectionClosedError, AuthFailedError)
            ):
                client.get("/")
        finally:
            client.stop()
            client.close()


class TestSASLGSSAPIAuthentication:
    """Positive/negative coverage for the sasl_gssapi flavor."""

    @pytest.mark.zk_auth("sasl_gssapi")
    def test_valid_credentials_authenticate(self, zkensemble, zkchroot):
        _require_puresasl()
        _require_kerberos()
        client = zkensemble.get_client()  # implied use_ssl + GSSAPI + KRB5 env
        client.start()
        try:
            client.ensure_path(zkchroot)
            path = f"{zkchroot}/gssapi-valid"
            client.create(path, b"data")
            data, _ = client.get(path)
            assert data == b"data"
        finally:
            client.stop()
            client.close()

    @pytest.mark.zk_auth("sasl_gssapi")
    def test_invalid_credentials_rejected(self, zkensemble):
        _require_puresasl()
        _require_kerberos()
        # A GSSAPI exchange requires a valid TGT for the requested service;
        # pointing the client at a nonexistent service cannot authenticate.
        client = zkensemble.get_client(
            sasl_options={"mechanism": "GSSAPI", "service": "nosuchsvc"}
        )
        try:
            client.start(timeout=5)
        except (
            AuthFailedError,
            SessionClosedRequireSaslError,
            KazooTimeoutError,
            ConnectionLoss,
        ):
            client.stop()
            client.close()
            return

        try:
            _wait_until_unusable(client)
            with pytest.raises(
                (AuthFailedError, ConnectionClosedError, ConnectionLoss)
            ):
                client.get("/")
        finally:
            client.stop()
            client.close()
