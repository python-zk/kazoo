"""SASL authentication integration tests for the compose harness.

The harness provides the ``sasl_digest`` and ``sasl_gssapi`` auth axes with a
KDC sidecar replacing the host keytab dance, and the groups below map onto
those flavors:

* ``TestLegacySASLDigestAuthentication`` -- the legacy
  ``auth_data=[("sasl", "user:pass")]`` string form (the deprecated but still
  supported path in ``kazoo/client.py``).
* ``TestSASLDigestAuthentication`` -- the ``sasl_options`` ``DIGEST-MD5`` form.
* ``TestSASLGSSAPIAuthentication`` -- the ``GSSAPI`` mechanism over TLS.

On the SASL axes the ensemble enforces authentication for *every* session
(``enforce.auth.enabled=true`` + ``enforce.auth.schemes=sasl``), so an
unauthenticated client cannot even connect. Node-level isolation is therefore
tested the same way as in ``test_auth.py``: give a znode a SASL ACL for one
principal and assert a client authenticated under a *different* principal is
denied with ``NoAuthError``.
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
from kazoo.security import make_acl

from kazoo.tests.integ.test_auth import (
    _require_puresasl,
    _require_kerberos,
    _wait_until_unusable,
)


class TestLegacySASLDigestAuthentication:
    """Legacy ``auth_data=[("sasl", "user:pass")]`` string form."""

    @pytest.mark.zk_auth("sasl_digest")
    def test_connect_sasl_auth(self, zkensemble, zkchroot):
        _require_puresasl()
        username = "jaasuser"
        password = "jaas_password"

        acl = make_acl("sasl", credential=username, all=True)

        # The legacy string form: "sasl" scheme entries in auth_data are still
        # translated into DIGEST-MD5 options by the client (deprecated but
        # supported; see kazoo/client.py "Managing legacy SASL options").
        # Explicit sasl_options=None suppresses the axis's implied options so
        # the legacy auth_data path is exercised (and no conflict is raised).
        sasl_auth = "%s:%s" % (username, password)
        client = zkensemble.get_client(
            auth_data=[("sasl", sasl_auth)], sasl_options=None
        )
        client.start()
        try:
            client.ensure_path(zkchroot)
            path = f"{zkchroot}/legacy-sasl"
            client.create(path, b"data", acl=(acl,))
            # give ZK a chance to copy data to other node
            time.sleep(0.1)
            # A node protected by a SASL ACL for this principal is readable.
            data, _ = client.get(path)
            assert data == b"data"
        finally:
            client.delete(path, recursive=True)
            client.stop()
            client.close()

    @pytest.mark.zk_auth("sasl_digest")
    def test_invalid_sasl_auth(self, zkensemble):
        _require_puresasl()
        client = zkensemble.get_client(
            auth_data=[("sasl", "baduser:badpassword")], sasl_options=None
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


class TestSASLDigestAuthentication:
    """SASL DIGEST-MD5 via ``sasl_options``."""

    @pytest.mark.zk_auth("sasl_digest")
    def test_connect_sasl_auth(self, zkensemble, zkchroot):
        _require_puresasl()
        username = "jaasuser"
        password = "jaas_password"

        acl = make_acl("sasl", credential=username, all=True)

        client = zkensemble.get_client(
            sasl_options={
                "mechanism": "DIGEST-MD5",
                "username": username,
                "password": password,
            }
        )
        client.start()
        try:
            client.ensure_path(zkchroot)
            path = f"{zkchroot}/sasl-valid"
            client.create(path, b"data", acl=(acl,))
            time.sleep(0.1)
            data, _ = client.get(path)
            assert data == b"data"
        finally:
            client.delete(path, recursive=True)
            client.stop()
            client.close()

    @pytest.mark.zk_auth("sasl_digest")
    def test_acl_isolates_other_principal(self, zkensemble, zkchroot):
        """A SASL ACL for one principal bars other authenticated sessions."""
        _require_puresasl()
        client = zkensemble.get_client()  # implied sasl_options (jaasuser)
        client.start()
        try:
            client.ensure_path(zkchroot)
            # Protect a node with an ACL for an identity other than the one
            # this session authenticated under (jaasuser).
            alien_acl = make_acl(
                "sasl", credential="some_other_user", all=True
            )
            path = f"{zkchroot}/sasl-protected"
            client.create(path, b"secret", acl=(alien_acl,))
            # The authenticated SASL identity (jaasuser) does not satisfy the
            # "some_other_user" ACL, so reading is denied.
            with pytest.raises(NoAuthError):
                client.get(path)
        finally:
            client.stop()
            client.close()

    @pytest.mark.zk_auth("sasl_digest")
    def test_invalid_sasl_auth(self, zkensemble):
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


class TestSASLGSSAPIAuthentication:
    """SASL GSSAPI (Kerberos) over the sasl_gssapi axis."""

    @pytest.mark.zk_auth("sasl_gssapi")
    def test_connect_gssapi_auth(self, zkensemble, zkchroot):
        _require_puresasl()
        _require_kerberos()
        principal = "client@EXAMPLE.ORG"

        acl = make_acl("sasl", credential=principal, all=True)

        # Implied options on the sasl_gssapi axis: TLS certs + GSSAPI
        # mechanism; KRB5_CONFIG/KRB5CCNAME are set up by the harness (the
        # legacy kinit invocation is handled by the KDC sidecar).
        client = zkensemble.get_client()
        client.start()
        try:
            client.ensure_path(zkchroot)
            path = f"{zkchroot}/gssapi-valid"
            client.create(path, b"data", acl=(acl,))
            time.sleep(0.1)
            data, _ = client.get(path)
            assert data == b"data"
        finally:
            client.delete(path, recursive=True)
            client.stop()
            client.close()

    @pytest.mark.zk_auth("sasl_gssapi")
    def test_acl_isolates_other_principal(self, zkensemble, zkchroot):
        _require_puresasl()
        _require_kerberos()
        client = zkensemble.get_client()
        client.start()
        try:
            client.ensure_path(zkchroot)
            alien_acl = make_acl(
                "sasl", credential="alice@OTHER.ORG", all=True
            )
            path = f"{zkchroot}/gssapi-protected"
            client.create(path, b"secret", acl=(alien_acl,))
            # The authenticated GSSAPI principal (client@EXAMPLE.ORG) does not
            # satisfy the other principal's ACL.
            with pytest.raises(NoAuthError):
                client.get(path)
        finally:
            client.stop()
            client.close()

    @pytest.mark.zk_auth("sasl_gssapi")
    def test_invalid_gssapi_auth(self, zkensemble):
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
