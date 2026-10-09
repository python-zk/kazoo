.. _testing:

=======
Testing
=======

Kazoo's own integration test suite uses a Docker-Compose based test harness
that is also exposed as public API for use in your own tests. The harness
starts a real ZooKeeper ensemble in containers (`zookeeper` image, ≥ 3.7),
waits for it to become healthy, and provides pytest fixtures that give you a
connected :class:`~kazoo.client.KazooClient`.

Requirements
============

* A `docker compose` compatible CLI (v2.12+) with a running Docker daemon.
* Python 3.9+ (Python 3.8 support was dropped).
* No ZooKeeper binary, Java, keytool, or local ZK classpath is required —
  everything runs in containers.

Install the project with the test extras:

.. code-block:: bash

    pip install -e '.[test]'

The harness is implemented in :mod:`kazoo.testing.common` (business logic)
and :mod:`kazoo.testing.fixtures` (pytest fixtures and plugin hooks). The
`legacy <https://docs.python.org/3/library/unittest.html>`_ ``KazooTestHarness`` /
``KazooTestCase`` API was removed; use the pytest fixtures below instead
(see `CHANGES.md <https://github.com/python-zk/kazoo/blob/main/CHANGES.md>`_
under BREAKING CHANGES).

Entry point
===========

The :class:`~kazoo.testing.common.ZkEnsemble` class and the fixtures
it provides are registered as a pytest plugin. To use the harness in your own
``pytest`` suite, import the fixtures from :mod:`kazoo.testing.fixtures` in
your ``conftest.py`` (fixtures are plain functions that receive the ensemble):

.. code-block:: python

    from kazoo.testing.fixtures import zkensemble, zkclient, zkchroot

Fixtures
========

``zkensemble``
    Session-scoped. Starts a three-node ZooKeeper ensemble via
    ``docker compose up --wait`` and tears it down (``down --volumes``) at
    session end. Yields a :class:`~kazoo.testing.common.ZkEnsemble`
    that can create and manage clients, stop/start individual ensemble
    members (for failure-injection tests), and expose the resolved axis
    configuration.

``zkclient``
    Function-scoped. A started :class:`~kazoo.client.KazooClient` connected
    to the ensemble. The connection options implied by the active
    configuration (auth, features) are applied automatically.

``zkchroot``
    Function-scoped. The chroot path (``/``) under which the test may create
    nodes; it is created and removed around each test to keep runs isolated.

``zksuperadmin_client``
    Function-scoped. A client authenticated with the superDigest digest
    credentials, for tests that need to bypass ACLs.

Example:

.. code-block:: python

    def test_create_and_read(zkclient):
        zkclient.ensure_path("/my/test/path")
        assert zkclient.exists("/my/test/path") is not None

Testing axes
============

The harness exposes three axes as pytest command-line options (each also
honors an environment variable):

``--zk-version`` (or ``KAZOO_TESTING_ZK_VERSION``, fallback ``ZK_VERSION``)
    ZooKeeper server tag, e.g. ``3.7.2``, ``3.8.6``, ``3.9.5`` (default).

``--zk-auth`` (or ``KAZOO_TESTING_ZK_AUTH``, fallback ``ZK_AUTH``)
    Authentication flavor: ``plain`` (default), ``digest``, ``sasl_digest``,
    ``sasl_gssapi``, ``tls``. The auth flavor selects the matching
    docker-compose overlay file and the client-side connection options (TLS
    certs, SASL options).

``--zk-features`` (or ``KAZOO_TESTING_ZK_FEATURES``, fallback ``ZK_FEATURES``)
    Comma-separated ZooKeeper feature set: ``standard`` (default), ``ttl``,
    ``readonly``, ``reconfig`` (injected as server JVM flags).

``--zk-features=capture``
    Adds the **capture** harness feature: per-member ``tshark`` sidecars record
    all client-port traffic for the session into per-member pcapng artifacts
    that survive teardown, plus — on the ``tls`` flavor — the keylog material
    to decrypt them. Capture is observational and never changes test outcomes.
    See :ref:`capture` for the full workflow (merge + TLS decryption).

Compose layout
==============

The compose files live in ``kazoo/testing/``:

* ``docker-compose.base.yml`` — the base three-node ensemble (ephemeral
  ports, tmpfs data dirs, healthcheck).
* ``docker-compose.auth-<auth>.yml`` — per-auth overlay files layered on top
  of the base file (digest, sasl-digest, tls, sasl-gssapi).
* ``docker-compose.features-capture.yml`` — the capture overlay (``tshark``
  sidecars, keylog agent); other features (ttl/readonly/reconfig) are pure
  JVM-flag interpolation in the base file.
* ``dockerfiles/`` — in-repo support images: TLS cert generation, the Kerberos
  KDC for the GSSAPI axis, the capture ``tshark`` sidecars, and the
  ``tls-secrets-agent`` keylog provisioner.

The active overlay set is resolved by the ``docker_compose_config`` fixture
in :mod:`kazoo.testing.fixtures` and the ensemble is driven through
`testcontainers <https://testcontainers-python.readthedocs.io>`_
(:class:`testcontainers.compose.DockerCompose`).

Architecture
============

The harness drives Docker Compose programmatically inside pytest. Three
orthogonal dimensions define every test run — the **ZK version** (the official
``zookeeper`` image tag), the **auth scheme** (``plain``, ``digest``,
``sasl_digest``, ``sasl_gssapi``, ``tls``, and a TLS tunnel + GSSAPI combo),
and the **feature set** (``standard``, ``ttl``, ``readonly``, ``reconfig``,
plus harness capabilities like ``capture``). They are selected per run via the
``--zk-*`` CLI options (documented above) and materialized into a single
compose project assembled from a base file plus optional overlays:

1. **Base compose file** — ``docker-compose.base.yml`` defines the three-node
   ensemble from the official image: parameterized ``KAZOO_TESTING_ZK_VERSION`` tag, clear
   client ports, tmpfs data dirs, and the 4-letter-word healthcheck
   (``ruok``/``imok``). Each member is split into a *network holder*
   (``zoo1``/``zoo2``/``zoo3``, ``sleep infinity``) and a *ZooKeeper process*
   service joined into the holder's netns via ``network_mode: service:zooN``,
   so restarting the ZooKeeper JVM (failure-injection tests) never tears down
   published client ports or the capture sidecar's tap.
2. **JVM flags are interpolated host-side** — feature flags (``-Dzookeeper.ttl.enabled``,
   read-only ``true``, ``-Dzookeeper.dynamicConfigFile=...``) and auth flags
   (the digest ``superDigest``, TLS/GSSAPI quorum config) are computed in
   ``kazoo/testing/common.py`` and injected into
   ``SERVER_JVMFLAGS`` **here in the base file** via ``${KAZOO_TESTING_ZK_FEATURES_JVMFLAGS}``
   and ``${KAZOO_TESTING_ZK_AUTH_JVMFLAGS}``. Deliberately not composed across overlay files:
   docker-compose merges an ``environment`` map wholesale by key, so a cross-file
   ``SERVER_JVMFLAGS`` override would silently replace the base value instead of
   appending to it.
3. **Auth overlays** — ``docker-compose.auth-<auth>.yml`` resolve per-flavor
   *services* only (they never set ``SERVER_JVMFLAGS``, since compose merges
   an ``environment`` map wholesale per key and would silently override the
   base interpolation): ``sasl-digest`` binds ``jaas/sasl-digest.conf`` and set
   ``JVMFLAGS`` for the login module, ``tls`` spins up a certgen container that
   materializes the throwaway PKI, and ``sasl-gssapi`` adds the in-repo Alpine
   KDC (_kazoo/testing/dockerfiles/kdc_) plus a gssapi init sidecar that
   provisions the realm, keytabs and the server JAAS. ``digest`` declares no
   services at all — it is configured purely by the host-side ``superDigest``
   interpolation.
4. **Capture overlay** — ``docker-compose.features-capture.yml`` adds the
   ``tshark`` sidecar per member that taps the shared netns and writes pcap
   captures to a ``capture://`` URL (useful for deterministically exercising
   client-side cert/keylog handling).

The pytest integration mirrors that layering:

* **Session-scoped lifecycle** — ``zkensemble`` boots one compose project
  (per-session ``COMPOSE_PROJECT_NAME``) matching the active axes, waits for
  it via ``docker compose up --wait``, and tears it down in a ``finally``
  (``down --volumes``) so a partial/failed session is still cleaned up.
* **Collection-time skipping** — every collected item's markers are evaluated
  against the active axes during collection; mismatches become skips with an
  actionable reason, so a run never collects tests that cannot pass on its
  configuration.
* **Per-test isolation** — ``zkclient`` yields one connected client per test,
  scoped to an ephemeral chroot that is created and removed around each test,
  so tests share the live ensemble but not each other's data.

Marker shortcuts
================

The harness registers pytest markers that let you gate tests on the active
axes (they skip with an actionable reason when the active configuration does
not match):

* ``@pytest.mark.zk_version("<3.8")`` — PEP 440 specifier vs. the active ZK
  version.
* ``@pytest.mark.zk_auth("digest", "tls")`` — run only under the listed auth
  schemes.
* ``@pytest.mark.zk_features(require=[...], skip=[...])`` — run only when the
  listed features are (or are not) active.

.. _capture:

Network capture
===============

The ``capture`` feature value layers per-member ``tshark`` sidecars onto the
ensemble. Each sidecar joins its member's network namespace and records all
traffic on that member's *client ports* (clear ``2181``, and secure ``2281``
when TLS is enabled) for the whole session — full frames, no truncation —
into a bind-mounted directory that **survives cluster teardown**, so you can
analyze a failed or interesting run afterwards.

* Artifacts are written **per member**: ``kazoo-client-zooN-*.pcapng`` (one
  per ensemble member, uniquely named per run).
* On the ``tls`` auth flavor the harness also emits the **decryption
  material** (an SSLKEYLOGFILE plus the server/CA certificates), so the
  captured TLS traffic can be decrypted into plaintext using only what the
  run produced.
* Capture is observational: it never changes test outcomes, skip decisions,
  or connection behavior, and it composes with every auth flavor and
  server feature.

Run a capture session
---------------------

.. code-block:: bash

    # plain-auth capture
    pytest kazoo/tests/integ/test_client.py --zk-features=capture -v

    # TLS-auth capture (emits the decryption keylog too)
    pytest kazoo/tests/integ/test_client.py --zk-auth=tls --zk-features=capture -v

At teardown the harness prints the artifact location (the pytest session
basetemp, exported as ``KAZOO_TESTING_ZK_WORK_DIR``). The artifacts are left in place after
the suite exits:

.. code-block:: bash

    ls "$KAZOO_TESTING_ZK_WORK_DIR/captures/"                # kazoo-client-zoo{1,2,3}-*.pcapng
    ls "$KAZOO_TESTING_ZK_WORK_DIR/captures/tls/"            # tls run only: zk-secrets.log, server-cert.pem, ca.pem

Re-assemble (merge) the per-member files
----------------------------------------

The Kazoo client connects to whichever ensemble member it happens to pick, so
a single session's traffic can be split across the three files. Merge them
into one capture for a combined view (``mergecap`` ships with Wireshark/tshark;
no capture tooling is required on the host to *run* the tests — this analysis
step is optional):

.. code-block:: bash

    mergecap -w session-all.pcapng \
      "$KAZOO_TESTING_ZK_WORK_DIR"/captures/kazoo-client-zoo1-*.pcapng \
      "$KAZOO_TESTING_ZK_WORK_DIR"/captures/kazoo-client-zoo2-*.pcapng \
      "$KAZOO_TESTING_ZK_WORK_DIR"/captures/kazoo-client-zoo3-*.pcapng

On the plain/digest/sasl flavors the client protocol is unencrypted on port
2181, so the merged capture is immediately readable:

.. code-block:: bash

    tshark -r session-all.pcapng -Y "tcp.port == 2181" -c 10

Decrypt the TLS traffic
-----------------------

Modern TLS uses forward secrecy, so a private key alone cannot decrypt a
session. The ``tls`` capture run attaches a passive *keylog agent* to the
three server JVMs, which records each handshake's master secret; the harness
merges those into ``captures/tls/zk-secrets.log`` and copies the server/CA
certificates alongside. Provide that keylog file to tshark via
``tls.keylog_file``:

.. code-block:: bash

    # decrypt the merged capture and show plaintext ZK protocol magic
    tshark -o tls.keylog_file:"$KAZOO_TESTING_ZK_WORK_DIR/captures/tls/zk-secrets.log" \
      -r session-all.pcapng \
      -Y "tls" -T fields -e tcp.payload | grep -c "ffffffff"

    # alternative: decrypt the per-member files directly, no merge needed
    tshark -o tls.keylog_file:"$KAZOO_TESTING_ZK_WORK_DIR/captures/tls/zk-secrets.log" \
      -r "$KAZOO_TESTING_ZK_WORK_DIR"/captures/kazoo-client-zoo1-*.pcapng -Y "tls"

``server-cert.pem`` and ``ca.pem`` identify the throwaway test PKI the
ensemble used; they are context for the trace. No real credentials are ever
involved, and no private key is exported — the keylog *is* the key material.
In Wireshark, set **Edit → Preferences → Protocols → TLS → (Pre)-Master-Secret
log filename** to ``zk-secrets.log`` and reload.

Zake
====

For those that do not need (or desire) to setup a Zookeeper cluster to test
integration with kazoo there is also a library called
`zake <https://pypi.python.org/pypi/zake/>`_. Contributions to
`Zake's github repository <https://github.com/yahoo/Zake>`_ are welcome.

Zake can be used to provide a *mock client* to layers of your application that
interact with kazoo (using the same client interface) during testing to allow
for introspection of what was stored, which watchers are active (and more)
after your test of your application code has finished.