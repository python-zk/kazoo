"""Testing utilities for running integration tests against ZooKeeper.

The package is split into two modules:

* :mod:`kazoo.testing.common` — the harness business logic: the testing axes
  (version / auth / features), the ensemble and client helpers, the Docker and
  bind-mount helpers, the capture / keylog / Kerberos assembly, and the marker
  evaluation. It stays importable without pytest or a Docker engine.
* :mod:`kazoo.testing.fixtures` — the thin pytest-facing fixtures and plugin
  hooks (:data:`zkclient`, :data:`zkensemble`, :data:`docker_compose`, ...)
  that delegate to :mod:`kazoo.testing.common`.
"""

from kazoo.testing import common, fixtures  # noqa: F401

__all__ = ("common", "fixtures")
