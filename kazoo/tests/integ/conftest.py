from __future__ import annotations

from kazoo.testing.fixtures import (
    docker_compose,
    docker_compose_config,
    docker_env,
    pytest_addoption as kazoo_fixtures_pytest_addoption,
    pytest_collection_modifyitems as kazoo_fixtures_pytest_collection_modifyitems,  # noqa: E501
    pytest_configure as kazoo_fixtures_pytest_configure,
    zkchroot,
    zkclient,
    zkensemble,
    zksuperadmin_client,
)

# pytest discovers fixtures by name in conftest modules; re-export the
# ensemble fixtures so the integ tests can request them directly. Declaring
# them in ``__all__`` marks them as intentional re-exports (F401/F811).
__all__ = [
    "docker_compose",
    "docker_compose_config",
    "docker_env",
    "zkchroot",
    "zkclient",
    "zkensemble",
    "zksuperadmin_client",
]

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


# These hooks are implemented in kazoo.testing.fixtures; local stubs
# re-expose them through this conftest module for pytest's plugin discovery.
def pytest_addoption(parser: pytest.Parser) -> None:
    kazoo_fixtures_pytest_addoption(parser)


def pytest_configure(config: pytest.Config) -> None:
    kazoo_fixtures_pytest_configure(config)


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    kazoo_fixtures_pytest_collection_modifyitems(session, config, items)
