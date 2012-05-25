"""Kazoo Interfaces

"""
from zope.interface import (
    Attribute,
    Interface,
)

# public API


class IHandler(Interface):
    """A Callback Handler for Zookeeper completion and watcher callbacks

    This object must implement several methods responsible for determing
    how completion and watch callbacks are handled.

    """
