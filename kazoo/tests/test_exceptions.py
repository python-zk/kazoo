from __future__ import annotations

from unittest import TestCase

from types import ModuleType

import pytest


class ExceptionsTestCase(TestCase):
    def _get(self) -> ModuleType:
        from kazoo import exceptions

        return exceptions

    def test_backwards_alias(self) -> None:
        module = self._get()
        assert hasattr(module, "NoNodeException")
        assert module.NoNodeException is module.NoNodeError

    def test_exceptions_code(self) -> None:
        module = self._get()
        exc_8 = module.EXCEPTIONS[-8]
        assert isinstance(exc_8(), module.BadArgumentsError)

    def test_invalid_code(self) -> None:
        module = self._get()
        with pytest.raises(RuntimeError):
            module.EXCEPTIONS.__getitem__(666)

    def test_exceptions_construction(self) -> None:
        module = self._get()
        exc = module.EXCEPTIONS[-101]()
        assert type(exc) is module.NoNodeError
        assert exc.args == ()
