from __future__ import annotations

import uuid

from typing import Any

import pytest

from kazoo.recipe.counter import Counter
from kazoo.testing import KazooTestCase


class KazooCounterTests(KazooTestCase):
    def _makeOne(self, **kw: Any) -> Counter:
        path = "/" + uuid.uuid4().hex
        return self.client.Counter(path, **kw)

    def test_int_counter(self) -> None:
        counter = self._makeOne()
        assert counter.value == 0
        counter += 2
        counter + 1
        assert counter.value == 3
        counter -= 3
        counter - 1
        assert counter.value == -1

    def test_int_curator_counter(self) -> None:
        counter = self._makeOne(support_curator=True)
        assert counter.value == 0
        counter += 2
        counter + 1
        assert counter.value == 3
        counter -= 3
        counter - 1
        assert counter.value == -1
        counter += 1
        counter += 2147483647
        assert counter.value == 2147483647
        counter -= 2147483647
        counter -= 2147483647
        assert counter.value == -2147483647

    def test_float_counter(self) -> None:
        counter = self._makeOne(default=0.0)
        assert counter.value == 0.0
        counter += 2.1
        assert counter.value == 2.1
        counter -= 3.1
        assert counter.value == -1.0

    def test_errors(self) -> None:
        counter = self._makeOne()
        with pytest.raises(TypeError):
            counter.__add__(2.1)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            counter.__add__(b"a")  # type: ignore[operator]
        with pytest.raises(TypeError):
            counter = self._makeOne(  # type: ignore[arg-type]
                default=0.0, support_curator=True
            )

    def test_pre_post_values(self) -> None:
        counter = self._makeOne()
        assert counter.value == 0
        assert counter.pre_value is None
        assert counter.post_value is None
        counter += 2
        assert counter.pre_value == 0
        assert counter.post_value == 2
        counter -= 3
        assert counter.pre_value == 2
        assert counter.post_value == -1
