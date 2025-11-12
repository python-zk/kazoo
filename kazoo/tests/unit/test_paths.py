from __future__ import annotations

import pytest

from kazoo.protocol import paths


class TestNormPath:
    def test_normpath(self) -> None:
        assert paths.normpath("/a/b") == "/a/b"

    def test_normpath_empty(self) -> None:
        assert paths.normpath("") == ""

    def test_normpath_unicode(self) -> None:
        assert paths.normpath("/\xe4/b") == "/\xe4/b"

    def test_normpath_dots(self) -> None:
        assert paths.normpath("/a./b../c") == "/a./b../c"

    def test_normpath_slash(self) -> None:
        assert paths.normpath("/") == "/"

    def test_normpath_multiple_slashes(self) -> None:
        assert paths.normpath("//") == "/"
        assert paths.normpath("//a/b") == "/a/b"
        assert paths.normpath("/a//b//") == "/a/b"
        assert paths.normpath("//a////b///c/") == "/a/b/c"

    def test_normpath_relative(self) -> None:
        with pytest.raises(ValueError):
            paths.normpath("./a/b")
        with pytest.raises(ValueError):
            paths.normpath("/a/../b")

    def test_normpath_trailing(self) -> None:
        assert paths.normpath("/", trailing=True) == "/"


class TestJoin:
    def test_join(self) -> None:
        assert paths.join("/a") == "/a"
        assert paths.join("/a", "b/") == "/a/b/"
        assert paths.join("/a", "b", "c") == "/a/b/c"

    def test_join_empty(self) -> None:
        assert paths.join("") == ""
        assert paths.join("", "a", "b") == "a/b"
        assert paths.join("/a", "", "b/", "c") == "/a/b/c"

    def test_join_absolute(self) -> None:
        assert paths.join("/a/b", "/c") == "/c"


class TestIsAbs:
    def test_isabs(self) -> None:
        assert paths.isabs("/") is True
        assert paths.isabs("/a") is True
        assert paths.isabs("/a//b/c") is True
        assert paths.isabs("//a/b") is True

    def test_isabs_false(self) -> None:
        assert paths.isabs("") is False
        assert paths.isabs("a/") is False
        assert paths.isabs("a/../") is False


class TestBaseName:
    def test_basename(self) -> None:
        assert paths.basename("") == ""
        assert paths.basename("/") == ""
        assert paths.basename("//a") == "a"
        assert paths.basename("//a/") == ""
        assert paths.basename("/a/b.//c..") == "c.."


class TestPrefixRoot:
    def test_prefix_root(self) -> None:
        assert paths._prefix_root("/a/", "b/c") == "/a/b/c"
        assert paths._prefix_root("/a/b", "c/d") == "/a/b/c/d"
        assert paths._prefix_root("/a", "/b/c") == "/a/b/c"
        assert paths._prefix_root("/a", "//b/c.") == "/a/b/c."


class TestNormRoot:
    def test_norm_root(self) -> None:
        assert paths._norm_root("") == "/"
        assert paths._norm_root("/") == "/"
        assert paths._norm_root("//a") == "/a"
        assert paths._norm_root("//a./b") == "/a./b"
