from __future__ import annotations

import struct
import unittest

from kazoo.exceptions import ZookeeperError
from kazoo.protocol import serialization
from kazoo.protocol.states import ZnodeStat
from kazoo.security import ACL, Id

from unittest_parametrize import ParametrizedTestCase, parametrize, param

_int_struct = struct.Struct("!i")
_int_int_struct = struct.Struct("!ii")
_int_int_long_struct = struct.Struct("!iiq")
_int_long_int_long_struct = struct.Struct("!iqiq")
_long_struct = struct.Struct("!q")
_multiheader_struct = struct.Struct("!iBi")
_reply_header_struct = struct.Struct("!iqi")
_stat_struct = struct.Struct("!qqqqiiiqiiq")


def _write_string(value: str | None) -> bytes:
    if value is None:
        return _int_struct.pack(-1)
    encoded = value.encode("utf-8")
    return _int_struct.pack(len(encoded)) + encoded


def _write_buffer(value: bytes | None) -> bytes:
    if value is None:
        return _int_struct.pack(-1)
    return _int_struct.pack(len(value)) + value


def _make_stat() -> ZnodeStat:
    return ZnodeStat(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)


def _make_acl_list() -> list[ACL]:
    return [ACL(1, Id("scheme", "identifier"))]


class TestUtils(ParametrizedTestCase):
    @parametrize(
        ("s",),
        [
            param("hello-\u03c0", id="non_empty_string"),
            param(None, id="none_value"),
        ],
    )
    def test_write_read_string_roundtrip(self, s: str | None) -> None:
        b = serialization.write_string(s)
        decoded, offset = serialization.read_string(b, 0)
        assert decoded == s
        assert offset == len(b)

    def test_write_read_empty_string_roundtrip(self) -> None:
        b = serialization.write_string("")
        decoded, offset = serialization.read_string(b, 0)
        assert offset == len(b)
        assert decoded is None

    @parametrize(
        ("data",),
        [
            param(b"\x00\xff\x01", id="non_empty_buffer"),
            param(b"", id="empty_buffer"),
            param(None, id="none_value"),
        ],
    )
    def test_write_read_buffer_roundtrip(self, data: bytes | None) -> None:
        b = serialization.write_buffer(data)
        decoded, offset = serialization.read_buffer(b, 0)
        assert decoded == data
        assert offset == len(b)

    def test_read_acl(self) -> None:
        perms = 7
        scheme = "scheme-x"
        idv = "ident-y"
        # build bytes: perms + scheme + id
        b = (
            serialization.int_struct.pack(perms)
            + serialization.write_string(scheme)
            + serialization.write_string(idv)
        )
        acl_obj, offset = serialization.read_acl(b, 0)
        assert acl_obj.perms == perms
        assert acl_obj.id.scheme == scheme
        assert acl_obj.id.id == idv
        assert offset == len(b)


class TestClose(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Close.type == -11

    def test_serialize(self) -> None:
        assert serialization.Close.serialize() == b""
        assert serialization.CloseInstance.serialize() == b""


class TestPing(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Ping.type == 11

    def test_serialize(self) -> None:
        assert serialization.Ping.serialize() == b""
        assert serialization.PingInstance.serialize() == b""


class TestConnect(unittest.TestCase):
    def test_serialize(self) -> None:
        obj = serialization.Connect(1, 2, 3, 4, b"pwd", True)
        assert obj.serialize() == (
            _int_long_int_long_struct.pack(1, 2, 3, 4)
            + _write_buffer(b"pwd")
            + b"\x01"
        )

    def test_deserialize(self) -> None:
        data = (
            _int_int_long_struct.pack(1, 3, 4)
            + _write_buffer(b"pwd")
            + b"\x01"
        )
        decoded, offset = serialization.Connect.deserialize(data, 0)
        assert decoded == serialization.Connect(1, 0, 3, 4, b"pwd", True)
        assert offset == len(data)


class TestCreate(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Create.type == 1

    def test_serialize(self) -> None:
        obj = serialization.Create("/path", b"data", _make_acl_list(), 3)
        assert obj.serialize() == (
            _write_string("/path")
            + _write_buffer(b"data")
            + _int_struct.pack(1)
            + _int_struct.pack(1)
            + _write_string("scheme")
            + _write_string("identifier")
            + _int_struct.pack(3)
        )

    def test_deserialize(self) -> None:
        assert (
            serialization.Create.deserialize(_write_string("/path"), 0)
            == "/path"
        )


class TestDelete(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Delete.type == 2

    def test_serialize(self) -> None:
        obj = serialization.Delete("/path", 42)
        assert obj.serialize() == _write_string("/path") + _int_struct.pack(42)

    def test_deserialize(self) -> None:
        assert serialization.Delete.deserialize(b"", 0) is True


class TestExists(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Exists.type == 3

    def test_serialize(self) -> None:
        obj = serialization.Exists("/path", None)
        assert obj.serialize() == _write_string("/path") + b"\x00"

    def test_deserialize(self) -> None:
        response = _stat_struct.pack(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
        assert serialization.Exists.deserialize(response, 0) == _make_stat()


class TestGetData(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.GetData.type == 4

    def test_serialize(self) -> None:
        obj = serialization.GetData("/path", None)
        assert obj.serialize() == _write_string("/path") + b"\x00"

    def test_deserialize(self) -> None:
        response = _write_buffer(b"data") + _stat_struct.pack(
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        )
        data, result_stat = serialization.GetData.deserialize(response, 0)
        assert data == b"data"
        assert result_stat == _make_stat()


class TestSetData(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.SetData.type == 5

    def test_serialize(self) -> None:
        obj = serialization.SetData("/path", b"data", 7)
        assert obj.serialize() == (
            _write_string("/path")
            + _write_buffer(b"data")
            + _int_struct.pack(7)
        )

    def test_deserialize(self) -> None:
        result_stat = serialization.SetData.deserialize(
            _stat_struct.pack(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
            0,
        )
        assert result_stat == _make_stat()


class TestGetACL(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.GetACL.type == 6

    def test_serialize(self) -> None:
        obj = serialization.GetACL("/path")
        assert obj.serialize() == _write_string("/path")

    def test_deserialize(self) -> None:
        acls, stat = serialization.GetACL.deserialize(
            _int_struct.pack(1)
            + _int_struct.pack(1)
            + _write_string("scheme")
            + _write_string("identifier")
            + _stat_struct.pack(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
            0,
        )
        assert acls == _make_acl_list()
        assert stat == _make_stat()


class TestSetACL(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.SetACL.type == 7

    def test_serialize(self) -> None:
        obj = serialization.SetACL("/path", _make_acl_list(), 2)
        assert obj.serialize() == (
            _write_string("/path")
            + _int_struct.pack(1)
            + _int_struct.pack(1)
            + _write_string("scheme")
            + _write_string("identifier")
            + _int_struct.pack(2)
        )

    def test_deserialize(self) -> None:
        result_stat = serialization.SetACL.deserialize(
            _stat_struct.pack(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
            0,
        )
        assert result_stat == _make_stat()


class TestGetChildren(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.GetChildren.type == 8

    def test_serialize(self) -> None:
        obj = serialization.GetChildren("/path", None)
        assert obj.serialize() == _write_string("/path") + b"\x00"

    def test_deserialize(self) -> None:
        children = serialization.GetChildren.deserialize(
            _int_struct.pack(2) + _write_string("a") + _write_string("b"),
            0,
        )
        assert children == ["a", "b"]


class TestSync(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Sync.type == 9

    def test_serialize(self) -> None:
        obj = serialization.Sync("/path")
        assert obj.serialize() == _write_string("/path")

    def test_deserialize(self) -> None:
        assert (
            serialization.Sync.deserialize(_write_string("/path"), 0)
            == "/path"
        )


class TestGetChildren2(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.GetChildren2.type == 12

    def test_serialize(self) -> None:
        obj = serialization.GetChildren2("/path", None)
        assert obj.serialize() == _write_string("/path") + b"\x00"

    def test_deserialize(self) -> None:
        children, stat = serialization.GetChildren2.deserialize(
            _int_struct.pack(2)
            + _write_string("a")
            + _write_string("b")
            + _stat_struct.pack(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
            0,
        )
        assert children == ["a", "b"]
        assert stat == _make_stat()


class TestCheckVersion(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.CheckVersion.type == 13

    def test_serialize(self) -> None:
        obj = serialization.CheckVersion("/path", 99)
        assert obj.serialize() == _write_string("/path") + _int_struct.pack(99)


class TestTransaction(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Transaction.type == 14

    def test_serialize(self) -> None:
        transaction = serialization.Transaction(
            [
                serialization.Create("/path", b"data", _make_acl_list(), 3),
                serialization.Delete("/path", 1),
            ]
        )
        serialized = transaction.serialize()
        assert serialized.startswith(
            serialization.MultiHeader(
                serialization.Create.type, False, -1
            ).serialize()
        )
        assert serialized.endswith(
            serialization.MultiHeader(-1, True, -1).serialize()
        )
        assert _write_string("/path") in serialized

    def test_deserialize(self) -> None:
        response = (
            _multiheader_struct.pack(serialization.Create.type, 0, -1)
            + _write_string("/path")
            + _multiheader_struct.pack(serialization.Delete.type, 0, -1)
            + _multiheader_struct.pack(-1, 1, -1)
        )
        result = serialization.Transaction.deserialize(response, 0)
        assert result == ["/path", True]

    def test_deserialize_all_types(self) -> None:
        response = (
            _multiheader_struct.pack(serialization.Create.type, 0, -1)
            + _write_string("/path")
            + _multiheader_struct.pack(serialization.Delete.type, 0, -1)
            + _multiheader_struct.pack(serialization.SetData.type, 0, -1)
            + _stat_struct.pack(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
            + _multiheader_struct.pack(serialization.CheckVersion.type, 0, -1)
            + _multiheader_struct.pack(-1, 0, -1)
            + _int_struct.pack(-4)
            + _multiheader_struct.pack(-1, 1, -1)
        )
        result = serialization.Transaction.deserialize(response, 0)
        assert result[0] == "/path"
        assert result[1] is True
        assert result[2] == _make_stat()
        assert result[3] is True
        assert isinstance(result[4], ZookeeperError)
        assert result[4].__class__.__name__ == "ConnectionLoss"

    def test_unchroot(self) -> None:
        class DummyClient:
            def __init__(self, chroot: str) -> None:
                self.chroot = chroot

            def unchroot(self, path: str) -> str:
                if self.chroot == path:
                    return "/"
                if path.startswith(self.chroot):
                    return path[len(self.chroot) :]
                return path

        response = ["/a/b", True, "/a/c"]
        client = DummyClient("/a")
        result = serialization.Transaction.unchroot(
            client,  # type: ignore[arg-type]
            response,  # type: ignore[arg-type]
        )
        assert result == ["/b", True, "/c"]


class TestCreate2(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Create2.type == 15

    def test_serialize(self) -> None:
        obj = serialization.Create2("/path", b"data", _make_acl_list(), 4)
        assert obj.serialize() == (
            _write_string("/path")
            + _write_buffer(b"data")
            + _int_struct.pack(1)
            + _int_struct.pack(1)
            + _write_string("scheme")
            + _write_string("identifier")
            + _int_struct.pack(4)
        )

    def test_deserialize(self) -> None:
        path, stat = serialization.Create2.deserialize(
            _write_string("/path")
            + _stat_struct.pack(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
            0,
        )
        assert path == "/path"
        assert stat == _make_stat()


class TestReconfig(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Reconfig.type == 16

    def test_serialize(self) -> None:
        obj = serialization.Reconfig("join", "leave", "members", 37)
        assert obj.serialize() == (
            _write_string("join")
            + _write_string("leave")
            + _write_string("members")
            + _long_struct.pack(37)
        )

    def test_deserialize(self) -> None:
        data, stat = serialization.Reconfig.deserialize(
            _write_buffer(b"config-data")
            + _stat_struct.pack(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
            0,
        )
        assert data == b"config-data"
        assert stat == _make_stat()


class TestAuth(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.Auth.type == 100

    def test_serialize(self) -> None:
        obj = serialization.Auth(1, "scheme", "auth")
        assert obj.serialize() == (
            _int_struct.pack(1)
            + _write_string("scheme")
            + _write_string("auth")
        )


class TestSASL(unittest.TestCase):
    def test_type(self) -> None:
        assert serialization.SASL.type == 102

    def test_serialize(self) -> None:
        obj = serialization.SASL(b"challenge")
        assert obj.serialize() == _write_buffer(b"challenge")

    def test_deserialize(self) -> None:
        challenge, offset = serialization.SASL.deserialize(
            _write_buffer(b"challenge"), 0
        )
        assert challenge == b"challenge"
        assert offset == len(_write_buffer(b"challenge"))


class TestWatch(unittest.TestCase):
    def test_deserialize(self) -> None:
        data = _int_int_struct.pack(1, 2) + _write_string("/path")
        decoded, offset = serialization.Watch.deserialize(data, 0)
        assert decoded.type == 1
        assert decoded.state == 2
        assert decoded.path == "/path"
        assert offset == len(data)


class TestReplyHeader(unittest.TestCase):
    def test_deserialize(self) -> None:
        reply = serialization.ReplyHeader(1, 2, 3)
        data = _reply_header_struct.pack(1, 2, 3)
        decoded_reply, offset = serialization.ReplyHeader.deserialize(data, 0)
        assert decoded_reply == reply
        assert offset == len(data)


class TestMultiHeader(unittest.TestCase):
    def test_serialize(self) -> None:
        header = serialization.MultiHeader(1, True, 2)
        assert header.serialize() == _multiheader_struct.pack(1, 1, 2)

    def test_deserialize(self) -> None:
        data = _multiheader_struct.pack(1, 1, 2)
        decoded_multi, offset = serialization.MultiHeader.deserialize(data, 0)
        assert decoded_multi.type == 1
        assert decoded_multi.done is True
        assert decoded_multi.err == 2
        assert offset == len(data)
