"""Zookeeper Serializers, Deserializers, and NamedTuple objects"""
from __future__ import annotations

import struct
from collections import namedtuple
from typing import Any, ClassVar, Sequence, TYPE_CHECKING

from kazoo.exceptions import EXCEPTIONS
from kazoo.protocol.states import ZnodeStat
from kazoo.security import ACL
from kazoo.security import Id

if TYPE_CHECKING:
    from kazoo.client import KazooClient, WatchFunc

# Struct objects with formats compiled
bool_struct = struct.Struct("B")
int_struct = struct.Struct("!i")
int_int_struct = struct.Struct("!ii")
int_int_long_struct = struct.Struct("!iiq")

int_long_int_long_struct = struct.Struct("!iqiq")
long_struct = struct.Struct("!q")
multiheader_struct = struct.Struct("!iBi")
reply_header_struct = struct.Struct("!iqi")
stat_struct = struct.Struct("!qqqqiiiqiiq")


def read_string(buffer: bytes, offset: int) -> tuple:
    """Reads an int specified buffer into a string and returns the
    string and the new offset in the buffer"""
    length = int_struct.unpack_from(buffer, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return buffer[index : index + length].decode("utf-8"), offset


def read_acl(bytes: bytes, offset: int) -> tuple:
    perms = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    scheme, offset = read_string(bytes, offset)
    id, offset = read_string(bytes, offset)
    return ACL(perms, Id(scheme, id)), offset


def write_string(bytes: str | None) -> bytes:
    if not bytes:
        return int_struct.pack(-1)
    else:
        utf8_str = bytes.encode("utf-8")
        return int_struct.pack(len(utf8_str)) + utf8_str


def write_buffer(bytes: bytes | None) -> bytes:
    if bytes is None:
        return int_struct.pack(-1)
    else:
        return int_struct.pack(len(bytes)) + bytes


def read_buffer(bytes: bytes, offset: int) -> tuple:
    length = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return bytes[index : index + length], offset


class Close(namedtuple("Close", "")):
    type: ClassVar[int] = -11

    @classmethod
    def serialize(self) -> bytes:
        return b""


CloseInstance = Close()


class Ping(namedtuple("Ping", "")):
    type: ClassVar[int] = 11

    @classmethod
    def serialize(cls) -> bytes:
        return b""


PingInstance = Ping()


class Connect(
    namedtuple(
        "Connect",
        "protocol_version last_zxid_seen"
        " time_out session_id passwd read_only",
    )
):
    protocol_version: int
    last_zxid_seen: int
    time_out: int
    session_id: int
    passwd: bytes
    read_only: bool

    type: int | None = None  # Note: Not a classvar

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(
            int_long_int_long_struct.pack(
                self.protocol_version,
                self.last_zxid_seen,
                self.time_out,
                self.session_id,
            )
        )
        b.extend(write_buffer(self.passwd))
        b.extend([1 if self.read_only else 0])
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> tuple[Any, int]:
        proto_version, timeout, session_id = int_int_long_struct.unpack_from(
            bytes, offset
        )
        offset += int_int_long_struct.size
        password, offset = read_buffer(bytes, offset)

        try:
            read_only = bool_struct.unpack_from(bytes, offset)[0] == 1
            offset += bool_struct.size
        except struct.error:
            read_only = False
        return (
            cls(proto_version, 0, timeout, session_id, password, read_only),
            offset,
        )


class Create(namedtuple("Create", "path data acl flags")):
    path: str
    data: bytes | None
    acl: Sequence[ACL]
    flags: int

    type: ClassVar[int] = 1

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(write_buffer(self.data))
        b.extend(int_struct.pack(len(self.acl)))
        for acl in self.acl:
            b.extend(
                int_struct.pack(acl.perms)
                + write_string(acl.id.scheme)
                + write_string(acl.id.id)
            )
        b.extend(int_struct.pack(self.flags))
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> str:
        return read_string(bytes, offset)[0]


class Delete(namedtuple("Delete", "path version")):
    path: str
    version: int

    type: ClassVar[int] = 2

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> bool:
        return True


class Exists(namedtuple("Exists", "path watcher")):
    path: str
    watcher: WatchFunc | None

    type: ClassVar[int] = 3

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> ZnodeStat | None:
        stat = ZnodeStat(*stat_struct.unpack_from(bytes, offset))
        return stat if stat.czxid != -1 else None


class GetData(namedtuple("GetData", "path watcher")):
    path: str
    watcher: WatchFunc | None

    type: ClassVar[int] = 4

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        return b

    @classmethod
    def deserialize(
        cls, bytes: bytes, offset: int
    ) -> tuple[bytes | None, ZnodeStat]:
        data, offset = read_buffer(bytes, offset)
        stat = ZnodeStat(*stat_struct.unpack_from(bytes, offset))
        return data, stat


class SetData(namedtuple("SetData", "path data version")):
    path: str
    data: bytes | None
    version: int

    type: ClassVar[int] = 5

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(write_buffer(self.data))
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> ZnodeStat:
        return ZnodeStat(*stat_struct.unpack_from(bytes, offset))


class GetACL(namedtuple("GetACL", "path")):
    path: str

    type: ClassVar[int] = 6

    def serialize(self) -> bytearray:
        return bytearray(write_string(self.path))

    @classmethod
    def deserialize(
        cls, bytes: bytes, offset: int
    ) -> tuple[list[ACL], ZnodeStat] | list[ACL]:
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        if count == -1:  # pragma: nocover
            return []

        acls = []
        for c in range(count):
            acl, offset = read_acl(bytes, offset)
            acls.append(acl)
        stat = ZnodeStat(*stat_struct.unpack_from(bytes, offset))
        return acls, stat


class SetACL(namedtuple("SetACL", "path acls version")):
    path: str
    acls: Sequence[ACL]
    version: int

    type: ClassVar[int] = 7

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(len(self.acls)))
        for acl in self.acls:
            b.extend(
                int_struct.pack(acl.perms)
                + write_string(acl.id.scheme)
                + write_string(acl.id.id)
            )
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> ZnodeStat:
        return ZnodeStat(*stat_struct.unpack_from(bytes, offset))


class GetChildren(namedtuple("GetChildren", "path watcher")):
    path: str
    watcher: WatchFunc | None

    type: ClassVar[int] = 8

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> list[str]:
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        if count == -1:  # pragma: nocover
            return []

        children = []
        for c in range(count):
            child, offset = read_string(bytes, offset)
            children.append(child)
        return children


class Sync(namedtuple("Sync", "path")):
    path: str

    type: ClassVar[int] = 9

    def serialize(self) -> bytes:
        return write_string(self.path)

    @classmethod
    def deserialize(cls, buffer: bytes, offset: int) -> str:
        return read_string(buffer, offset)[0]


class GetChildren2(namedtuple("GetChildren2", "path watcher")):
    path: str
    watcher: WatchFunc | None

    type: ClassVar[int] = 12

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        return b

    @classmethod
    def deserialize(
        cls, bytes: bytes, offset: int
    ) -> tuple[list[str], ZnodeStat] | list[str]:
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        if count == -1:  # pragma: nocover
            return []

        children = []
        for c in range(count):
            child, offset = read_string(bytes, offset)
            children.append(child)
        stat = ZnodeStat(*stat_struct.unpack_from(bytes, offset))
        return children, stat


class CheckVersion(namedtuple("CheckVersion", "path version")):
    path: str
    version: int

    type: ClassVar[int] = 13

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        return b


class Transaction(namedtuple("Transaction", "operations")):
    operations: list[Any]

    type: ClassVar[int] = 14

    def serialize(self) -> bytearray:
        b = bytearray()
        for op in self.operations:
            b.extend(
                MultiHeader(op.type, False, -1).serialize() + op.serialize()
            )
        return b + multiheader_struct.pack(-1, True, -1)

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> list[Any]:
        header = MultiHeader(None, False, None)
        results = []
        response = None
        while not header.done:
            if header.type == Create.type:
                response, offset = read_string(bytes, offset)
            elif header.type == Delete.type:
                response = True
            elif header.type == SetData.type:
                response = ZnodeStat(*stat_struct.unpack_from(bytes, offset))
                offset += stat_struct.size
            elif header.type == CheckVersion.type:
                response = True
            elif header.type == -1:
                err = int_struct.unpack_from(bytes, offset)[0]
                offset += int_struct.size
                response = EXCEPTIONS[err]()
            if response:
                results.append(response)
            header, offset = MultiHeader.deserialize(bytes, offset)
        return results

    @staticmethod
    def unchroot(client: KazooClient, response: list[Any]) -> list[Any]:
        resp = []
        for result in response:
            if isinstance(result, str):
                resp.append(client.unchroot(result))
            else:
                resp.append(result)
        return resp


class Create2(namedtuple("Create2", "path data acl flags")):
    path: str
    data: bytes | None
    acl: Sequence[ACL]
    flags: int

    type: ClassVar[int] = 15

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(write_buffer(self.data))
        b.extend(int_struct.pack(len(self.acl)))
        for acl in self.acl:
            b.extend(
                int_struct.pack(acl.perms)
                + write_string(acl.id.scheme)
                + write_string(acl.id.id)
            )
        b.extend(int_struct.pack(self.flags))
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> tuple[str, ZnodeStat]:
        path, offset = read_string(bytes, offset)
        stat = ZnodeStat(*stat_struct.unpack_from(bytes, offset))
        return path, stat


class Reconfig(
    namedtuple("Reconfig", "joining leaving new_members config_id")
):
    joining: str | None
    leaving: str | None
    new_members: str | None
    config_id: int

    type: ClassVar[int] = 16

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_string(self.joining))
        b.extend(write_string(self.leaving))
        b.extend(write_string(self.new_members))
        b.extend(long_struct.pack(self.config_id))
        return b

    @classmethod
    def deserialize(
        cls, bytes: bytes, offset: int
    ) -> tuple[bytes | None, ZnodeStat]:
        data, offset = read_buffer(bytes, offset)
        stat = ZnodeStat(*stat_struct.unpack_from(bytes, offset))
        return data, stat


class Auth(namedtuple("Auth", "auth_type scheme auth")):
    auth_type: int
    scheme: str
    auth: str

    type: ClassVar[int] = 100

    def serialize(self) -> bytes:
        return (
            int_struct.pack(self.auth_type)
            + write_string(self.scheme)
            + write_string(self.auth)
        )


class SASL(namedtuple("SASL", "challenge")):
    challenge: bytes | None

    type: ClassVar[int] = 102

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(write_buffer(self.challenge))
        return b

    @classmethod
    def deserialize(
        cls, bytes: bytes, offset: int
    ) -> tuple[bytes | None, int]:
        challenge, offset = read_buffer(bytes, offset)
        return challenge, offset


class Watch(namedtuple("Watch", "type state path")):
    type: int
    state: int
    path: str

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> tuple[Watch, int]:
        """Given bytes and the current bytes offset, return the
        type, state, path, and new offset"""
        type, state = int_int_struct.unpack_from(bytes, offset)
        offset += int_int_struct.size
        path, offset = read_string(bytes, offset)
        return cls(type, state, path), offset


class ReplyHeader(namedtuple("ReplyHeader", "xid, zxid, err")):
    xid: int
    zxid: int
    err: int

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> tuple[ReplyHeader, int]:
        """Given bytes and the current bytes offset, return a
        :class:`ReplyHeader` instance and the new offset"""
        new_offset = offset + reply_header_struct.size
        return (
            cls(*reply_header_struct.unpack_from(bytes, offset)),
            new_offset,
        )


class MultiHeader(namedtuple("MultiHeader", "type, done, err")):
    type: int | None
    done: bool
    err: int | None

    def serialize(self) -> bytearray:
        b = bytearray()
        b.extend(int_struct.pack(self.type))
        b.extend([1 if self.done else 0])
        b.extend(int_struct.pack(self.err))
        return b

    @classmethod
    def deserialize(cls, bytes: bytes, offset: int) -> tuple[MultiHeader, int]:
        t, done, err = multiheader_struct.unpack_from(bytes, offset)
        offset += multiheader_struct.size
        return cls(t, done == 1, err), offset
