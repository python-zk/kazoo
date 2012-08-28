"""Zookeeper Serializers, Deserializers, and NamedTuple objects"""
from collections import namedtuple
import struct

ReplyHeader = namedtuple('ReplyHeader', 'xid zxid err')

# Struct objects with formats compiled
int_struct = struct.Struct('!i')
int_int_struct = struct.Struct('!ii')
int_int_long_struct = struct.Struct('!iiq')

int_long_int_long_struct = struct.Struct('!iqiq')
reply_header_struct = struct.Struct('!iqi')


def read_string(buffer, offset):
    """Reads an int specified buffer into a string and returns the
    string and the new offset in the buffer"""
    length = int_struct.unpack_from(buffer, offset)
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return str(buffer[index:index + length].decode('utf-8')), offset


def write_buffer(bytes):
    if not bytes:
        return int_struct.pack(-1)
    else:
        return int_struct.pack(len(bytes)) + bytes


def read_buffer(bytes, offset):
    length = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return bytes[index:index + length], offset


class Connect(namedtuple('Connect', 'protocol_version last_zxid_seen'
                         ' time_out session_id passwd read_only')):
    """A connection request"""
    type = None

    def serialize(self):
        b = bytearray()
        b.extend(int_long_int_long_struct.pack(
            self.protocol_version, self.last_zxid_seen, self.time_out,
            self.session_id))
        b.extend(write_buffer(self.passwd))
        b.extend([1 if self.read_only else 0])
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        proto_version, timeout, session_id = int_int_long_struct.unpack_from(
            bytes, offset)
        offset += int_int_long_struct.size
        password, offset = read_buffer(bytes, offset)
        return cls(proto_version, 0, timeout, session_id, password, 0), offset


class Close(object):
    __slots__ = ['type']
    type = -11

    @classmethod
    def serialize(cls):
        return ''


class Ping(object):
    __slots__ = ['type']
    type = 11

    @classmethod
    def serialize(cls):
        return ''


class Watch(namedtuple('Watch', 'type state path')):
    @classmethod
    def deserialize(cls, buffer, offset):
        """Given a buffer and the current buffer offset, return the type,
        state, path, and new offset"""
        type, state = int_int_struct.unpack_from(buffer, offset)
        offset += int_int_struct.size
        path, offset = read_string(buffer, offset)
        return cls(type, state, path), offset


def deserialize_reply_header(buffer, offset):
    """Given a buffer and the current buffer offset, return a
    :class:`ReplyHeader` instance and the new offset"""
    new_offset = offset + reply_header_struct.size
    return ReplyHeader._make(
        reply_header_struct.unpack_from(buffer, offset)), new_offset
