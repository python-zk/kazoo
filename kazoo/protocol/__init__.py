"""Zookeeper Protocol Implementation"""
import errno
import logging
import socket

from kazoo.exceptions import AuthFailedError
from kazoo.exceptions import ConnectionDropped
from kazoo.exceptions import EXCEPTIONS
from kazoo.exceptions import SessionExpiredError
from kazoo.protocol.serialization import int_struct
from kazoo.protocol.serialization import ReplyHeader
from kazoo.protocol.serialization import Close
from kazoo.protocol.serialization import Connect
from kazoo.protocol.serialization import Ping
from kazoo.protocol.serialization import Watch
from kazoo.protocol.serialization import GetChildren
from kazoo.protocol.serialization import Exists
from kazoo.protocol.states import KeeperState
from kazoo.protocol.states import WatchedEvent
from kazoo.protocol.states import Callback
from kazoo.protocol.states import EVENT_TYPE_MAP
from kazoo.protocol.paths import _prefix_root

log = logging.getLogger(__name__)


def proto_reader(client, s, reader_started, reader_done, read_timeout):
    reader_started.set()

    while True:
        try:
            header, buffer, offset = _read_header(client, s, read_timeout)
            if header.xid == -2:
                # log.debug('Received PING')
                continue
            elif header.xid == -4:
                log.debug('Received AUTH')
                continue
            elif header.xid == -1:
                watch, offset = Watch.deserialize(buffer, offset)
                path = watch.path
                log.debug('Received EVENT: %s', watch)

                watchers = set()
                with client._state_lock:
                    # Ignore watches if we've been stopped
                    if client._stopped.is_set():
                        continue

                    if watch.type in (1, 2, 3):
                        watchers |= client._data_watchers.pop(path, set())
                    elif watch.type == 4:
                        watchers |= client._child_watchers.pop(path, set())
                    else:
                        log.warn('Received unknown event %r', watch.type)
                        continue
                    ev = WatchedEvent(EVENT_TYPE_MAP[watch.type],
                                      client._state, path)

                client.handler.dispatch_callback(
                    Callback('watch', lambda: map(lambda w: w(ev), watchers),
                             ())
                )
            else:
                log.debug('Reading for header %r', header)
                request, async_object, xid = client._pending.get()

                if header.zxid and header.zxid > 0:
                    client.last_zxid = header.zxid
                if header.xid != xid:
                    raise RuntimeError('xids do not match, expected %r '
                                       'received %r', xid, header.xid)

                exists_request = isinstance(request, Exists)
                if header.err and not exists_request:
                    callback_exception = EXCEPTIONS[header.err]()
                    log.debug('Received error %r', callback_exception)
                    if async_object:
                        async_object.set_exception(callback_exception)
                elif request and async_object:
                    if exists_request and header.err == -101:
                        # It's a NoNodeError, which is fine for an exists
                        # request
                        async_object.set(False)
                    else:
                        response = request.deserialize(buffer, offset)
                        log.debug('Received response: %r', response)
                        async_object.set(response)

                    # Determine if watchers should be registered
                    with client._state_lock:
                        if (not client._stopped.is_set() and
                            hasattr(request, 'watcher')):
                            if isinstance(request, GetChildren):
                                client._child_watchers[request.path].add(
                                    request.watcher)
                            else:
                                client._data_watchers[request.path].add(
                                    request.watcher)

                if isinstance(request, Close):
                    log.debug('Read close response')
                    s.close()
                    reader_done.set()
                    break
        except ConnectionDropped:
            log.debug('Connection dropped for reader')
            break
        except Exception as e:
            log.exception(e)
            break

    log.debug('Reader stopped')


def proto_writer(client):
    log.debug('Starting writer')
    retry = client.retry_sleeper.copy()
    while not client._stopped.is_set():
        # If the connect_loop returns False, stop retrying
        if connect_loop(client, retry) is False:
            break

        # Still going, increment our retry then go through the
        # list of hosts again
        if not client._stopped.is_set():
            retry.increment()
    log.debug('Writer stopped')
    client._writer_stopped.set()


def connect_loop(client, retry):
    writer_done = False
    for host, port in client.hosts:
        s = client.handler.socket()

        if client._state != KeeperState.CONNECTING:
            client._session_callback(KeeperState.CONNECTING)

        try:
            read_timeout, connect_timeout = _connect(
                client, s, host, port)

            # Now that connection is good, reset the retries
            retry.reset()

            reader_started = client.handler.event_object()
            reader_done = client.handler.event_object()

            client.handler.spawn(proto_reader, client, s,
                                 reader_started, reader_done, read_timeout)
            reader_started.wait()

            xid = 0
            while not writer_done:
                try:
                    request, async_object = client._queue.peek(
                        True, read_timeout / 2000.0)
                    xid += 1
                    log.debug('xid: %r', xid)

                    _submit(client, s, request, connect_timeout, xid)

                    if isinstance(request, Close):
                        log.debug('Received close request, closing')
                        writer_done = True

                    client._queue.get()
                    client._pending.put((request, async_object, xid))
                except client.handler.empty:
                    # log.debug('Queue timeout.  Sending PING')
                    _submit(client, s, Ping, connect_timeout, -2)
                except Exception as e:
                    log.exception(e)
                    break

            log.debug('Waiting for reader to read close response')
            reader_done.wait()
            log.info('Closing connection to %s:%s', host, port)

            if writer_done:
                client._session_callback(KeeperState.CLOSED)
                return False
        except ConnectionDropped:
            log.warning('Connection dropped')
            client._session_callback(KeeperState.CONNECTING)
        except AuthFailedError:
            log.warning('AUTH_FAILED closing')
            client._session_callback(KeeperState.AUTH_FAILED)
            return False
        except SessionExpiredError:
            log.warning('Session has expired')
            client._session_callback(KeeperState.EXPIRED_SESSION)
            if async_obj[0]:
                async_obj[0].set_exception(AuthFailedError())
        except Exception as e:
            log.exception(e)
            raise
        finally:
            if not writer_done:
                # The read thread will close the socket since there
                # could be a number of pending requests whose response
                # still needs to be read from the socket.
                s.close()


def _connect(client, s, host, port):
    log.info('Connecting to %s:%s', host, port)
    log.debug('    Using session_id: %r session_passwd: 0x%s',
              client._session_id, client._session_passwd.encode('hex'))

    s.connect((host, port))
    s.setblocking(0)

    connect = Connect(0, client.last_zxid, client._session_timeout,
                      client._session_id or 0, client._session_passwd,
                      client.read_only)

    connect_result, zxid = _invoke(client, s, client._session_timeout, connect)

    if connect_result.time_out <= 0:
        raise SessionExpiredError("Session has expired")

    if zxid:
        client.last_zxid = zxid

    # Load return values
    client._session_id = connect_result.session_id
    negotiated_session_timeout = connect_result.time_out
    connect_timeout = negotiated_session_timeout / len(client.hosts)
    read_timeout = negotiated_session_timeout * 2.0 / 3.0
    client._session_passwd = connect_result.passwd
    log.debug('Session created, session_id: %r session_passwd: 0x%s\n'
              '    negotiated session timeout: %s\n'
              '    connect timeout: %s\n'
              '    read timeout: %s', client._session_id,
              client._session_passwd.encode('hex'), negotiated_session_timeout,
              connect_timeout, read_timeout)

    client._session_callback(KeeperState.CONNECTED)

    # for scheme, auth in client.auth_data:
    #     ap = AuthPacket(0, scheme, auth)
    #     zxid = _invoke(s, connect_timeout, ap, xid=-4)
    #     if zxid:
    #         client.last_zxid = zxid
    return read_timeout, connect_timeout


def _invoke(client, socket, timeout, request, xid=None):
    b = bytearray()
    if xid:
        b.extend(int_struct.pack(xid))
    if request.type:
        b.extend(int_struct.pack(request.type))
    b.extend(request.serialize())
    buff = int_struct.pack(len(b)) + b
    _write(client, socket, buff, timeout)

    zxid = None
    if xid:
        header, buffer, offset = _read_header(client, socket, timeout)
        if header.xid != xid:
            raise RuntimeError('xids do not match, expected %r received %r',
                               xid, header.xid)
        if header.zxid > 0:
            zxid = header.zxid
        if header.err:
            callback_exception = EXCEPTIONS[header.err]()
            log.debug('Received error %r', callback_exception)
            raise callback_exception
        return zxid

    msg = _read(client, socket, 4, timeout)
    length = int_struct.unpack(msg)[0]
    msg = _read(client, socket, length, timeout)

    if hasattr(request, 'deserialize'):
        obj, _ = request.deserialize(msg, 0)
        log.debug('Read response %s', obj)
        return obj, zxid

    return zxid


def _submit(client, s, request, timeout, xid=None):
    b = bytearray()
    b.extend(int_struct.pack(xid))
    if request.type:
        b.extend(int_struct.pack(request.type))
    b += request.serialize()
    b = int_struct.pack(len(b)) + b
    _write(client, s, b, timeout)


def _write(client, s, msg, timeout):
    sent = 0
    msg_length = len(msg)
    try:
        while sent < msg_length:
            _, sock, _ = client.handler.select([], [s], [], timeout)
            msg_slice = buffer(msg, sent)
            bytes_sent = sock[0].send(msg_slice)
            if not bytes_sent:
                raise ConnectionDropped('socket connection broken')
            sent += bytes_sent
    except socket.error, e:
        if isinstance(e.args, tuple):
            if e[0] in (errno.EPIPE, errno.ECONNRESET):
                # remote peer disconnected
                raise ConnectionDropped('socket connection dropped')
            else:
                raise


def _read_header(client, s, timeout):
    b = _read(client, s, 4, timeout)
    length = int_struct.unpack(b)[0]
    b = _read(client, s, length, timeout)
    header, offset = ReplyHeader.deserialize(b, 0)
    return header, b, offset


def _read(client, s, length, timeout):
    msgparts = []
    remaining = length
    try:
        while remaining > 0:
            sock, _, _ = client.handler.select([s], [], [], timeout)
            chunk = sock[0].recv(remaining)
            if chunk == '':
                raise ConnectionDropped('socket connection broken')
            msgparts.append(chunk)
            remaining -= len(chunk)
        return b"".join(msgparts)
    except socket.error, e:
        if isinstance(e.args, tuple):
            if e[0] in (errno.EPIPE, errno.ECONNRESET):
                # remote peer disconnected
                raise ConnectionDropped('socket connection dropped')
            else:
                raise
