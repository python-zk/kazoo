"""Zookeeper Protocol Implementation"""
import logging

from kazoo.exceptions import AuthFailedError
from kazoo.exceptions import ConnectionDropped
from kazoo.exceptions import EXCEPTIONS
from kazoo.protocol.serialization import int_struct
from kazoo.protocol.serialization import int_int_struct
from kazoo.protocol.serialization import deserialize_watcher_event
from kazoo.protocol.serialization import deserialize_reply_header
from kazoo.protocol.serialization import Connect
from kazoo.protocol.states import KeeperState


log = logging.getlog(__name__)


def proto_reader(client, s, reader_started, reader_done, read_timeout):
    reader_started.set()

    while True:
        try:
            header, buffer, offset = _read_header(s, read_timeout)
            if header.xid == -2:
                log.debug('Received PING')
                continue
            elif header.xid == -4:
                log.debug('Received AUTH')
                continue
            elif header.xid == -1:
                log.debug('Received EVENT')
                wtype, state, path, offset = deserialize_watcher_event(
                    buffer, offset)

                watchers = set()
                with client._state_lock:
                    if wtype == 1:
                        watchers |= client._data_watchers.pop(path, set())
                        watchers |= client._exists_watchers.pop(path, set())

                        event = lambda: map(lambda w: w.node_created(path),
                                            watchers)
                    elif wtype == 2:
                        watchers |= client._data_watchers.pop(path, set())
                        watchers |= client._exists_watchers.pop(path, set())
                        watchers |= client._child_watchers.pop(path, set())

                        event = lambda: map(lambda w: w.node_deleted(path),
                                            watchers)
                    elif wtype == 3:
                        watchers |= client._data_watchers.pop(path, set())
                        watchers |= client._exists_watchers.pop(path, set())

                        event = lambda: map(lambda w: w.data_changed(path),
                                            watchers)
                    elif wtype == 4:
                        watchers |= client._child_watchers.pop(path, set())

                        event = lambda: map(lambda w: w.children_changed(path),
                                            watchers)
                    else:
                        log.warn('Received unknown event %r', type)
                        continue

                client._events.put(event)
            else:
                log.debug('Reading for header %r', header)

                request, response, callback, xid = client._pending.get()

                if header.zxid and header.zxid > 0:
                    client.last_zxid = header.zxid
                if header.xid != xid:
                    raise RuntimeError('xids do not match, expected %r received %r', xid, header.xid)

                callback_exception = None
                if header.err:
                    callback_exception = EXCEPTIONS[header.err]()
                    log.debug('Received error %r', callback_exception)
                elif response:
                    response.deserialize(buffer, 'response')
                    log.debug('Received response: %r', response)

                try:
                    callback(callback_exception)
                except Exception as e:
                    log.exception(e)

                if isinstance(response, CloseResponse):
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

    writer_done = False
    retry = client.retry_sleeper.copy()

    for host, port in client.hosts:
        s = client.handler.socket()

        client._state = KeeperState.CONNECTING

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
                    request, response, callback = client._queue.peek(
                        True, read_timeout / 2000.0)
                    log.debug('Sending %r', request)

                    xid += 1
                    log.debug('xid: %r', xid)

                    _submit(s, request, connect_timeout, xid)

                    if isinstance(request, CloseRequest):
                        log.debug('Received close request, closing')
                        writer_done = True

                    client._queue.get()
                    client._pending.put((request, response, callback, xid))
                except client.handler.empty:
                    log.debug('Queue timeout.  Sending PING')
                    _submit(s, PingRequest(), connect_timeout, -2)
                except Exception as e:
                    log.exception(e)
                    break

            log.debug('Waiting for reader to read close response')
            reader_done.wait()
            log.info('Closing connection to %s:%s', host, port)

            if writer_done:
                client._close(KeeperState.CLOSED)
                break
        except ConnectionDropped:
            log.warning('Connection dropped')
            client._events.put(lambda: client._default_watcher.connection_dropped())
            retry.increment()
        except AuthFailedError:
            log.warning('AUTH_FAILED closing')
            client._close(KeeperState.AUTH_FAILED)
            break
        except Exception as e:
            log.warning(e)
            retry.increment()
        finally:
            if not writer_done:
                # The read thread will close the socket since there
                # could be a number of pending requests whose response
                # still needs to be read from the socket.
                s.close()

    log.debug('Writer stopped')


def _connect(client, s, host, port):
    log.info('Connecting to %s:%s', host, port)
    log.debug('    Using session_id: %r session_passwd: 0x%s',
              client.session_id, _hex(client.session_passwd))

    s.connect((host, port))
    s.setblocking(0)

    log.debug('Connected')

    connect = Connect(
        0,
        client.last_zxid,
        int(client.session_timeout * 1000),
        client.session_passwd,
        client.read_only)

    connect_result, zxid = _invoke(client, s, client.session_timeout, None,
                                   connect.serialize(), connect.deserialize)

    if connect_result.time_out < 0:
        log.error('Session expired')
        client._events.put(lambda: client._default_watcher.session_expired(client.session_id))
        client._state = KeeperState.EXPIRED_SESSION
        raise RuntimeError('Session expired')

    if zxid:
        client.last_zxid = zxid
    client.session_id = connect_result.session_id
    negotiated_session_timeout = connect_result.time_out
    connect_timeout = negotiated_session_timeout / len(client.hosts)
    read_timeout = negotiated_session_timeout * 2.0 / 3.0
    client.session_passwd = connect_result.passwd
    log.debug('Session created, session_id: %r session_passwd: 0x%s', client.session_id, _hex(client.session_passwd))
    log.debug('    negotiated session timeout: %s', negotiated_session_timeout)
    log.debug('    connect timeout: %s', connect_timeout)
    log.debug('    read timeout: %s', read_timeout)
    client._events.put(lambda: client._default_watcher.session_connected(client.session_id, client.session_passwd, client.read_only))
    client._state = KeeperState.CONNECTED

    for scheme, auth in client.auth_data:
        ap = AuthPacket(0, scheme, auth)
        zxid = _invoke(s, connect_timeout, ap, xid=-4)
        if zxid:
            client.last_zxid = zxid
    return read_timeout, connect_timeout


def _invoke(client, socket, timeout, request_type, request_bytes,
            response_deserializer=None, xid=None):
    b = bytearray()
    if xid and request_type:
        b.extend(int_int_struct.pack(xid, request_type))
    elif xid:
        b.extend(int_struct.pack(xid))
    elif request_type:
        b.extend(int_struct.pack(request_type))
    b.extend(request_bytes)
    b = int_struct.pack(len(b)) + b
    _write(client, socket, b, timeout)

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

    msg = _read(socket, 4, timeout)
    length = int_struct.unpack_from(msg, 0)[0]

    msg = _read(socket, length, timeout)

    if response_deserializer:
        log.debug('Read response %s', response_deserializer(msg))
        return response_deserializer(msg), zxid

    return zxid


def _hex(bindata):
    return bindata.encode('hex')


def _submit(client, socket, request_type, request_buffer, timeout, xid=None):
    b = bytearray()
    b.extend(int_struct.pack(xid))
    if request_type:
        b.extend(int_struct.pack(request_type))
    b += request_buffer
    b = int_struct.pack(len(b)) + b
    _write(client, socket, b, timeout)


def _write(client, socket, msg, timeout):
    sent = 0
    select = client.handler.select
    while sent < len(msg):
        _, ready_to_write, _ = select([], [socket], [], timeout)
        bytes_sent = ready_to_write[0].send(msg[sent:])
        if not sent:
            raise ConnectionDropped('socket connection broken')
        sent += bytes_sent


def _read_header(client, socket, timeout):
    b = _read(client, socket, 4, timeout)
    length = int_struct.unpack(b)[0]
    b = _read(client, socket, length, timeout)
    header, offset = deserialize_reply_header(b, 0)
    return header, b, offset


def _read(client, socket, length, timeout):
    msg = ''
    select = client.handler.select
    while len(msg) < length:
        ready_to_read, _, _ = select([socket], [], [], timeout)
        chunk = ready_to_read[0].recv(length - len(msg))
        if chunk == '':
            raise ConnectionDropped('socket connection broken')
        msg = msg + chunk
    return msg
