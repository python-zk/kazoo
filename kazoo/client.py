"""Kazoo client

"""
import logging
import os
from collections import namedtuple
from functools import partial

import zookeeper

from kazoo.exceptions import err_to_exception
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.retry import KazooRetry
from kazoo.handlers.util import thread

log = logging.getLogger(__name__)

ZK_OPEN_ACL_UNSAFE = {"perms": zookeeper.PERM_ALL, "scheme": "world",
                       "id": "anyone"}


# Setup Zookeeper logging thread
_logging_pipe = os.pipe()
zookeeper.set_log_stream(os.fdopen(_logging_pipe[1], 'w'))


@thread
def _loggingthread():
    r, w = _logging_pipe
    log = logging.getLogger('ZooKeeper').log
    f = os.fdopen(r)
    levels = dict(ZOO_INFO=logging.INFO,
                  ZOO_WARN=logging.WARNING,
                  ZOO_ERROR=logging.ERROR,
                  ZOO_DEBUG=logging.DEBUG,
                  )
    while 1:
        line = f.readline().strip()
        try:
            if '@' in line:
                level, message = line.split('@', 1)
                level = levels.get(level.split(':')[-1])

                if 'Exceeded deadline by' in line and level == logging.WARNING:
                    level = logging.DEBUG

            else:
                level = None

            if level is None:
                log(logging.INFO, line)
            else:
                log(level, message)
        except Exception, v:
            logging.getLogger('ZooKeeper').exception("Logging error: %s", v)


def validate_path(path):
    if not path.startswith('/'):
        raise ValueError("invalid path '%s'. must start with /" % path)


def _generic_callback(async_result, handle, code, *args):
    if code != zookeeper.OK:
        exc = err_to_exception(code)
        async_result.set_exception(exc)
    else:
        if not args:
            result = None
        elif len(args) == 1:
            result = args[0]
        else:
            result = tuple(args)

        async_result.set(result)


def _exists_callback(async_result, handle, code, stat):
    if code not in (zookeeper.OK, zookeeper.NONODE):
        exc = err_to_exception(code)
        async_result.set_exception(exc)
    else:
        async_result.set(stat)


class KazooState(object):
    """High level connection state values

    States inspired by Netflix Curator

    """
    # The connection has been lost but may be recovered.
    # We should operate in a "safe mode" until then.
    SUSPENDED = "SUSPENDED"

    # The connection is alive and well.
    CONNECTED = "CONNECTED"

    # The connection has been confirmed dead.
    LOST = "LOST"


class KeeperState(object):
    ASSOCIATING = zookeeper.ASSOCIATING_STATE
    AUTH_FAILED = zookeeper.AUTH_FAILED_STATE
    CONNECTED = zookeeper.CONNECTED_STATE
    CONNECTING = zookeeper.CONNECTING_STATE
    EXPIRED_SESSION = zookeeper.EXPIRED_SESSION_STATE


class EventType(object):
    NOTWATCHING = zookeeper.NOTWATCHING_EVENT
    SESSION = zookeeper.SESSION_EVENT
    CREATED = zookeeper.CREATED_EVENT
    DELETED = zookeeper.DELETED_EVENT
    CHANGED = zookeeper.CHANGED_EVENT
    CHILD = zookeeper.CHILD_EVENT


class WatchedEvent(namedtuple('WatchedEvent', ('type', 'state', 'path'))):
    """A change on ZooKeeper that a Watcher is able to respond to.

    The :class:`WatchedEvent` includes exactly what happened, the current state
    of ZooKeeper, and the path of the znode that was involved in the event. An
    instance of :class:`WatchedEvent` will be passed to registered watch
    functions.

    """


class Callback(namedtuple('Callback', ('type', 'func', 'args'))):
    """A callback being triggered

    :param type: Type of the callback, can be 'session' or 'watch'
    :param func: Callback function
    :param args: Argument list for the callback function

    """


class KazooClient(object):
    """An Apache Zookeeper Python wrapper supporting alternate callback
    handlers and high-level functionality

    TODO lots to do:
    * better handling of ZK client session events
    * disconnected state handling
    * the rest of the operations

    """
    def __init__(self, hosts='127.0.0.1:2181', namespace=None, watcher=None,
                 timeout=10.0, client_id=None, max_retries=None, handler=None):
        """Create a KazooClient instance

        :param hosts: List of hosts to connect to
        :param namespace: A Zookeeper namespace for nodes to be used under
        :param watcher: Set a default watcher. This will be called by the
                        actual default watcher that :class:`KazooClient`
                        establishes.
        :param timeout: The longest to wait for a Zookeeper connection
        :param client_id: A Zookeeper client id, used when re-establishing a
                          prior session connection
        :param handler: An object implementing the
                        :class:`~kazoo.interfaces.IHandler` interface for
                        callback handling

        """
        # remove any trailing slashes
        if namespace:
            namespace = namespace.rstrip('/')
        if namespace:
            validate_path(namespace)
        self.namespace = namespace

        self._needs_ensure_path = bool(namespace)

        self._hosts = hosts
        self._watcher = watcher
        self._provided_client_id = client_id

        # ZK uses milliseconds
        self._timeout = int(timeout * 1000)

        self._handler = handler if handler else SequentialThreadingHandler()

        self._handle = None
        self._connected = False
        self._connected_async_result = self._handler.async_result()
        self._connection_timed_out = False

        self.retry = KazooRetry(max_retries)

        self.state = KazooState.LOST
        self.state_listeners = set()

    def _session_watcher(self, event):
        """called by the underlying ZK client when the connection state changes
        """
        if event.type != EventType.SESSION:
            return

        if event.state == KeeperState.CONNECTED:
            self._make_state_change(KazooState.CONNECTED)
        elif event.state in (KeeperState.AUTH_FAILED,
                             KeeperState.EXPIRED_SESSION):
            self._make_state_change(KazooState.LOST)
        elif event.state == KeeperState.CONNECTING:
            self._make_state_change(KazooState.SUSPENDED)

    def _make_state_change(self, state):
        # skip if state is current
        if self.state == state:
            return
        self.state = state

        for listener in self.state_listeners:
            try:
                listener(state)
            except Exception:
                log.exception("Error in connection state listener")

    def _assure_namespace(self):
        if self._needs_ensure_path:
            self.ensure_path('/')
            self._needs_ensure_path = False

    def add_listener(self, listener):
        """Add a function to be called for connection state changes

        This function will be called with a :class:`KazooState` instance
        indicating the new connection state.

        """
        if not (listener and callable(listener)):
            raise ValueError("listener must be callable")
        self.state_listeners.add(listener)

    def remove_listener(self, listener):
        """Remove a listener function
        """
        self.state_listeners.discard(listener)

    @property
    def connected(self):
        """Returns whether the Zookeeper connection has been established"""
        return self._connected

    @property
    def client_id(self):
        """Returns the client id for this Zookeeper session if connected"""
        if self._handle is not None:
            return zookeeper.client_id(self._handle)
        return None

    def _wrap_session_callback(self, func):
        def wrapper(handle, type, state, path):
            event = WatchedEvent(type, state, path)
            callback = Callback('session', func, (event,))
            self._handler.dispatch_callback(callback)
        return wrapper

    def _wrap_watch_callback(self, func):
        def wrapper(handle, type, state, path):
            # don't send session events to all watchers
            if state != zookeeper.SESSION_EVENT:
                event = WatchedEvent(type, state, path)
                callback = Callback('watch', func, (event,))
                self._handler.dispatch_callback(callback)
        return wrapper

    def _session_callback(self, event):
        if event.state == zookeeper.CONNECTED_STATE:
            self._connected = True
        elif event.state == zookeeper.CONNECTING_STATE:
            self._connected = False

        if not self._connected_async_result.ready():
            #close the connection if we already timed out
            if self._connection_timed_out and self._connected:
                self.close()
            else:
                self._connected_async_result.set()

        if self._watcher:
            self._watcher(event)

    def connect_async(self):
        """Asynchronously initiate connection to ZK

        @return: AsyncResult object set on connection success
        @rtype AsyncResult
        """

        cb = self._wrap_session_callback(self._session_callback)
        if self._provided_client_id:
            self._handle = zookeeper.init(self._hosts, cb, self._timeout,
                self._provided_client_id)
        else:
            self._handle = zookeeper.init(self._hosts, cb, self._timeout)

        return self._connected_async_result

    def connect(self, timeout=None):
        """Initiate connection to ZK

        @param timeout: time in seconds to wait for connection to succeed
        """
        async_result = self.connect_async()
        try:
            async_result.get(timeout=timeout)
        except self._handler.timeout_error:
            self._connection_timed_out = True
            raise

    def close(self):
        """Disconnect from ZooKeeper
        """
        if self._connected:
            code = zookeeper.close(self._handle)
            self._handle = None
            self._connected = False
            self._connected_async_result = self._handler.async_result()
            if code != zookeeper.OK:
                raise err_to_exception(code)

    def add_auth_async(self, scheme, credential):
        """Asynchronously send credentials to server

        @param scheme: authentication scheme (default supported: "digest")
        @param credential: the credential -- value depends on scheme
        @return: AsyncResult object set on completion
        @rtype AsyncResult
        """
        async_result = self._handler.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.add_auth(self._handle, scheme, credential, callback)
        return async_result

    def add_auth(self, scheme, credential):
        """Send credentials to server

        @param scheme: authentication scheme (default supported: "digest")
        @param credential: the credential -- value depends on scheme
        """
        return self.add_auth_async(scheme, credential).get()

    def create_async(self, path, value, acl=None, ephemeral=False, sequence=False):
        """Asynchronously create a ZNode

        @param path: path of node
        @param value: initial value of node
        @param acl: permissions for node
        @param ephemeral: boolean indicating whether node is ephemeral (tied to this session)
        @param sequence: boolean indicating whether path is suffixed with a unique index
        @return: AsyncResult object set on completion with the real path of the new node
        @rtype AsyncResult
        """
        flags = 0
        if ephemeral:
            flags |= zookeeper.EPHEMERAL
        if sequence:
            flags |= zookeeper.SEQUENCE
        if acl is None:
            acl = (ZK_OPEN_ACL_UNSAFE,)

        async_result = self._handler.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.acreate(self._handle, path, value, list(acl), flags, callback)
        return async_result

    def create(self, path, value, acl=None, ephemeral=False, sequence=False):
        """Create a ZNode

        @param path: path of node
        @param value: initial value of node
        @param acl: permissions for node
        @param ephemeral: boolean indicating whether node is ephemeral (tied to this session)
        @param sequence: boolean indicating whether path is suffixed with a unique index
        @return: real path of the new node
        """
        return self.create_async(path, value, acl, ephemeral, sequence).get()

    def exists_async(self, path, watch=None):
        """Asynchronously check if a node exists

        @param path: path of node
        @param watch: optional watch callback to set for future changes to this path
        @return stat of the node if it exists, else None
        """
        async_result = self._handler.async_result()
        callback = partial(_exists_callback, async_result)
        watch_callback = self._wrap_watch_callback(watch) if watch else None

        zookeeper.aexists(self._handle, path, watch_callback, callback)
        return async_result

    def exists(self, path, watch=None):
        """Check if a node exists

        @param path: path of node
        @param watch: optional watch callback to set for future changes to this path
        @return stat of the node if it exists, else None
        """
        return self.exists_async(path, watch).get()

    def get_async(self, path, watch=None):
        """Asynchronously get the value of a node

        @param path: path of node
        @param watch: optional watch callback to set for future changes to this path
        @return AsyncResult set with tuple (value, stat) of node on success
        @rtype AsyncResult
        """
        async_result = self._handler.async_result()
        callback = partial(_generic_callback, async_result)
        watch_callback = self._wrap_watch_callback(watch) if watch else None

        zookeeper.aget(self._handle, path, watch_callback, callback)
        return async_result

    def get(self, path, watch=None):
        """Get the value of a node

        @param path: path of node
        @param watch: optional watch callback to set for future changes to this path
        @return tuple (value, stat) of node
        """
        return self.get_async(path, watch).get()

    def get_children_async(self, path, watch=None):
        """Asynchronously get a list of child nodes of a path

        @param path: path of node to list
        @param watch: optional watch callback to set for future changes to this path
        @return: AsyncResult set with list of child node names on success
        @rtype: AsyncResult
        """
        async_result = self._handler.async_result()
        callback = partial(_generic_callback, async_result)
        watch_callback = self._wrap_watch_callback(watch) if watch else None

        zookeeper.aget_children(self._handle, path, watch_callback, callback)
        return async_result

    def get_children(self, path, watch=None):
        """Get a list of child nodes of a path

        @param path: path of node to list
        @param watch: optional watch callback to set for future changes to this path
        @return: list of child node names
        """
        return self.get_children_async(path, watch).get()

    def set_async(self, path, data, version=-1):
        """Set the value of a node

        If the version of the node being updated is newer than the supplied
        version (and the supplied version is not -1), a BadVersionException
        will be raised.

        @param path: path of node to set
        @param data: new data value
        @param version: version of node being updated, or -1
        @return: AsyncResult set with new node stat on success
        @rtype AsyncResult
        """
        async_result = self._handler.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.aset(self._handle, path, data, version, callback)
        return async_result

    def set(self, path, data, version=-1):
        """Set the value of a node

        If the version of the node being updated is newer than the supplied
        version (and the supplied version is not -1), a BadVersionException
        will be raised.

        @param path: path of node to set
        @param data: new data value
        @param version: version of node being updated, or -1
        @return: updated node stat
        """
        return self.set_async(path, data, version).get()

    def delete_async(self, path, version=-1):
        """Asynchronously delete a node

        @param path: path of node to delete
        @param version: version of node to delete, or -1 for any
        @return AyncResult set upon completion
        @rtype AsyncResult
        """
        async_result = self._handler.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.adelete(self._handle, path, version, callback)
        return async_result

    def delete(self, path, version=-1):
        """Delete a node

        @param path: path of node to delete
        @param version: version of node to delete, or -1 for any
        """
        self.delete_async(path, version).get()
