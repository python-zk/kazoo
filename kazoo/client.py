"""Kazoo Zookeeper Client"""
import inspect
import logging
import random
from collections import namedtuple
from functools import partial
from os.path import split

import zookeeper

from kazoo.exceptions import BadArgumentsException
from kazoo.exceptions import ConfigurationError
from kazoo.exceptions import ZookeeperStoppedError
from kazoo.exceptions import NoNodeException
from kazoo.exceptions import err_to_exception
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.recipe.lock import Lock
from kazoo.recipe.party import Party
from kazoo.recipe.party import ShallowParty
from kazoo.recipe.election import Election
from kazoo.retry import KazooRetry
from kazoo.klog import setup_logging

log = logging.getLogger(__name__)

ZK_OPEN_ACL_UNSAFE = {"perms": zookeeper.PERM_ALL, "scheme": "world",
                       "id": "anyone"}
ZK_STATES = {}
ZK_TYPES = {}
for name, val in zookeeper.__dict__.items():
    if name.endswith('_STATE'):
        ZK_STATES[val] = name
    if name.endswith('EVENT'):
        ZK_TYPES[val] = name


## Client State and Event objects

class KazooState(object):
    """High level connection state values

    States inspired by Netflix Curator.

    .. attribute:: SUSPENDED

        The connection has been lost but may be recovered. We should
        operate in a "safe mode" until then.

    .. attribute:: CONNECTED

        The connection is alive and well.

    .. attribute:: LOST

        The connection has been confirmed dead. Any ephemeral nodes
        will need to be recreated upon re-establishing a connection.

    """
    SUSPENDED = "SUSPENDED"
    CONNECTED = "CONNECTED"
    LOST = "LOST"


class KeeperState(object):
    """Zookeeper State

    Represents the Zookeeper state. Watch functions will receive a
    :class:`KeeperState` attribute as their state argument.

    .. attribute:: ASSOCIATING

        The Zookeeper ASSOCIATING state

    .. attribute:: AUTH_FAILED

        Authentication has failed, this is an unrecoverable error.

    .. attribute:: CONNECTED

        Zookeeper is connected.

    .. attribute:: CONNECTING

        Zookeeper is currently attempting to establish a connection.

    .. attribute:: EXPIRED_SESSION

        The prior session was invalid, all prior ephemeral nodes are
        gone.

    """
    ASSOCIATING = zookeeper.ASSOCIATING_STATE
    AUTH_FAILED = zookeeper.AUTH_FAILED_STATE
    CONNECTED = zookeeper.CONNECTED_STATE
    CONNECTING = zookeeper.CONNECTING_STATE
    EXPIRED_SESSION = zookeeper.EXPIRED_SESSION_STATE


class EventType(object):
    """Zookeeper Event

    Represents a Zookeeper event. Events trigger watch functions which
    will receive a :class:`EventType` attribute as their event
    argument.

    .. attribute:: NOTWATCHING

        This event type was added to Zookeeper in the event that
        watches get overloaded. It's never been used though and will
        likely be removed in a future Zookeeper version. **This event
        will never actually be set, don't bother testing for it.**

    .. attribute:: SESSION

        A Zookeeper session event. Watch functions do not receive
        session events. A session event watch can be registered with
        :class:`KazooClient` during creation that can receive these
        events. It's recommended to add a listener for connection state
        changes instead.

    .. attribute:: CREATED

        A node has been created.

    .. attribute:: DELETED

        A node has been deleted.

    .. attribute:: CHANGED

        The data for a node has changed.

    .. attribute:: CHILD

        The children under a node have changed (a child was added or
        removed). This event does not indicate the data for a child
        node has changed, which must have its own watch established.

    """
    NOTWATCHING = zookeeper.NOTWATCHING_EVENT
    SESSION = zookeeper.SESSION_EVENT
    CREATED = zookeeper.CREATED_EVENT
    DELETED = zookeeper.DELETED_EVENT
    CHANGED = zookeeper.CHANGED_EVENT
    CHILD = zookeeper.CHILD_EVENT


class WatchedEvent(namedtuple('WatchedEvent', ('type', 'state', 'path'))):
    """A change on ZooKeeper that a Watcher is able to respond to.

    The :class:`WatchedEvent` includes exactly what happened, the
    current state of ZooKeeper, and the path of the znode that was
    involved in the event. An instance of :class:`WatchedEvent` will be
    passed to registered watch functions.

    .. attribute:: type

        A :class:`EventType` attribute indicating the event type.

    .. attribute:: state

        A :class:`KeeperState` attribute indicating the Zookeeper
        state.

    .. attribute:: path

        The path of the node for the watch event.

    """


class ZnodeStat(namedtuple('ZnodeStat', ('aversion', 'ctime', 'cversion',
                                         'czxid', 'dataLength',
                                         'ephemeralOwner', 'mtime', 'mzxid',
                                         'numChildren', 'pzxid', 'version'))):
    """A ZnodeStat structure with convenience properties

    When getting the value of a node from Zookeeper, the properties for
    the node known as a "Stat structure" will be retrieved. The
    :class:`ZnodeStat` object provides access to the standard Stat
    properties and additional properties that are more readable and use
    Python time semantics (seconds since epoch instead of ms).

    .. note::

        The original Zookeeper Stat name is in parens next to the name
        when it differs from the convenience attribute. These are **not
        functions**, just attributes.

    .. attribute:: creation_transaction_id (czxid)

        The transaction id of the change that caused this znode to be
        created.

    .. attribute:: last_modified_transaction_id (mzxid)

        The transaction id of the change that last modified this znode.

    .. attribute:: created (ctime)

        The time in seconds from epoch when this node was created.
        (ctime is in milliseconds)

    .. attribute:: last_modified (mtime)

        The time in seconds from epoch when this znode was last
        modified. (mtime is in milliseconds)

    .. attribute:: version

        The number of changes to the data of this znode.

    .. attribute:: acl_version (aversion)

        The number of changes to the ACL of this znode.

    .. attribute:: owner_session_id (ephemeralOwner)

        The session id of the owner of this znode if the znode is an
        ephemeral node. If it is not an ephemeral node, it will be
        `None`. (ephemeralOwner will be 0 if it is not ephemeral)

    .. attribute:: data_length (dataLength)

        The length of the data field of this znode.

    .. attribute:: children_count (numChildren)

        The number of children of this znode.

    """
    @property
    def acl_version(self):
        return self.aversion

    @property
    def children_version(self):
        return self.cversion

    @property
    def created(self):
        return self.ctime / 1000.0

    @property
    def last_modified(self):
        return self.mtime / 1000.0

    @property
    def owner_session_id(self):
        return self.ephemeralOwner or None

    @property
    def creation_transaction_id(self):
        return self.czxid

    @property
    def last_modified_transaction_id(self):
        return self.mzxid

    @property
    def data_length(self):
        return self.dataLength

    @property
    def children_count(self):
        return self.numChildren


class Callback(namedtuple('Callback', ('type', 'func', 'args'))):
    """A callback that is handed to a handler for dispatch

    :param type: Type of the callback, can be 'session' or 'watch'
    :param func: Callback function
    :param args: Argument list for the callback function

    """


## Client Callbacks

def _generic_callback(async_result, handle, code, *args):
    if code != zookeeper.OK:
        exc = err_to_exception(code)
        async_result.set_exception(exc)
    else:
        if not args:
            result = None
        elif len(args) == 1:
            if isinstance(args[0], dict):
                # It's a node struct, put it in a ZnodeStat
                result = ZnodeStat(**args[0])
            else:
                result = args[0]
        else:
            # if there's two, the second is a stat object
            args = list(args)
            if len(args) == 2 and isinstance(args[1], dict):
                args[1] = ZnodeStat(**args[1])
            result = tuple(args)

        async_result.set(result)


def _exists_callback(async_result, handle, code, stat):
    if code not in (zookeeper.OK, zookeeper.NONODE):
        exc = err_to_exception(code)
        async_result.set_exception(exc)
    else:
        async_result.set(stat)


class KazooClient(object):
    """An Apache Zookeeper Python wrapper supporting alternate callback
    handlers and high-level functionality

    Watch functions registered with this class will not get session
    events, unlike the default Zookeeper watch's. They will also be
    called with a single argument, a :class:`WatchedEvent` instance.

    """
    # for testing purposes
    zookeeper = zookeeper

    def __init__(self, hosts='127.0.0.1:2181', watcher=None,
                 timeout=10.0, client_id=None, max_retries=None, retry_delay=0.1,
                 retry_backoff=2, retry_jitter=0.8, handler=None,
                 default_acl=None):
        """Create a KazooClient instance. All time arguments are in seconds.

        :param hosts: Comma-separated list of hosts to connect to
                      (e.g. 127.0.0.1:2181,127.0.0.1:2182).
        :param watcher: Set a default watcher. This will be called by
                        the actual default watcher that
                        :class:`KazooClient` establishes.
        :param timeout: The longest to wait for a Zookeeper connection.
        :param client_id: A Zookeeper client id, used when
                          re-establishing a prior session connection.
        :param max_retries: Maximum retries when using the
                            :meth:`KazooClient.retry` method.
        :param retry_delay: Initial delay when retrying a call.
        :param retry_backoff: Backoff multiplier between retry attempts.
                              Defaults to 2 for exponential backoff.
        :param retry_jitter: How much jitter delay to introduce per call. An
                             amount of time up to this will be added per retry
                             call to avoid hammering the server.
        :param handler: An instance of a class implementing the
                        :class:`~kazoo.interfaces.IHandler` interface
                        for callback handling.
        :param default_acl: A default ACL used on node creation.


        Retry parameters will be used for connection establishment attempts
        and reconnects.

        """
        from kazoo.recipe.barrier import Barrier
        from kazoo.recipe.barrier import DoubleBarrier
        from kazoo.recipe.partitioner import SetPartitioner
        from kazoo.recipe.watchers import ChildrenWatch
        from kazoo.recipe.watchers import DataWatch

        # Record the handler strategy used
        self._handler = handler if handler else SequentialThreadingHandler()
        self.handler = self._handler

        # Check for logging
        using_gevent = 'gevent' in self.handler.name
        setup_logging(use_gevent=using_gevent)

        # Check for chroot
        chroot_check = hosts.split('/', 1)
        if len(chroot_check) == 2:
            self.chroot = '/' + chroot_check[1]
        else:
            self.chroot = None

        # remove any trailing slashes
        self.default_acl = default_acl

        self._hosts = hosts
        self._watcher = watcher
        self._provided_client_id = client_id

        # ZK uses milliseconds
        self._timeout = int(timeout * 1000)

        if inspect.isclass(self.handler):
            raise ConfigurationError("Handler must be an instance of a class, "
                                     "not the class: %s" % self.handler)

        self._handle = None

        # We use events like twitter's client to track current and desired
        # state (connected, and whether to shutdown)
        self._live = self.handler.event_object()
        self._stopped = self.handler.event_object()
        self._stopped.set()

        self._connection_timed_out = False

        self.retry = KazooRetry(
            max_tries=max_retries,
            delay=retry_delay,
            backoff=retry_backoff,
            max_jitter=retry_jitter,
            sleep_func=self.handler.sleep_func
        )

        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.retry_jitter = int(retry_jitter * 100)
        self._connection_attempts = None

        # Curator like simplified state tracking, and listeners for state
        # transitions
        self.state = KazooState.LOST
        self.state_listeners = set()

        # convenience API
        self.Barrier = partial(Barrier, self)
        self.DoubleBarrier = partial(DoubleBarrier, self)
        self.ChildrenWatch = partial(ChildrenWatch, self)
        self.DataWatch = partial(DataWatch, self)
        self.Election = partial(Election, self)
        self.Lock = partial(Lock, self)
        self.Party = partial(Party, self)
        self.SetPartitioner = partial(SetPartitioner, self)
        self.ShallowParty = partial(ShallowParty, self)

    def _safe_call(self, func, async_result, *args, **kwargs):
        """Safely call a zookeeper function and handle errors related
        to a bad zhandle and state bugs

        In older zkpython bindings, a SystemError can arise if some
        functions are called when the session handle is expired. This
        client clears the old handle as soon as possible, but its
        possible a command may run against the old handle that is
        expired resulting in this error being thrown. See
        https://issues.apache.org/jira/browse/ZOOKEEPER-1318

        """
        try:
            return func(self._handle, *args)
        except BadArgumentsException:
            # Validate the path to throw a better except
            if isinstance(args[0], basestring) and not args[0].startswith('/'):
                raise ValueError("invalid path '%s'. must start with /" %
                                 args[0])
            else:
                raise
        except (TypeError, SystemError) as exc:
            # Handle was cleared or isn't set. If it was cleared and we are
            # supposed to be running, it means we had session expiration and
            # are attempting to reconnect. We treat it as a session expiration
            # in that case for appropriate behavior.
            if self._stopped.is_set():
                async_result.set_exception(
                    ZookeeperStoppedError(
                    "The Kazoo client is stopped, call connect before running "
                    "commands that talk to Zookeeper"))
            elif isinstance(exc, SystemError):
                # Set this to the error it should be for appropriate handling
                async_result.set_exception(
                    self.zookeeper.InvalidStateException(
                        "invalid handle state"))
            elif self._handle is None or 'an integer is required' in exc:
                async_result.set_exception(
                    self.zookeeper.SessionExpiredException("session expired"))
            else:
                raise

    def add_listener(self, listener):
        """Add a function to be called for connection state changes

        This function will be called with a :class:`KazooState`
        instance indicating the new connection state.

        """
        if not (listener and callable(listener)):
            raise ConfigurationError("listener must be callable")
        self.state_listeners.add(listener)

    def remove_listener(self, listener):
        """Remove a listener function"""
        self.state_listeners.discard(listener)

    @property
    def connected(self):
        """Returns whether the Zookeeper connection has been established"""
        return self._live.is_set()

    @property
    def client_id(self):
        """Returns the client id for this Zookeeper session if connected"""
        if self._handle is not None:
            return self.zookeeper.client_id(self._handle)
        return None

    def _wrap_session_callback(self, func):
        def wrapper(handle, type, state, path):
            callback = Callback('session', func, (handle, type, state, path))
            self.handler.dispatch_callback(callback)
        return wrapper

    def _wrap_watch_callback(self, func):
        def func_wrapper(*args):
            try:
                func(*args)
            except Exception:
                log.exception("Exception in kazoo callback <func: %s>" % func)

        def wrapper(handle, type, state, path):
            # don't send session events to all watchers
            if type != self.zookeeper.SESSION_EVENT:
                event = WatchedEvent(type, state, path)
                callback = Callback('watch', func_wrapper, (event,))
                self.handler.dispatch_callback(callback)
        return wrapper

    def _make_state_change(self, state):
        # skip if state is current
        if self.state == state:
            return
        self.state = state

        # Create copy of listeners for iteration in case one needs to
        # remove itself
        for listener in list(self.state_listeners):
            try:
                remove = listener(state)
                if remove is True:
                    self.remove_listener(listener)
            except Exception:
                log.exception("Error in connection state listener")

    def _session_callback(self, handle, type, state, path):
        log.debug("Client Instance: %s, Handle: %s, Type: %s, State: %s"
                  ", Path: %s", id(self), handle, ZK_TYPES.get(type, type),
                  ZK_STATES.get(state, state),
                  path)

        if type != EventType.SESSION:
            return

        if self._handle != handle:
            try:
                # latent handle callback from previous connection
                self.zookeeper.close(handle)
            except Exception:
                pass
            return

        if self._stopped.is_set():
            # Any events at this point can be ignored
            return

        if state == KeeperState.CONNECTED:
            log.info("Zookeeper connection established")
            self._live.set()
            self._connection_attempts = None

            # Clear the client id when we successfully connect
            self._provided_client_id = None

            self._make_state_change(KazooState.CONNECTED)
        elif state in (KeeperState.EXPIRED_SESSION,
                       KeeperState.AUTH_FAILED):
            log.info("Zookeeper session lost, state: %s", state)
            self._live.clear()
            self._make_state_change(KazooState.LOST)
            self._handle = None

            # This session callback already runs in the handler so
            # its safe to spawn the start_async call so this function
            # can return immediately
            self.handler.spawn(self._reconnect)
        else:
            log.info("Zookeeper connection lost")
            # Connection lost
            self._live.clear()
            self._make_state_change(KazooState.SUSPENDED)

        if self._watcher:
            self._watcher(WatchedEvent(type, state, path))

    def _safe_close(self):
        if self._handle is not None:
            # Stop the handler
            self.handler.stop()
            zh, self._handle = self._handle, None
            try:
                self.zookeeper.close(zh)
            except self.zookeeper.ZooKeeperException:
                # Corrupt session or otherwise disconnected
                pass
            self._live.clear()
            self._make_state_change(KazooState.LOST)

    def start_async(self):
        """Asynchronously initiate connection to ZK

        :returns: An event object that can be checked to see if the
                  connection is alive.
        :rtype: :class:`~threading.Event` compatible object

        """
        # If we're already connected, ignore
        if self._live.is_set():
            return self._live

        # Make sure we're safely closed
        self._safe_close()

        # We've been asked to connect, clear the stop
        self._stopped.clear()

        # Start the handler
        self.handler.start()

        cb = self._wrap_session_callback(self._session_callback)
        self._connection_attempts = 1
        self._connection_delay = self.retry_delay
        if self._provided_client_id:
            self._handle = self.zookeeper.init(self._hosts, cb, self._timeout,
                self._provided_client_id)
        else:
            self._handle = self.zookeeper.init(self._hosts, cb, self._timeout)
        return self._live
    connect_async = start_async

    def _reconnect(self):
        """Reconnect to Zookeeper, staggering connection attempts"""
        if not self._connection_attempts:
            self._connection_attempts = 1
            self._connection_delay = self.retry_delay
        else:
            if self._connection_attempts == self.max_retries:
                raise self.handler.timeout_exception("Time out reconnecting")
            self._connection_attempts += 1
            jitter = random.randint(0, self.retry_jitter) / 100.0
            self.handler.sleep_func(self._connection_delay + jitter)
            self._connection_delay *= self.retry_delay
        self.start_async()

    def start(self, timeout=15):
        """Initiate connection to ZK

        :param timeout: Time in seconds to wait for connection to
                        succeed.
        :throws: :attr:`~kazoo.interfaces.IHandler.timeout_exception`
                 if the connection wasn't established within `timeout`
                 seconds.

        """
        event = self.start_async()
        event.wait(timeout=timeout)
        if not self.connected:
            # We time-out, ensure we are disconnected
            self.stop()
            raise self.handler.timeout_exception("Connection time-out")
    connect = start

    def stop(self):
        """Gracefully stop this Zookeeper session"""
        self._stopped.set()
        self._safe_close()

    def restart(self):
        """Stop and restart the Zookeeper session."""
        self._safe_close()
        self._stopped.clear()
        self.start()

    def add_auth_async(self, scheme, credential):
        """Asynchronously send credentials to server

        :param scheme: authentication scheme (default supported:
                       "digest")
        :param credential: the credential -- value depends on scheme
        :returns: AsyncResult object set on completion
        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        async_result = self.handler.async_result()
        callback = partial(_generic_callback, async_result)

        self._safe_call(self.zookeeper.add_auth, async_result, scheme,
                        credential, callback)

        # Compensate for io polling bug on auth by running an exists call
        # See https://issues.apache.org/jira/browse/ZOOKEEPER-770
        self.exists_async("/")
        return async_result

    def add_auth(self, scheme, credential):
        """Send credentials to server

        :param scheme: authentication scheme (default supported:
                       "digest")
        :param credential: the credential -- value depends on scheme

        """
        return self.add_auth_async(scheme, credential).get()

    def ensure_path(self, path, acl=None):
        """Recursively create a path if it doesn't exist
        """
        self._inner_ensure_path(path, acl)

    def _inner_ensure_path(self, path, acl):
        if self.exists(path):
            return

        if acl is None and self.default_acl:
            acl = self.default_acl

        parent, node = split(path)

        if node:
            self._inner_ensure_path(parent, acl)
        try:
            self.create_async(path, "", acl=acl).get()
        except self.zookeeper.NodeExistsException:
            # someone else created the node. how sweet!
            pass

    def unchroot(self, path):
        if not self.chroot:
            return path

        if path.startswith(self.chroot):
            return path[len(self.chroot):]
        else:
            return path

    def create_async(self, path, value, acl=None, ephemeral=False,
                     sequence=False):
        """Asynchronously create a ZNode

        :param path: path of node
        :param value: initial value of node
        :param acl: permissions for node
        :param ephemeral: boolean indicating whether node is ephemeral
                          (tied to this session)
        :param sequence: boolean indicating whether path is suffixed
                         with a unique index
        :returns: AsyncResult object set on completion with the real
                  path of the new node
        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if acl is None and self.default_acl:
            acl = self.default_acl

        flags = 0
        if ephemeral:
            flags |= self.zookeeper.EPHEMERAL
        if sequence:
            flags |= self.zookeeper.SEQUENCE
        if acl is None:
            acl = (ZK_OPEN_ACL_UNSAFE,)

        async_result = self.handler.async_result()
        callback = partial(_generic_callback, async_result)

        self._safe_call(self.zookeeper.acreate, async_result, path, value,
                        list(acl), flags, callback)
        return async_result

    def create(self, path, value, acl=None, ephemeral=False, sequence=False,
               makepath=False):
        """Create a ZNode

        :param path: path of node
        :param value: initial value of node
        :param acl: permissions for node
        :param ephemeral: boolean indicating whether node is ephemeral
                          (tied to this session)
        :param sequence: boolean indicating whether path is suffixed
                         with a unique index
        :param makepath: Whether the path should be created if it
                         doesn't exist
        :returns: real path of the new node

        """
        try:
            realpath = self.create_async(path, value, acl=acl,
                ephemeral=ephemeral, sequence=sequence).get()

        except NoNodeException:
            # some or all of the parent path doesn't exist. if makepath is set
            # we will create it and retry. If it fails again, someone must be
            # actively deleting ZNodes and we'd best bail out.
            if not makepath:
                raise

            parent, _ = split(path)

            # using the inner call directly because path is already namespaced
            self._inner_ensure_path(parent, acl)

            # now retry
            realpath = self.create_async(path, value, acl=acl,
                ephemeral=ephemeral, sequence=sequence).get()

        return self.unchroot(realpath)

    def exists_async(self, path, watch=None):
        """Asynchronously check if a node exists

        :param path: path of node
        :param watch: optional watch callback to set for future changes
                      to this path
        :returns: stat of the node if it exists, else None
        :rtype: `dict` or `None`

        """
        async_result = self.handler.async_result()
        callback = partial(_exists_callback, async_result)
        watch_callback = self._wrap_watch_callback(watch) if watch else None

        self._safe_call(self.zookeeper.aexists, async_result, path,
                        watch_callback, callback)
        return async_result

    def exists(self, path, watch=None):
        """Check if a node exists

        :param path: path of node
        :param watch: optional watch callback to set for future changes
                      to this path
        :returns: stat of the node if it exists, else None
        :rtype: `dict` or `None`

        """
        return self.exists_async(path, watch).get()

    def get_async(self, path, watch=None):
        """Asynchronously get the value of a node

        :param path: path of node
        :param watch: optional watch callback to set for future changes
                      to this path
        :returns: AsyncResult set with tuple (value, :class:`ZnodeStat`
                  ) of node on success
        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        async_result = self.handler.async_result()
        callback = partial(_generic_callback, async_result)
        watch_callback = self._wrap_watch_callback(watch) if watch else None

        self._safe_call(self.zookeeper.aget, async_result, path,
                        watch_callback, callback)
        return async_result

    def get(self, path, watch=None):
        """Get the value of a node

        :param path: path of node
        :param watch: optional watch callback to set for future changes
                      to this path
        :returns: tuple (value, :class:`ZnodeStat`) of node

        """
        return self.get_async(path, watch).get()

    def get_children_async(self, path, watch=None):
        """Asynchronously get a list of child nodes of a path

        :param path: path of node to list
        :param watch: optional watch callback to set for future changes
                      to this path
        :returns: AsyncResult set with list of child node names on
                  success
        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        async_result = self.handler.async_result()
        callback = partial(_generic_callback, async_result)
        watch_callback = self._wrap_watch_callback(watch) if watch else None

        self._safe_call(self.zookeeper.aget_children, async_result, path,
                        watch_callback, callback)
        return async_result

    def get_children(self, path, watch=None):
        """Get a list of child nodes of a path

        :param path: path of node to list
        :param watch: optional watch callback to set for future changes
                      to this path
        :returns: list of child node names

        """
        return self.get_children_async(path, watch).get()

    def set_async(self, path, data, version=-1):
        """Set the value of a node

        If the version of the node being updated is newer than the
        supplied version (and the supplied version is not -1), a
        BadVersionException will be raised.

        :param path: path of node to set
        :param data: new data value
        :param version: version of node being updated, or -1
        :returns: AsyncResult set with new node :class:`ZnodeStat` on
                  success
        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        async_result = self.handler.async_result()
        callback = partial(_generic_callback, async_result)

        self._safe_call(self.zookeeper.aset, async_result, path, data, version,
                        callback)
        return async_result

    def set(self, path, data, version=-1):
        """Set the value of a node

        If the version of the node being updated is newer than the
        supplied version (and the supplied version is not -1), a
        BadVersionException will be raised.

        :param path: path of node to set
        :param data: new data value
        :param version: version of node being updated, or -1
        :returns: updated :class:`ZnodeStat` of the node

        """
        return self.set_async(path, data, version).get()

    def delete_async(self, path, version=-1):
        """Asynchronously delete a node

        :param path: path of node to delete
        :param version: version of node to delete, or -1 for any
        :returns: AyncResult set upon completion
        :rtype: :class:`~kazoo.interfaces.IAsyncResult`
        """
        async_result = self.handler.async_result()
        callback = partial(_generic_callback, async_result)

        self._safe_call(self.zookeeper.adelete, async_result, path, version,
                        callback)
        return async_result

    def delete(self, path, version=-1, recursive=False):
        """Delete a node

        :param path: path of node to delete
        :param version: version of node to delete, or -1 for any
        :param recursive: Recursively delete node and all its children,
            defaults to False.
        """
        if recursive:
            self._delete_recursive(path)
        else:
            self.delete_async(path, version).get()

    def _delete_recursive(self, path):
        try:
            children = self.get_children(path)
        except self.zookeeper.NoNodeException:
            return

        if children:
            for child in children:
                if path == "/":
                    child_path = path + child
                else:
                    child_path = path + "/" + child

                self._delete_recursive(child_path)
        try:
            self.delete(path)
        except self.zookeeper.NoNodeException:
            pass
