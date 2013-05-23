"""Higher level child and data watching API's.
"""
import logging
import time
from functools import partial, wraps

from kazoo.client import KazooState
from kazoo.exceptions import ConnectionClosedError, NoNodeError

log = logging.getLogger(__name__)


def _ignore_closed(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectionClosedError:
            pass
    return wrapper


class DataWatch(object):
    """Watches a node for data updates and calls the specified
    function each time it changes

    The function will also be called the very first time its
    registered to get the data.

    Returning `False` from the registered function will disable future
    data change calls. If the client connection is closed (using the
    close command), the DataWatch will no longer get updates.

    Example with client:

    .. code-block:: python

        @client.DataWatch('/path/to/watch')
        def my_func(data, stat):
            print("Data is %s" % data)
            print("Version is %s" % stat.version)

        # Above function is called immediately and prints

    If allow_missing_node=False in __init__, then in the
    event the node does not exist, the function will be called with
    ``(None, None)`` and will not be called again. This should be
    considered the last function call. This behavior will also occur
    if the node is deleted.

    If allow_missing_node=True in __init__, then in the
    event the node does not exist, the function will be called with
    ``(None, None)`` and it will later be called again if the node is
    recreated. In the event the node exists and is later deleted, the
    function will be called with ``(None, None)`` and it will later
    be called if the node is recreated.

    if send_event=True in __init__, then the function will always be
    called with third parameter, ``event``. Upon initial call or when
    recovering a lost session the ``event`` is always ``None``.
    Otherwise it's a :class:`~kazoo.prototype.state.WatchedEvent`
    instance.

    """
    def __init__(self, client, path, func=None,
                 allow_session_lost=True, allow_missing_node=False,
                 send_event=False):
        """Create a data watcher for a path

        :param client: A zookeeper client.
        :type client: :class:`~kazoo.client.KazooClient`
        :param path: The path to watch for data changes on.
        :type path: str
        :param func: Function to call initially and every time the
                     node changes. `func` will be called with a
                     tuple, the value of the node and a
                     :class:`~kazoo.client.ZnodeStat` instance.
        :type func: callable
        :param allow_session_lost: Whether the watch should be
                                   re-registered if the zookeeper
                                   session is lost.
        :type allow_session_lost: bool
        :param allow_missing_node:
            Allow the node to be missing when the watch is initially
            set.
        :type send_event: bool
        :param send_event:
            If set to true, the function called with the
            :class:`~kazoo.prototype.state.WatchedEvent` event sent
            by ZooKeeper or ``None`` (see class documentation).

        """
        self._client = client
        self._path = path
        self._func = func
        self._send_event = send_event
        self._stopped = False
        self._allow_session_lost = allow_session_lost
        self._allow_missing_node = allow_missing_node
        self._run_lock = client.handler.lock_object()
        self._prior_data = ()

        # Register our session listener if we're going to resume
        # across session losses
        if func is not None:
            if allow_session_lost:
                self._client.add_listener(self._session_watcher)
            if self._get_data():
                self._log_func_exception(None, None)

    def __call__(self, func):
        """Callable version for use as a decorator

        :param func: Function to call initially and every time the
                     data changes. `func` will be called with a
                     tuple, the value of the node and a
                     :class:`~kazoo.client.ZnodeStat` instance.
        :type func: callable

        """
        self._func = func

        if self._allow_session_lost:
            self._client.add_listener(self._session_watcher)
        if self._get_data():
            self._log_func_exception(None, None, None)
        return func

    def _log_func_exception(self, data, stat, event=None):
        try:
            # For backwards compatibility, don't send event to the
            # callback unless the send_event is set in constructor
            if self._send_event:
                result = self._func(data, stat, event)
            else:
                result = self._func(data, stat)
            if result is False:
                self._stopped = True
        except Exception as exc:
            log.exception(exc)
            raise

    @_ignore_closed
    def _get_data(self, event=None, using_exists=False):
        # Ensure this runs one at a time, possible because the session
        # watcher may trigger a run
        with self._run_lock:
            if using_exists and self._prior_data:
                # We were able to get data after the first exists
                # check, abort _get_data since there's a separate
                # watcher for it now
                return

            if self._stopped:
                return

            try:
                data, stat = self._client.retry(self._client.get,
                                                self._path, self._watcher)
            except NoNodeError:
                if self._allow_missing_node:
                    data = None

                    # This will set 'stat' to None if the node does not yet
                    # exist.
                    stat = self._client.retry(self._client.exists,
                                              self._path, self._exists_watcher)
                    if stat:
                        return self._get_data()
                else:
                    # This can only happen if _allow_missing_node
                    # is False, because when it is True we use the
                    # ZK 'retry' method, which can't have this exception.
                    self._stopped = True
                    self._log_func_exception(None, None, event)
                    return

            # No prior data, no current data, nothing to do
            if stat is None and not self._prior_data:
                # This returns True so that the first-run can trigger
                # the function itself as needed on first-run
                return True
            elif stat is None:
                # Prior data, node has now been deleted, watch was set if
                # needed
                self._prior_data = ()
            elif self._prior_data and self._prior_data[1].mzxid == stat.mzxid:
                # We have data that hasn't changed
                return
            else:
                # We have new data!
                self._prior_data = data, stat
            self._log_func_exception(data, stat, event)

    def _watcher(self, event):
        self._get_data(event=event)

    def _exists_watcher(self, event):
        self._get_data(event=event, using_exists=True)

    def _session_watcher(self, state):
        if state in (KazooState.LOST, KazooState.SUSPENDED):
            with self._run_lock:
                self._watch_established = False
        elif state == KazooState.CONNECTED and not self._stopped:
            self._client.handler.spawn(self._get_data)


class ChildrenWatch(object):
    """Watches a node for children updates and calls the specified
    function each time it changes

    The function will also be called the very first time its
    registered to get children.

    Returning `False` from the registered function will disable future
    children change calls. If the client connection is closed (using
    the close command), the ChildrenWatch will no longer get updates.

    if send_event=True in __init__, then the function will always be
    called with second parameter, ``event``. Upon initial call or when
    recovering a lost session the ``event`` is always ``None``.
    Otherwise it's a :class:`~kazoo.prototype.state.WatchedEvent`
    instance.

    Example with client:

    .. code-block:: python

        @client.ChildrenWatch('/path/to/watch')
        def my_func(children):
            print "Children are %s" % children

        # Above function is called immediately and prints children

    """
    def __init__(self, client, path, func=None,
                 allow_session_lost=True, send_event=False):
        """Create a children watcher for a path

        :param client: A zookeeper client.
        :type client: :class:`~kazoo.client.KazooClient`
        :param path: The path to watch for children on.
        :type path: str
        :param func: Function to call initially and every time the
                     children change. `func` will be called with a
                     single argument, the list of children.
        :type func: callable
        :param allow_session_lost: Whether the watch should be
                                   re-registered if the zookeeper
                                   session is lost.
        :type allow_session_lost: bool
        :type send_event: bool
        :param send_event: Whether the function should be passed the
                           event sent by ZooKeeper or None upon
                           initialization (see class documentation)

        The path must already exist for the children watcher to
        run.

        """
        self._client = client
        self._path = path
        self._func = func
        self._send_event = send_event
        self._stopped = False
        self._watch_established = False
        self._allow_session_lost = allow_session_lost
        self._run_lock = client.handler.lock_object()
        self._prior_children = None

        # Register our session listener if we're going to resume
        # across session losses
        if func is not None:
            if allow_session_lost:
                self._client.add_listener(self._session_watcher)
            self._get_children()

    def __call__(self, func):
        """Callable version for use as a decorator

        :param func: Function to call initially and every time the
                     children change. `func` will be called with a
                     single argument, the list of children.
        :type func: callable

        """
        self._func = func

        if self._allow_session_lost:
            self._client.add_listener(self._session_watcher)
        self._get_children()
        return func

    @_ignore_closed
    def _get_children(self, event=None):
        with self._run_lock:  # Ensure this runs one at a time
            if self._stopped:
                return

            children = self._client.retry(self._client.get_children,
                                          self._path, self._watcher)
            if not self._watch_established:
                self._watch_established = True

                if self._prior_children is not None and \
                   self._prior_children == children:
                    return

            self._prior_children = children

            try:
                if self._send_event:
                    result = self._func(children, event)
                else:
                    result = self._func(children)
                if result is False:
                    self._stopped = True
            except Exception as exc:
                log.exception(exc)
                raise

    def _watcher(self, event):
        self._get_children(event)

    def _session_watcher(self, state):
        if state in (KazooState.LOST, KazooState.SUSPENDED):
            self._watch_established = False
        elif state == KazooState.CONNECTED and \
             not self._watch_established and not self._stopped:
            self._client.handler.spawn(self._get_children)


class PatientChildrenWatch(object):
    """Patient Children Watch that returns values after the children
    of a node don't change for a period of time

    A separate watcher for the children of a node, that ignores
    changes within a boundary time and sets the result only when the
    boundary time has elapsed with no children changes.

    Example::

        watcher = PatientChildrenWatch(client, '/some/path',
                                       time_boundary=5)
        async_object = watcher.start()

        # Blocks until the children have not changed for time boundary
        # (5 in this case) seconds, returns children list and an
        # async_result that will be set if the children change in the
        # future
        children, child_async = async_object.get()

    .. note::

        This Watch is different from :class:`DataWatch` and
        :class:`ChildrenWatch` as it only returns once, does not take
        a function that is called, and provides an
        :class:`~kazoo.interfaces.IAsyncResult` object that can be
        checked to see if the children have changed later.

    """
    def __init__(self, client, path, time_boundary=30):
        self.client = client
        self.path = path
        self.children = []
        self.time_boundary = time_boundary
        self.children_changed = client.handler.event_object()

    def start(self):
        """Begin the watching process asynchronously

        :returns: An :class:`~kazoo.interfaces.IAsyncResult` instance
                  that will be set when no change has occurred to the
                  children for time boundary seconds.

        """
        self.asy = asy = self.client.handler.async_result()
        self.client.handler.spawn(self._inner_start)
        return asy

    def _inner_start(self):
        try:
            while True:
                async_result = self.client.handler.async_result()
                self.children = self.client.retry(
                    self.client.get_children, self.path,
                    partial(self._children_watcher, async_result))
                self.client.handler.sleep_func(self.time_boundary)

                if self.children_changed.is_set():
                    self.children_changed.clear()
                else:
                    break

            self.asy.set((self.children, async_result))
        except Exception as exc:
            self.asy.set_exception(exc)

    def _children_watcher(self, async, event):
        self.children_changed.set()
        async.set(time.time())
