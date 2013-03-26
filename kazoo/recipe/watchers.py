"""Higher level child and data watching API's.
"""
import logging
import time
from functools import partial

from kazoo.client import KazooState
from kazoo.exceptions import NoNodeError

log = logging.getLogger(__name__)


class DataWatch(object):
    """Watches a node for data updates and calls the specified
    function each time it changes

    The function will also be called the very first time its
    registered to get the data.

    Returning `False` from the registered function will disable
    future data change calls.

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

    """
    def __init__(self, client, path, func=None,
                 allow_session_lost=True, allow_missing_node=False):
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

        """
        self._client = client
        self._path = path
        self._func = func
        self._stopped = False
        self._watch_established = False
        self._allow_session_lost = allow_session_lost
        self._allow_missing_node = allow_missing_node
        self._run_lock = client.handler.lock_object()
        self._prior_data = ()

        # Register our session listener if we're going to resume
        # across session losses
        if func is not None:
            if allow_session_lost:
                self._client.add_listener(self._session_watcher)
            self._get_data()

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
        self._get_data()
        return func

    def _get_data(self, using_exists=False):
        with self._run_lock:  # Ensure this runs one at a time
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
                        # Apparently the node now exists... try again
                        data, stat = self._client.retry(
                            self._client.get, self._path, self._watcher)
                    else:
                        # We return here, to ensure we don't indicate
                        # there was prior data
                        if self._func(data, stat) is False:
                            self._stopped = True
                        return
                else:
                    # This can only happen if _allow_missing_node
                    # is False, because when it is True we use the
                    # ZK 'retry' method, which can't have this exception.
                    self._stopped = True
                    self._func(None, None)
                    return

            if not self._watch_established:
                self._watch_established = True

                # If we had data and it hasn't changed, this is a session
                # re-establishment and nothing changed, so don't call the func
                if self._prior_data:
                    # If the prior session had no data, then it was
                    # watching a node that did not exist.
                    if self._prior_data[1] is None:
                        # If the current session also has no data, then don't
                        # call the func, since nothing has changed.
                        if stat is None:
                            return
                    elif stat is not None and \
                         self._prior_data[1].mzxid == stat.mzxid:
                        return

            self._prior_data = data, stat

            try:
                if self._func(data, stat) is False:
                    self._stopped = True
            except Exception as exc:
                log.exception(exc)
                raise

    def _watcher(self, event):
        self._get_data()

    def _exists_watcher(self, event):
        self._get_data(using_exists=True)

    def _session_watcher(self, state):
        if state in (KazooState.LOST, KazooState.SUSPENDED):
            self._watch_established = False
        elif state == KazooState.CONNECTED and \
             not self._watch_established and not self._stopped:
            self._client.handler.spawn(self._get_data)


class ChildrenWatch(object):
    """Watches a node for children updates and calls the specified
    function each time it changes

    The function will also be called the very first time its
    registered to get children.

    Returning `False` from the registered function will disable
    future children change calls.

    Example with client:

    .. code-block:: python

        @client.ChildrenWatch('/path/to/watch')
        def my_func(children):
            print "Children are %s" % children

        # Above function is called immediately and prints children

    """
    def __init__(self, client, path, func=None,
                 allow_session_lost=True):
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

        The path must already exist for the children watcher to
        run.

        """
        self._client = client
        self._path = path
        self._func = func
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

    def _get_children(self):
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
                if self._func(children) is False:
                    self._stopped = True
            except Exception as exc:
                log.exception(exc)
                raise

    def _watcher(self, event):
        self._get_children()

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
