"""Child and Data watching higher level API's

"""
import logging

from kazoo.client import KazooState

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
            print "Data is %s" % data
            print "Version is %s" % stat.version

        # Above function is called immediately and prints

    """
    def __init__(self, client, path, func=None,
                 allow_session_lost=True):
        """Create a children watcher for a path

        :param client: A zookeeper client
        :type client: :class:`~kazoo.client.KazooClient`
        :param path: The path to watch for children on
        :type path: str
        :param func: Function to call initially and every time the
                     children change. `func` will be called with a
                     tuple, the value of the node and a
                     :class:`~kazoo.client.ZnodeStat` instance
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
        self._run_lock = client._handler.lock_object()
        self._prior_data = ()

        # Register our session listener if we're going to resume
        # across session losses
        if func:
            if allow_session_lost:
                self._client.add_listener(self._session_watcher)
            self._get_data()

    def __call__(self, func):
        """Callable version for use as a decorator

        :param func: Function to call initially and every time the
                     children change. `func` will be called with a
                     tuple, the value of the node and a
                     :class:`~kazoo.client.ZnodeStat` instance
        :type func: callable

        """
        self._func = func

        if self._allow_session_lost:
            self._client.add_listener(self._session_watcher)
        self._get_data()
        return func

    def _get_data(self):
        with self._run_lock:  # Ensure this runs one at a time
            if self._stopped:
                return

            data, stat = self._client.retry(self._client.get,
                                            self._path, self._watcher)
            if not self._watch_established:
                self._watch_established = True

                # If we already had data, and it hasn't changed, this is a
                # session re-establishment and nothing changed, don't call the
                # func
                if self._prior_data and \
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

    def _session_watcher(self, state):
        if state == KazooState.LOST:
            self._watch_established = False
        elif state == KazooState.CONNECTED and \
             not self._watch_established and not self._stopped:
            self._get_data()


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

        :param client: A zookeeper client
        :type client: :class:`~kazoo.client.KazooClient`
        :param path: The path to watch for children on
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
        self._run_lock = client._handler.lock_object()
        self._prior_children = None

        # Register our session listener if we're going to resume
        # across session losses
        if func:
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
        if state == KazooState.LOST:
            self._watch_established = False
        elif state == KazooState.CONNECTED and \
             not self._watch_established and not self._stopped:
            self._get_children()
