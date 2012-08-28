"""Kazoo State and Event objects"""


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


class State(object):
    def __init__(self, code, description):
        self.code = code
        self.description = description

    def __eq__(self, other):
        return self.code == other.code

    def __hash__(self):
        return hash(self.code)

    def __str__(self):
        return self.code

    def __repr__(self):
        return '%s()' % self.__class__.__name__


class Connecting(State):
    def __init__(self):
        super(Connecting, self).__init__('CONNECTING', 'Connecting')


class Connected(State):
    def __init__(self):
        super(Connected, self).__init__('CONNECTED', 'Connected')


class ConnectedRO(State):
    def __init__(self):
        super(ConnectedRO, self).__init__('CONNECTED_RO', 'Connected Read-Only')


class AuthFailed(State):
    def __init__(self):
        super(AuthFailed, self).__init__('AUTH_FAILED', 'Authorization Failed')


class Closed(State):
    def __init__(self):
        super(Closed, self).__init__('CLOSED', 'Closed')


class ExpiredSession(State):
    def __init__(self):
        super(ExpiredSession, self).__init__('EXPIRED_SESSION', 'Expired Session')


CONNECTING = Connecting()
CONNECTED = Connected()
CONNECTED_RO = ConnectedRO()
AUTH_FAILED = AuthFailed()
CLOSED = Closed()
EXPIRED_SESSION = ExpiredSession()


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
    AUTH_FAILED = AUTH_FAILED
    CONNECTED = CONNECTED
    CONNECTING = CONNECTING
    CLOSED = CLOSED
    EXPIRED_SESSION = EXPIRED_SESSION


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
