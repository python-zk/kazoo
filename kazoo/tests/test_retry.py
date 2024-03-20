from unittest import mock

import pytest

from kazoo import exceptions as ke
from kazoo import retry as kr


def _make_retry(*args, **kwargs):
    """Return a KazooRetry instance with a dummy sleep function."""

    def _sleep_func(_time):
        pass

    return kr.KazooRetry(*args, sleep_func=_sleep_func, **kwargs)


def _make_try_func(times=1):
    """Returns a function that raises ForceRetryError `times` time before
    returning None.
    """
    callmock = mock.Mock(
        side_effect=[kr.ForceRetryError("Failed!")] * times + [None],
    )
    return callmock


def test_call():
    retry = _make_retry(delay=0, max_tries=2)
    func = _make_try_func()
    retry(func, "foo", bar="baz")
    assert func.call_args_list == [
        mock.call("foo", bar="baz"),
        mock.call("foo", bar="baz"),
    ]


def test_reset():
    retry = _make_retry(delay=0, max_tries=2)
    func = _make_try_func()
    retry(func)
    assert (
        func.call_count == retry._attempts + 1 == 2
    ), "Called 2 times, failed _attempts 1, succeeded 1"
    retry.reset()
    assert retry._attempts == 0


def test_too_many_tries():
    retry = _make_retry(delay=0, max_tries=10)
    func = _make_try_func(times=999)
    with pytest.raises(kr.RetryFailedError):
        retry(func)
    assert (
        func.call_count == retry._attempts == 10
    ), "Called 10 times, failed _attempts 10"


def test_maximum_delay():
    retry = _make_retry(delay=10, max_tries=100, max_jitter=0)
    func = _make_try_func(times=2)
    retry(func)
    assert func.call_count == 3, "Called 3 times, 2 failed _attemps"
    assert retry._cur_delay == 10 * 2**2, "Normal exponential backoff"
    retry.reset()
    func = _make_try_func(times=10)
    retry(func)
    assert func.call_count == 11, "Called 11 times, 10 failed _attemps"
    assert retry._cur_delay == 60, "Delay capped by maximun"
    # gevent's sleep function is picky about the type
    assert isinstance(retry._cur_delay, float)


def test_copy():
    retry = _make_retry()
    rcopy = retry.copy()
    assert rcopy is not retry
    assert rcopy.sleep_func is retry.sleep_func


def test_connection_closed():
    retry = _make_retry()

    def testit():
        raise ke.ConnectionClosedError

    with pytest.raises(ke.ConnectionClosedError):
        retry(testit)


def test_session_expired():
    retry = _make_retry(max_tries=1)

    def testit():
        raise ke.SessionExpiredError

    with pytest.raises(kr.RetryFailedError):
        retry(testit)

    retry = _make_retry(max_tries=1, ignore_expire=False)

    with pytest.raises(ke.SessionExpiredError):
        retry(testit)
