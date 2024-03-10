from unittest import mock

import pytest

from kazoo import retry
from kazoo.handlers import threading
from kazoo.protocol import connection
from kazoo.protocol import states


@mock.patch("kazoo.protocol.connection.ConnectionHandler._expand_client_hosts")
def test_retry_logic(mock_expand):
    mock_client = mock.Mock()
    mock_client._state = states.KeeperState.CLOSED
    mock_client._session_id = None
    mock_client._session_passwd = b"\x00" * 16
    mock_client._stopped.is_set.return_value = False
    mock_client.handler.timeout_exception = threading.KazooTimeoutError
    mock_client.handler.create_connection.side_effect = (
        threading.KazooTimeoutError()
    )
    test_retry = retry.KazooRetry(
        max_tries=6,
        delay=1.0,
        backoff=2,
        max_delay=30.0,
        max_jitter=0.0,
        sleep_func=lambda _x: None,
    )
    test_cnx = connection.ConnectionHandler(
        client=mock_client,
        retry_sleeper=test_retry,
    )
    mock_expand.return_value = [
        ("a", "1.1.1.1", 2181),
        ("b", "2.2.2.2", 2181),
        ("c", "3.3.3.3", 2181),
    ]

    with pytest.raises(retry.RetryFailedError):
        test_retry(test_cnx._connect_loop, test_retry)

    assert mock_client.handler.create_connection.call_args_list[:3] == [
        mock.call(
            address=("1.1.1.1", 2181),
            timeout=1.0,
            use_ssl=mock.ANY,
            keyfile=mock.ANY,
            certfile=mock.ANY,
            ca=mock.ANY,
            keyfile_password=mock.ANY,
            verify_certs=mock.ANY,
        ),
        mock.call(
            address=("2.2.2.2", 2181),
            timeout=1.0,
            use_ssl=mock.ANY,
            keyfile=mock.ANY,
            certfile=mock.ANY,
            ca=mock.ANY,
            keyfile_password=mock.ANY,
            verify_certs=mock.ANY,
        ),
        mock.call(
            address=("3.3.3.3", 2181),
            timeout=1.0,
            use_ssl=mock.ANY,
            keyfile=mock.ANY,
            certfile=mock.ANY,
            ca=mock.ANY,
            keyfile_password=mock.ANY,
            verify_certs=mock.ANY,
        ),
    ], "All hosts are first tried with the lowest timeout value"
    assert mock_client.handler.create_connection.call_args_list[-3:] == [
        mock.call(
            address=("1.1.1.1", 2181),
            timeout=30.0,
            use_ssl=mock.ANY,
            keyfile=mock.ANY,
            certfile=mock.ANY,
            ca=mock.ANY,
            keyfile_password=mock.ANY,
            verify_certs=mock.ANY,
        ),
        mock.call(
            address=("2.2.2.2", 2181),
            timeout=30.0,
            use_ssl=mock.ANY,
            keyfile=mock.ANY,
            certfile=mock.ANY,
            ca=mock.ANY,
            keyfile_password=mock.ANY,
            verify_certs=mock.ANY,
        ),
        mock.call(
            address=("3.3.3.3", 2181),
            timeout=30.0,
            use_ssl=mock.ANY,
            keyfile=mock.ANY,
            certfile=mock.ANY,
            ca=mock.ANY,
            keyfile_password=mock.ANY,
            verify_certs=mock.ANY,
        ),
    ], "All hosts are last tried with the highest timeout value"
