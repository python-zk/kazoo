import pytest

from kazoo.aio.retry import AioKazooRetry
from kazoo.exceptions import NotEmptyError, NoNodeError
from kazoo.protocol.states import ZnodeStat
from kazoo.testing import KazooAioTestCase


class KazooAioTests(KazooAioTestCase):
    def test_basic_aio_functionality(self):
        self.loop.run_until_complete(self._test_basic_aio_functionality())

    async def _test_basic_aio_functionality(self):
        assert await self.client.create_aio("/tmp") == "/tmp"
        assert await self.client.get_children_aio("/") == ["tmp"]
        assert await self.client.ensure_path_aio("/tmp/x/y") == "/tmp/x/y"
        assert await self.client.exists_aio("/tmp/x/y")
        assert isinstance(
            await self.client.set_aio("/tmp/x/y", b"very aio"), ZnodeStat
        )
        data, stat = await self.client.get_aio("/tmp/x/y")
        assert data == b"very aio"
        assert isinstance(stat, ZnodeStat)
        with pytest.raises(NotEmptyError):
            await self.client.delete_aio("/tmp/x")
        await self.client.delete_aio("/tmp/x/y")
        with pytest.raises(NoNodeError):
            await self.client.get_aio("/tmp/x/y")
        async with self.client.transaction_aio() as tx:
            tx.create("/tmp/z", b"ZZZ")
            tx.set_data("/tmp/x", b"XXX")
        assert (await self.client.get_aio("/tmp/x"))[0] == b"XXX"
        assert (await self.client.get_aio("/tmp/z"))[0] == b"ZZZ"
        self.client.stop()
        assert self.client.connected is False
        await self.client.start_aio()
        assert self.client.connected is True
        assert set(await self.client.get_children_aio("/tmp")) == set(
            ["x", "z"]
        )

    def test_aio_retry_functionality(self):
        self.loop.run_until_complete(self._test_aio_retry_functionality())

    async def _test_aio_retry_functionality(self):
        # Just lump them all in here for now, they are short enough that
        # it does not matter much.
        await self._test_aio_retry()
        await self._test_too_many_tries()
        await self._test_connection_closed()
        await self._test_session_expired()

    async def _pass(self):
        pass

    def _fail(self, times=1):
        from kazoo.retry import ForceRetryError

        scope = dict(times=0)

        async def inner():
            if scope["times"] >= times:
                pass
            else:
                scope["times"] += 1
                raise ForceRetryError("Failed!")

        return inner

    async def _test_aio_retry(self):
        aio_retry = AioKazooRetry(delay=0, max_tries=2)
        await aio_retry(self._fail())
        assert aio_retry._attempts == 1
        aio_retry.reset()
        assert aio_retry._attempts == 0

    async def _test_too_many_tries(self):
        from kazoo.retry import RetryFailedError

        aio_retry = AioKazooRetry(delay=0)
        with pytest.raises(RetryFailedError):
            await aio_retry(self._fail(times=999))
        assert aio_retry._attempts == 1

    async def _test_connection_closed(self):
        from kazoo.exceptions import ConnectionClosedError

        aio_retry = AioKazooRetry()

        async def testit():
            raise ConnectionClosedError()

        with pytest.raises(ConnectionClosedError):
            await aio_retry(testit)

    async def _test_session_expired(self):
        from kazoo.exceptions import SessionExpiredError

        aio_retry = AioKazooRetry()

        async def testit():
            raise SessionExpiredError()

        with pytest.raises(Exception):
            await aio_retry(testit)
