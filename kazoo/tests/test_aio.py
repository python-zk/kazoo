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
        try:
            await self.client.delete_aio("/tmp/x")
        except NotEmptyError:
            pass
        await self.client.delete_aio("/tmp/x/y")
        try:
            await self.client.get_aio("/tmp/x/y")
        except NoNodeError:
            pass
        async with self.client.transaction_aio() as tx:
            tx.create("/tmp/z", b"ZZZ")
            tx.set_data("/tmp/x", b"XXX")
        assert (await self.client.get_aio("/tmp/x"))[0] == b"XXX"
        assert (await self.client.get_aio("/tmp/z"))[0] == b"ZZZ"
