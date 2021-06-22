import asyncio

from kazoo.aio.handler import AioSequentialThreadingHandler
from kazoo.client import KazooClient, TransactionRequest


class AioKazooClient(KazooClient):
    """
    The asyncio compatibility mostly mimics the behaviour of the base async one. All calls are wrapped in
    asyncio.shield() to prevent cancellation that is not supported in the base async implementation.

    The sync and base-async API are still completely functional. Mixing the use of any of the 3 should be okay.
    """

    def __init__(self, *args, **kwargs):
        if not kwargs.get("handler"):
            kwargs["handler"] = AioSequentialThreadingHandler()
        KazooClient.__init__(self, *args, **kwargs)

    # asyncio compatible api wrappers
    async def start_aio(self):
        return await asyncio.shield(self.start_async().future)

    async def add_auth_aio(self, *args, **kwargs):
        return await asyncio.shield(self.add_auth_async(*args, **kwargs).future)

    async def sync_aio(self, *args, **kwargs):
        return await asyncio.shield(self.sync_async(*args, **kwargs).future)

    async def create_aio(self, *args, **kwargs):
        return await asyncio.shield(self.create_async(*args, **kwargs).future)

    async def ensure_path_aio(self, *args, **kwargs):
        return await asyncio.shield(self.ensure_path_async(*args, **kwargs).future)

    async def exists_aio(self, *args, **kwargs):
        return await asyncio.shield(self.exists_async(*args, **kwargs).future)

    async def get_aio(self, *args, **kwargs):
        return await asyncio.shield(self.get_async(*args, **kwargs).future)

    async def get_children_aio(self, *args, **kwargs):
        return await asyncio.shield(self.get_children_async(*args, **kwargs).future)

    async def get_acls_aio(self, *args, **kwargs):
        return await asyncio.shield(self.get_acls_async(*args, **kwargs).future)

    async def set_acls_aio(self, *args, **kwargs):
        return await asyncio.shield(self.set_acls_async(*args, **kwargs).future)

    async def set_aio(self, *args, **kwargs):
        return await asyncio.shield(self.set_async(*args, **kwargs).future)

    def transaction_aio(self):
        return AioTransactionRequest(self)

    async def delete_aio(self, *args, **kwargs):
        return await asyncio.shield(self.delete_async(*args, **kwargs).future)

    async def reconfig_aio(self, *args, **kwargs):
        return await asyncio.shield(self.reconfig_async(*args, **kwargs).future)


class AioTransactionRequest(TransactionRequest):
    async def commit_aio(self):
        return await asyncio.shield(self.commit_async().future)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if not exc_type:
            await self.commit_aio()
