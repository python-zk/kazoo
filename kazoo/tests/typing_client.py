"""Static checks for the public type annotations."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import assert_type

    from kazoo.client import KazooClient
    from kazoo.interfaces import IAsyncResult
    from kazoo.protocol.serialization import Transaction_Response
    from kazoo.protocol.states import ZnodeStat
    from kazoo.security import ACL

    client = KazooClient()

    assert_type(
        client.add_auth_async("digest", "user:pass"), IAsyncResult[bool]
    )
    assert_type(client.sync_async("/node"), IAsyncResult[str])
    assert_type(
        client.create_async("/node"),
        IAsyncResult[str | tuple[str, ZnodeStat]],
    )
    assert_type(client.ensure_path_async("/node"), IAsyncResult[bool])
    assert_type(client.exists_async("/node"), IAsyncResult[ZnodeStat | None])
    assert_type(
        client.get_async("/node"),
        IAsyncResult[tuple[bytes, ZnodeStat]],
    )
    assert_type(
        client.get_children_async("/node"),
        IAsyncResult[list[str] | tuple[list[str], ZnodeStat]],
    )
    assert_type(
        client.get_acls_async("/node"),
        IAsyncResult[tuple[list[ACL], ZnodeStat]],
    )
    assert_type(client.set_acls_async("/node", []), IAsyncResult[ZnodeStat])
    assert_type(client.set_async("/node", b"data"), IAsyncResult[ZnodeStat])
    assert_type(client.delete_async("/node"), IAsyncResult[bool])
    assert_type(
        client.reconfig_async(None, None, None, -1),
        IAsyncResult[tuple[bytes, ZnodeStat]],
    )
    assert_type(
        client.transaction().commit_async(),
        IAsyncResult[list[Transaction_Response]],
    )
