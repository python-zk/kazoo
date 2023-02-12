from __future__ import annotations

import pytest

from kazoo.client import KazooClient, _create_opcode
from kazoo.exceptions import UnimplementedError
from kazoo.protocol.serialization import (
    Create,
    Create2,
    CreateContainer,
    CreateTTL,
)
from kazoo.security import OPEN_ACL_UNSAFE, ACL, Id


class TestCreateOpcode:
    """Unit tests for _create_opcode helper."""

    def test_standard_create(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            OPEN_ACL_UNSAFE,
            None,
            ephemeral=False,
            sequence=False,
            include_data=False,
            container=False,
            ttl=0,
        )
        assert isinstance(op, Create)
        assert op.path == "/test"
        assert op.data == b"data"
        assert op.flags == 0

    def test_ephemeral_create(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            OPEN_ACL_UNSAFE,
            None,
            ephemeral=True,
            sequence=False,
            include_data=False,
            container=False,
            ttl=0,
        )
        assert isinstance(op, Create)
        assert op.flags == 1

    def test_sequence_create(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            OPEN_ACL_UNSAFE,
            None,
            ephemeral=False,
            sequence=True,
            include_data=False,
            container=False,
            ttl=0,
        )
        assert isinstance(op, Create)
        assert op.flags == 2

    def test_include_data_create2(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            OPEN_ACL_UNSAFE,
            None,
            ephemeral=False,
            sequence=False,
            include_data=True,
            container=False,
            ttl=0,
        )
        assert isinstance(op, Create2)
        assert op.flags == 0

    def test_container_create(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            OPEN_ACL_UNSAFE,
            None,
            ephemeral=False,
            sequence=False,
            include_data=False,
            container=True,
            ttl=0,
        )
        assert isinstance(op, CreateContainer)
        assert op.path == "/test"
        assert op.data == b"data"
        assert op.flags == 4

    def test_ttl_create(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            OPEN_ACL_UNSAFE,
            None,
            ephemeral=False,
            sequence=False,
            include_data=False,
            container=False,
            ttl=5000,
        )
        assert isinstance(op, CreateTTL)
        assert op.path == "/test"
        assert op.data == b"data"
        assert op.flags == 5
        assert op.ttl == 5000

    def test_ttl_sequence_create(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            OPEN_ACL_UNSAFE,
            None,
            ephemeral=False,
            sequence=True,
            include_data=False,
            container=False,
            ttl=5000,
        )
        assert isinstance(op, CreateTTL)
        assert op.flags == 6
        assert op.ttl == 5000

    def test_chroot_prefixing(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            OPEN_ACL_UNSAFE,
            "/chroot",
            ephemeral=False,
            sequence=False,
            include_data=False,
            container=True,
            ttl=0,
        )
        assert op.path == "/chroot/test"

    def test_default_acl_applied_when_none(self) -> None:
        op = _create_opcode(
            "/test",
            b"data",
            None,
            None,
            ephemeral=False,
            sequence=False,
            include_data=False,
            container=False,
            ttl=0,
        )
        assert op.acl == OPEN_ACL_UNSAFE

    def test_invalid_path_type(self) -> None:
        with pytest.raises(TypeError, match="Invalid type for 'path'"):
            _create_opcode(
                123,  # type: ignore[arg-type]
                b"data",
                OPEN_ACL_UNSAFE,
                None,
                False,
                False,
                False,
                False,
                0,
            )

    def test_invalid_acl_type(self) -> None:
        with pytest.raises(TypeError, match="Invalid type for 'acl'"):
            _create_opcode(
                "/test",
                b"data",
                OPEN_ACL_UNSAFE[0],  # type: ignore[arg-type]
                None,
                False,
                False,
                False,
                False,
                0,
            )

    def test_invalid_value_type(self) -> None:
        with pytest.raises(TypeError, match="Invalid type for 'value'"):
            _create_opcode(
                "/test",
                "not_bytes",  # type: ignore[arg-type]
                OPEN_ACL_UNSAFE,
                None,
                False,
                False,
                False,
                False,
                0,
            )

    def test_invalid_ephemeral_type(self) -> None:
        with pytest.raises(TypeError, match="Invalid type for 'ephemeral'"):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                "yes",  # type: ignore[arg-type]
                False,
                False,
                False,
                0,
            )

    def test_invalid_sequence_type(self) -> None:
        with pytest.raises(TypeError, match="Invalid type for 'sequence'"):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                False,
                "yes",  # type: ignore[arg-type]
                False,
                False,
                0,
            )

    def test_invalid_include_data_type(self) -> None:
        with pytest.raises(TypeError, match="Invalid type for 'include_data'"):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                False,
                False,
                "yes",  # type: ignore[arg-type]
                False,
                0,
            )

    def test_invalid_container_type(self) -> None:
        with pytest.raises(TypeError, match="Invalid type for 'container'"):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                False,
                False,
                False,
                "yes",  # type: ignore[arg-type]
                0,
            )

    def test_invalid_ttl_type_and_value(self) -> None:
        with pytest.raises(TypeError, match="Invalid 'ttl'"):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                False,
                False,
                False,
                False,
                -1,
            )
        with pytest.raises(TypeError, match="Invalid 'ttl'"):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                False,
                False,
                False,
                False,
                "1000",  # type: ignore[arg-type]
            )

    def test_container_conflicts(self) -> None:
        with pytest.raises(
            TypeError, match="container & ephemeral/sequence/ttl"
        ):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                ephemeral=True,
                sequence=False,
                include_data=False,
                container=True,
                ttl=0,
            )
        with pytest.raises(
            TypeError, match="container & ephemeral/sequence/ttl"
        ):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                ephemeral=False,
                sequence=True,
                include_data=False,
                container=True,
                ttl=0,
            )
        with pytest.raises(
            TypeError, match="container & ephemeral/sequence/ttl"
        ):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                ephemeral=False,
                sequence=False,
                include_data=False,
                container=True,
                ttl=1000,
            )

    def test_ttl_ephemeral_conflict(self) -> None:
        with pytest.raises(TypeError, match="ephemeral & ttl"):
            _create_opcode(
                "/test",
                b"",
                OPEN_ACL_UNSAFE,
                None,
                ephemeral=True,
                sequence=False,
                include_data=False,
                container=False,
                ttl=1000,
            )


class TestClientCreateAsync:
    """Unit tests for client create/create_async validation."""

    def test_makepath_type_check(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        with pytest.raises(TypeError, match="Invalid type for 'makepath'"):
            client.create_async("/test", makepath="yes")  # type: ignore[arg-type]

    def test_default_acl_in_create_async(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")
        custom_acl = [ACL(31, Id("world", "anyone"))]
        client.default_acl = custom_acl

        # Intercept _call to verify opcode acl
        called_op = []

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            called_op.append(op)
            return True

        client._call = mock_call  # type: ignore[method-assign]
        client.create_async("/test")
        assert len(called_op) == 1
        assert called_op[0].acl == custom_acl


class TestClientCreateErrorHandling:
    """Unit tests verifying error handling when server lacks support for
    container or TTL.
    """

    def test_create_container_unimplemented_error(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set_exception(UnimplementedError("unimplemented"))
            return True

        client._call = mock_call  # type: ignore[method-assign]

        with pytest.raises(UnimplementedError):
            client.create("/container_path", container=True)

    def test_create_ttl_unimplemented_error(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set_exception(UnimplementedError("unimplemented"))
            return True

        client._call = mock_call  # type: ignore[method-assign]

        with pytest.raises(UnimplementedError):
            client.create("/ttl_path", ttl=1000)

    def test_transaction_container_unimplemented_error(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set([UnimplementedError("unimplemented")])
            return True

        client._call = mock_call  # type: ignore[method-assign]

        t = client.transaction()
        t.create("/container_path", container=True)
        results = t.commit()
        assert len(results) == 1
        assert isinstance(results[0], UnimplementedError)

    def test_transaction_ttl_unimplemented_error(self) -> None:
        client = KazooClient(hosts="127.0.0.1:2181")

        def mock_call(op, async_result):  # type: ignore[no-untyped-def]
            async_result.set([UnimplementedError("unimplemented")])
            return True

        client._call = mock_call  # type: ignore[method-assign]

        t = client.transaction()
        t.create("/ttl_path", ttl=5000)
        results = t.commit()
        assert len(results) == 1
        assert isinstance(results[0], UnimplementedError)
