from unittest.mock import Mock

from sqlalchemy import Column, Integer, MetaData, Sequence, Table
from sqlalchemy.schema import CreateSequence, DropSequence

from IfxAlchemy.base import (
    _drop_ifx_identity_sequences,
    _prepare_ifx_identity_sequences,
)
from IfxAlchemy.identity import (
    IDENTITY_SEQUENCE_INFO_KEY,
    iter_ifx_identity_sequences,
    register_ifx_identity_sequence,
)


def _table():
    return Table(
        "identity_owner",
        MetaData(),
        Column("id", Integer, primary_key=True),
    )


def test_identity_lifecycle_apis_are_reexported_from_base():
    assert callable(_prepare_ifx_identity_sequences)
    assert callable(_drop_ifx_identity_sequences)


def test_native_serial_path_requires_no_sequence_preparation():
    table = _table()
    connection = Mock()
    connection.dialect.name = "informix"

    _prepare_ifx_identity_sequences(table, connection)

    connection.execute.assert_not_called()


def test_registered_identity_sequences_are_created_in_insertion_order():
    table = _table()
    first = register_ifx_identity_sequence(table, Sequence("id_seq_1"))
    second = register_ifx_identity_sequence(table, Sequence("id_seq_2"))

    connection = Mock()
    connection.dialect.name = "informix"

    _prepare_ifx_identity_sequences(table, connection)

    calls = connection.execute.call_args_list
    assert len(calls) == 2
    assert isinstance(calls[0].args[0], CreateSequence)
    assert calls[0].args[0].element is first
    assert calls[0].args[0].if_not_exists is True
    assert isinstance(calls[1].args[0], CreateSequence)
    assert calls[1].args[0].element is second


def test_identity_preparation_ignores_non_informix_connections():
    table = _table()
    register_ifx_identity_sequence(table, Sequence("id_seq"))
    connection = Mock()
    connection.dialect.name = "sqlite"

    _prepare_ifx_identity_sequences(table, connection)

    connection.execute.assert_not_called()


def test_native_serial_path_requires_no_sequence_cleanup():
    table = _table()
    connection = Mock()
    connection.dialect.name = "informix"

    _drop_ifx_identity_sequences(table, connection)

    connection.execute.assert_not_called()


def test_registered_identity_sequences_are_deduplicated_and_dropped_reverse_order():
    table = _table()
    first = register_ifx_identity_sequence(table, Sequence("id_seq_1"))
    second = register_ifx_identity_sequence(table, "id_seq_2")
    duplicate = register_ifx_identity_sequence(table, Sequence("id_seq_1"))

    assert duplicate.name == first.name
    assert iter_ifx_identity_sequences(table) == (first, second)

    connection = Mock()
    connection.dialect.name = "informix"

    _drop_ifx_identity_sequences(table, connection)

    calls = connection.execute.call_args_list
    assert len(calls) == 2
    assert isinstance(calls[0].args[0], DropSequence)
    assert calls[0].args[0].element.name == "id_seq_2"
    assert calls[0].args[0].if_exists is True
    assert calls[1].args[0].element.name == "id_seq_1"


def test_identity_cleanup_ignores_non_informix_connections():
    table = _table()
    table.info[IDENTITY_SEQUENCE_INFO_KEY] = [Sequence("id_seq")]
    connection = Mock()
    connection.dialect.name = "sqlite"

    _drop_ifx_identity_sequences(table, connection)

    connection.execute.assert_not_called()


def test_legacy_mapping_registry_entry_is_supported():
    table = _table()
    table.info[IDENTITY_SEQUENCE_INFO_KEY] = [
        {"name": "legacy_identity_seq", "schema": "owner_a"}
    ]

    sequences = iter_ifx_identity_sequences(table)

    assert len(sequences) == 1
    assert sequences[0].name == "legacy_identity_seq"
    assert sequences[0].schema == "owner_a"
