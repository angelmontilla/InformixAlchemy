from sqlalchemy import Column, Identity, Integer, MetaData, Table

from IfxAlchemy.base import (
    _drop_ifx_identity_sequences,
    _prepare_ifx_identity_sequences,
)
from IfxAlchemy.identity import (
    IDENTITY_SEQUENCE_COLUMN_INFO_KEY,
    identity_requires_sequence,
    identity_sequence_for_column,
    identity_sequence_name,
    identity_uses_native_serial,
    iter_ifx_identity_sequences,
    register_ifx_identity_sequences_for_table,
)


def _table(identity: Identity | None = None, *, schema: str | None = None):
    args = ["id", Integer]
    if identity is not None:
        args.append(identity)
    return Table(
        "orders",
        MetaData(),
        Column(*args, primary_key=True),
        schema=schema,
    )


def test_complete_identity_api_imports_together():
    assert callable(_prepare_ifx_identity_sequences)
    assert callable(_drop_ifx_identity_sequences)
    assert callable(identity_sequence_for_column)
    assert callable(identity_sequence_name)
    assert callable(identity_requires_sequence)
    assert callable(identity_uses_native_serial)
    assert callable(register_ifx_identity_sequences_for_table)


def test_non_identity_column_has_no_identity_sequence():
    column = _table().c.id

    assert identity_uses_native_serial(column) is False
    assert identity_requires_sequence(column) is False
    assert identity_sequence_for_column(column) is None


def test_plain_identity_uses_native_serial_without_registry_side_effects():
    table = _table(Identity())
    column = table.c.id

    assert identity_uses_native_serial(column) is True
    assert identity_requires_sequence(column) is False
    assert identity_sequence_for_column(column) is None
    assert iter_ifx_identity_sequences(table) == ()


def test_start_one_increment_one_remains_native_serial():
    column = _table(Identity(start=1, increment=1)).c.id

    assert identity_uses_native_serial(column) is True
    assert identity_sequence_for_column(column) is None


def test_extended_identity_creates_one_stable_registered_sequence():
    table = _table(
        Identity(
            start=10,
            increment=5,
            minvalue=5,
            maxvalue=1000,
            cache=20,
            cycle=True,
        ),
        schema="reporting",
    )
    column = table.c.id

    first = identity_sequence_for_column(column)
    second = identity_sequence_for_column(column)

    assert first is second
    assert first is not None
    assert first.name == "orders_id_identity_seq"
    assert first.schema == "reporting"
    assert first.start == 10
    assert first.increment == 5
    assert first.minvalue == 5
    assert first.maxvalue == 1000
    assert first.cache == 20
    assert first.cycle is True
    assert first.data_type is column.type
    assert column.info[IDENTITY_SEQUENCE_COLUMN_INFO_KEY] is first
    assert iter_ifx_identity_sequences(table) == (first,)


def test_table_registration_materializes_all_extended_identity_columns():
    metadata = MetaData()
    table = Table(
        "identity_owner",
        metadata,
        Column("id", Integer, Identity(start=2), primary_key=True),
        Column("native_id", Integer, Identity()),
    )

    sequences = register_ifx_identity_sequences_for_table(table)

    assert len(sequences) == 1
    assert sequences[0] is identity_sequence_for_column(table.c.id)
    assert identity_sequence_for_column(table.c.native_id) is None


def test_special_and_long_identifiers_produce_stable_bounded_sequence_names():
    table = Table(
        "tabla con espacios " + "x" * 160,
        MetaData(),
        Column(
            "columna%especial " + "y" * 160,
            Integer,
            Identity(start=2),
            primary_key=True,
        ),
    )

    first = identity_sequence_name(table.c[0])
    second = identity_sequence_name(table.c[0])

    assert first == second
    assert len(first) <= 128
    assert " " not in first
    assert "%" not in first
    assert first.endswith("_identity_seq")
