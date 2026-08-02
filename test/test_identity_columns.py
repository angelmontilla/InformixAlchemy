from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import BigInteger, Column, Identity, Integer, MetaData, Table
from sqlalchemy.schema import CreateSequence, CreateTable, DropSequence

from IfxAlchemy.base import (
    _drop_ifx_identity_sequences,
    _prepare_ifx_identity_sequences,
)
from IfxAlchemy.identity import (
    identity_sequence_for_column,
    identity_sequence_name,
)
from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.reflection import IfxReflector


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _table(identity, type_=Integer, *, schema=None, table_name="orders"):
    metadata = MetaData()
    return Table(
        table_name,
        metadata,
        Column("id", type_, identity, primary_key=True),
        schema=schema,
    )


def test_identity_capability_flags_are_explicit(dialect):
    assert dialect.supports_identity_columns is True
    assert dialect.preexecute_sequences is True


def test_identity_compiles_as_integer_not_serial(dialect):
    table = _table(Identity())

    sql = str(CreateTable(table).compile(dialect=dialect)).upper()

    assert "ID INTEGER NOT NULL" in sql
    assert "SERIAL" not in sql
    assert table.c.id.default is not None
    assert table.c.id.default.is_sequence


def test_plain_integer_autoincrement_still_compiles_as_serial(dialect):
    metadata = MetaData()
    table = Table(
        "orders",
        metadata,
        Column("id", Integer, primary_key=True),
    )

    sql = str(CreateTable(table).compile(dialect=dialect)).upper()

    assert "ID SERIAL NOT NULL" in sql


def test_identity_sequence_preserves_start_increment_and_bounds(dialect):
    table = _table(
        Identity(
            start=0,
            increment=-5,
            minvalue=-1000,
            maxvalue=0,
            cycle=True,
            cache=4,
        )
    )
    CreateTable(table).compile(dialect=dialect)
    sequence = identity_sequence_for_column(table.c.id)

    sql = str(CreateSequence(sequence).compile(dialect=dialect)).upper()

    assert "START WITH 0" in sql
    assert "INCREMENT BY -5" in sql
    assert "MINVALUE -1000" in sql
    assert "MAXVALUE 1" in sql
    assert "CACHE 4" in sql
    assert "CYCLE" in sql


def test_big_integer_identity_uses_big_integer_sequence_type(dialect):
    table = _table(Identity(start=7), type_=BigInteger)
    CreateTable(table).compile(dialect=dialect)
    sequence = identity_sequence_for_column(table.c.id)

    assert isinstance(sequence.data_type, BigInteger)


def test_identity_sequence_name_is_stable_for_reflection():
    unquoted = identity_sequence_name("Mixed Table", "Id Column", "Owner")
    reflected = identity_sequence_name("mixed table", "id column", "owner")

    assert unquoted == reflected
    assert unquoted.startswith("ifx_id_")
    assert len(unquoted) < 128


def test_identity_insert_is_preexecuted(dialect):
    table = _table(Identity(start=42))
    CreateTable(table).compile(dialect=dialect)

    compiled = table.insert().values().compile(dialect=dialect)

    assert list(compiled.prefetch) == [table.c.id]
    assert compiled.params["id"] is None


def test_explicit_identity_value_is_not_prefetched(dialect):
    table = _table(Identity(start=42))
    CreateTable(table).compile(dialect=dialect)

    compiled = table.insert().values(id=99).compile(dialect=dialect)

    assert list(compiled.prefetch) == []
    assert compiled.params["id"] == 99


class _RecordingConnection:
    def __init__(self):
        self.dialect = SimpleNamespace(name="informix")
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)


def test_table_events_create_and_drop_private_identity_sequence():
    table = _table(Identity(start=3, increment=2))
    connection = _RecordingConnection()

    _prepare_ifx_identity_sequences(table, connection)
    _drop_ifx_identity_sequences(table, connection)

    assert isinstance(connection.statements[0], DropSequence)
    assert isinstance(connection.statements[1], CreateSequence)
    assert isinstance(connection.statements[2], DropSequence)
    assert connection.statements[0].if_exists is True
    assert connection.statements[2].if_exists is True


class _ScalarResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _ReflectionConnection:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def exec_driver_sql(self, statement, parameters):
        self.calls.append((statement, parameters))
        return _ScalarResult(self.row)


def test_reflection_identity_metadata_comes_from_private_sequence(dialect):
    reflector = IfxReflector(dialect)
    connection = _ReflectionConnection(
        (2, 3, -2, 42, "1", 4)
    )

    identity = reflector._identity_sequence_metadata(
        connection,
        "orders",
        "id",
        schema="test_schema",
    )

    assert identity == {
        "always": False,
        "start": 2,
        "increment": 3,
        "minvalue": -2,
        "maxvalue": 42,
        "cycle": True,
        "cache": 4,
        "order": False,
    }
    sequence_name, owner = connection.calls[0][1]
    assert sequence_name == identity_sequence_name(
        "orders",
        "id",
        "test_schema",
    )
    assert str(owner).casefold() == "test_schema"


def test_reflection_without_private_sequence_is_not_identity(dialect):
    reflector = IfxReflector(dialect)
    connection = _ReflectionConnection(None)

    assert (
        reflector._identity_sequence_metadata(
            connection,
            "orders",
            "id",
            schema="informix",
        )
        is None
    )
