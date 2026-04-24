from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    Identity,
    Integer,
    MetaData,
    String,
    Table,
    inspect,
    insert,
    select,
)
from sqlalchemy.orm import Session, registry
from sqlalchemy.schema import CreateTable

from IfxAlchemy.base import BIGSERIAL, SERIAL, SERIAL8, _SelectLastRowIDMixin
from IfxAlchemy.pyodbc import IfxDialect_pyodbc

@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _assert_generated_lastrowid_roundtrip(engine, name_factory, prefix, type_):
    table_name = name_factory(prefix)
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", type_, primary_key=True),
        Column("name", String(30)),
    )

    with engine.begin() as conn:
        table.create(conn)

        try:
            result = conn.execute(insert(table).values(name="alpha"))

            assert result.lastrowid is not None
            assert isinstance(result.lastrowid, int)
            assert result.lastrowid > 0

            pk = result.inserted_primary_key
            assert pk is not None
            assert len(pk) == 1
            assert pk[0] == result.lastrowid

            row = conn.execute(
                select(table.c.id, table.c.name).where(
                    table.c.id == result.lastrowid
                )
            ).one()

            assert row.id == result.lastrowid
            assert row.name == "alpha"
        finally:
            table.drop(conn)


class _RecordingCursor:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def execute(self, statement):
        self.executed.append(statement)

    def fetchone(self):
        return self.row


class _LastrowidContext(_SelectLastRowIDMixin):
    pass


@pytest.mark.serial_identity
def test_lastrowid_post_exec_uses_direct_cursor_execute():
    cursor = _RecordingCursor((42,))
    context = _LastrowidContext()
    context.cursor = cursor
    context.root_connection = SimpleNamespace(
        _cursor_execute=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("_cursor_execute should not be used")
        )
    )
    context._select_lastrowid = True
    context._lastrowid_query = "SELECT DBINFO('sqlca.sqlerrd1') FROM systables WHERE tabid = 1"

    context.post_exec()

    assert cursor.executed == [context._lastrowid_query]
    assert context.get_lastrowid() == 42


@pytest.mark.serial_identity
def test_lastrowid_post_exec_handles_missing_row():
    cursor = _RecordingCursor(None)
    context = _LastrowidContext()
    context.cursor = cursor
    context._select_lastrowid = True
    context._lastrowid_query = "SELECT DBINFO('sqlca.sqlerrd1') FROM systables WHERE tabid = 1"

    context.post_exec()

    assert context.get_lastrowid() is None


@pytest.mark.serial_identity
def test_compile_serial_types(dialect):
    assert dialect.type_compiler.process(SERIAL()).upper() == "SERIAL"
    assert dialect.type_compiler.process(SERIAL8()).upper() == "SERIAL8"
    assert dialect.type_compiler.process(BIGSERIAL()).upper() == "BIGSERIAL"


@pytest.mark.serial_identity
def test_integer_identity_does_not_enable_sqlalchemy_identity_flag(dialect):
    assert dialect.supports_identity_columns is False


@pytest.mark.serial_identity
def test_reflect_serial_autoincrement_true(db_builder, engine, name_factory):
    table_name = name_factory("sa_ser_")

    db_builder(
        f"""
        CREATE TABLE {table_name} (
            id SERIAL NOT NULL PRIMARY KEY,
            name VARCHAR(30)
        )
        """,
        f"DROP TABLE {table_name}",
    )

    with engine.connect() as connection:
        insp = inspect(connection)
        cols = {col["name"]: col for col in insp.get_columns(table_name)}

    assert cols["id"]["autoincrement"] is True, cols["id"]
    assert cols["id"]["nullable"] is False


@pytest.mark.serial_identity
def test_reflect_serial8_autoincrement_true(db_builder, engine, name_factory):
    table_name = name_factory("sa_ser8_")

    db_builder(
        f"""
        CREATE TABLE {table_name} (
            id SERIAL8 NOT NULL PRIMARY KEY,
            name VARCHAR(30)
        )
        """,
        f"DROP TABLE {table_name}",
    )

    with engine.connect() as connection:
        insp = inspect(connection)
        cols = {col["name"]: col for col in insp.get_columns(table_name)}

    assert cols["id"]["autoincrement"] is True, cols["id"]
    assert cols["id"]["nullable"] is False


@pytest.mark.serial_identity
def test_bigserial_round_trip_returns_generated_pk_and_reflects_autoincrement(
    engine, name_factory
):
    table_name = name_factory("sa_bigser_")
    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        Column("id", BIGSERIAL(), primary_key=True),
        Column("name", String(30)),
    )

    with engine.begin() as conn:
        metadata.create_all(conn)

        result = conn.execute(insert(table).values(name="alpha"))

        pk = result.inserted_primary_key
        assert pk is not None
        assert len(pk) == 1
        assert pk[0] is not None
        assert int(pk[0]) > 0

        row = conn.execute(
            select(table.c.id, table.c.name).where(table.c.id == pk[0])
        ).one()

        assert row.id == pk[0]
        assert row.name == "alpha"

        cols = {col["name"]: col for col in inspect(conn).get_columns(table_name)}
        assert cols["id"]["autoincrement"] is True, cols["id"]

        metadata.drop_all(conn)


@pytest.mark.serial_identity
def test_insert_without_pk_returns_generated_serial_pk(engine, name_factory):
    table_name = name_factory("sa_ins_ser_")
    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        Column("id", SERIAL(), primary_key=True),
        Column("name", String(30)),
    )

    with engine.begin() as conn:
        metadata.create_all(conn)

        result = conn.execute(insert(table).values(name="alpha"))

        pk = result.inserted_primary_key
        assert pk is not None
        assert len(pk) == 1
        assert pk[0] is not None
        assert int(pk[0]) > 0

        row = conn.execute(
            select(table.c.id, table.c.name).where(table.c.id == pk[0])
        ).one()

        assert row.id == pk[0]
        assert row.name == "alpha"

        metadata.drop_all(conn)


@pytest.mark.serial_identity
def test_lastrowid_round_trip_for_serial(engine, name_factory):
    _assert_generated_lastrowid_roundtrip(
        engine, name_factory, "sa_lr_ser_", SERIAL()
    )


@pytest.mark.serial_identity
def test_lastrowid_round_trip_for_serial8(engine, name_factory):
    _assert_generated_lastrowid_roundtrip(
        engine, name_factory, "sa_lr_ser8_", SERIAL8()
    )


@pytest.mark.serial_identity
def test_lastrowid_round_trip_for_bigserial(engine, name_factory):
    _assert_generated_lastrowid_roundtrip(
        engine, name_factory, "sa_lr_big_", BIGSERIAL()
    )


@pytest.mark.serial_identity
def test_insert_with_explicit_pk_preserves_value(engine, name_factory):
    table_name = name_factory("sa_ins_exp_")
    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        Column("id", SERIAL(), primary_key=True),
        Column("name", String(30)),
    )

    with engine.begin() as conn:
        metadata.create_all(conn)

        result = conn.execute(insert(table).values(id=12345, name="manual"))
        pk = result.inserted_primary_key

        row = conn.execute(
            select(table.c.id, table.c.name).where(table.c.id == 12345)
        ).one()

        assert row.id == 12345
        assert row.name == "manual"

        if pk and pk[0] is not None:
            assert int(pk[0]) == 12345

        metadata.drop_all(conn)


@pytest.mark.serial_identity
def test_orm_flush_populates_serial_pk(engine, name_factory):
    table_name = name_factory("sa_orm_ser_")
    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        Column("id", SERIAL(), primary_key=True),
        Column("name", String(30)),
    )

    class Row:
        pass

    mapper_registry = registry()
    mapper_registry.map_imperatively(Row, table)

    with engine.begin() as conn:
        metadata.create_all(conn)

    try:
        with Session(engine, expire_on_commit=False) as session:
            obj = Row()
            obj.name = "beta"

            session.add(obj)
            session.flush()

            assert obj.id is not None
            assert int(obj.id) > 0

            session.commit()
    finally:
        with engine.begin() as conn:
            metadata.drop_all(conn)
        mapper_registry.dispose()


@pytest.mark.serial_identity
def test_integer_pk_round_trip_uses_generated_serial_pk(engine, name_factory):
    table_name = name_factory("sa_intpk_")
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(30)),
    )

    with engine.begin() as conn:
        metadata.create_all(conn)

        result = conn.execute(insert(table).values(name="gamma"))
        pk = result.inserted_primary_key

        assert pk is not None
        assert len(pk) == 1
        assert pk[0] is not None
        assert int(pk[0]) > 0

        row = conn.execute(
            select(table.c.id, table.c.name).where(table.c.id == pk[0])
        ).one()
        cols = {col["name"]: col for col in inspect(conn).get_columns(table_name)}

        assert row.id == pk[0]
        assert row.name == "gamma"
        assert cols["id"]["autoincrement"] is True, cols["id"]

        metadata.drop_all(conn)


@pytest.mark.serial_identity
def test_integer_pk_contract_uses_serial(dialect, name_factory):
    metadata = MetaData()
    table = Table(
        name_factory("sa_intpk_"),
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(30)),
    )

    compiled = str(CreateTable(table).compile(dialect=dialect)).upper()

    assert "ID SERIAL NOT NULL" in compiled
    assert "IDENTITY" not in compiled


@pytest.mark.serial_identity
def test_identity_contract_is_normalized_to_serial(dialect, name_factory):
    metadata = MetaData()
    table = Table(
        name_factory("sa_auto_"),
        metadata,
        Column("id", Integer, Identity(start=1), primary_key=True),
        Column("name", String(30)),
    )

    compiled = str(CreateTable(table).compile(dialect=dialect)).upper()

    assert "ID SERIAL NOT NULL" in compiled
    assert "IDENTITY" not in compiled
