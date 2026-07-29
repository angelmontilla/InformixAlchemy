from __future__ import annotations

"""
Isolate the exact case from the optional probe in prueba4.py to answer clearly:

Does SERIAL actually fail in the dialect, or did only that specific probe fail
because of how it was set up or because of the connection's prior state?

This module does not test autoincrement generically (that is already covered by
`test_autoincrement_serial.py` and related tests). Instead, it tests the exact
`prueba4.py` flow:

1. create an ORM table with `Integer(primary_key=True)` -> compiled as SERIAL
2. call `session.flush()` without assigning a primary key
3. verify that the primary key is populated
4. verify that the row exists and reflection reports autoincrement=True
5. repeat the same flow after triggering the same invalid SAVEPOINT that failed
   in `prueba4.py`

If both tests pass, the conclusion is strong:
- SERIAL does NOT fail generally in the dialect.
- The optional `prueba4.py` failure was not "broken SERIAL"; it was the specific
  probe or the context in which it ran.
"""

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, select, text
from sqlalchemy.orm import Session, registry
from sqlalchemy.schema import CreateTable

from IfxAlchemy.base import _SelectLastRowIDMixin


pytestmark = [pytest.mark.serial_identity]


class _LastrowidContext(_SelectLastRowIDMixin):
    pass


@pytest.fixture
def unique_name(name_factory):
    def _make(prefix: str = "sa_probe4_auto_") -> str:
        return name_factory(prefix)

    return _make


@pytest.fixture
def force_unsupported_savepoint_sql(engine):
    """
    Execute the exact SAVEPOINT SQL that failed in `prueba4.py` and leave the
    connection fully clean (rollback plus physical handle invalidation) to
    determine whether that failure contaminates a later SERIAL/autoincrement
    probe.
    """

    def _run() -> str:
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                conn.exec_driver_sql("SAVEPOINT sa_savepoint_1 ON ROLLBACK RETAIN CURSORS")
            except Exception:
                try:
                    tx.rollback()
                finally:
                    # The legacy savepoint SQL leaves this ODBC session in an
                    # unreliable state for the next DDL statement, so invalidate
                    # the handle to force a fresh connection from the pool.
                    conn.invalidate()
                return "failed_as_expected"
            else:
                tx.rollback()
                return "unexpectedly_supported"

    return _run


def _drop_table_if_exists(engine, table):
    with engine.begin() as conn:
        try:
            if inspect(conn).has_table(table.name):
                table.drop(conn)
        except Exception:
            # Make one final attempt with direct SQL. If that also fails, let
            # the next operation reveal the actual problem.
            try:
                conn.exec_driver_sql(f'DROP TABLE "{table.name}"')
            except Exception:
                pass


def _run_probe4_style_autoincrement_roundtrip(engine, table_name: str) -> dict:
    """
    Reproduce the same pattern as the optional `prueba4.py` probe, but with
    more precise assertions and robust cleanup.

    The table is deliberately similar to ProbeAuto:
      id Integer, primary_key=True   -> the dialect compiles it as SERIAL
      payload String(50)
    """
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(50), nullable=False),
    )

    mapper_registry = registry()

    class ProbeAutoRow:
        pass

    mapper_registry.map_imperatively(ProbeAutoRow, table)

    compiled = str(CreateTable(table).compile(dialect=engine.dialect)).upper()

    _drop_table_if_exists(engine, table)

    try:
        with engine.begin() as conn:
            metadata.create_all(conn)

        generated_pk = None

        with Session(engine, expire_on_commit=False) as session:
            row = ProbeAutoRow()
            row.payload = "auto"
            session.add(row)
            session.flush()
            generated_pk = row.id
            session.commit()

        with engine.connect() as conn:
            cols = {col["name"]: col for col in inspect(conn).get_columns(table_name)}
            fetched = conn.execute(
                select(table.c.id, table.c.payload).where(table.c.id == generated_pk)
            ).one()

        return {
            "compiled": compiled,
            "generated_pk": generated_pk,
            "autoincrement": cols["id"].get("autoincrement"),
            "nullable": cols["id"].get("nullable"),
            "payload": fetched.payload,
        }
    finally:
        try:
            _drop_table_if_exists(engine, table)
        finally:
            mapper_registry.dispose()


def test_probe4_style_autoincrement_works_on_pristine_engine(engine, unique_name):
    """
    Answer the main question without unrelated noise:

    If this test passes, the exact `prueba4.py` pattern does NOT have a general
    SERIAL/autoincrement failure.
    """
    result = _run_probe4_style_autoincrement_roundtrip(engine, unique_name())

    assert "ID SERIAL NOT NULL" in result["compiled"], result["compiled"]
    assert result["generated_pk"] is not None
    assert int(result["generated_pk"]) > 0
    assert result["payload"] == "auto"
    assert result["autoincrement"] is True, result
    assert result["nullable"] is False, result


@pytest.mark.optional_probe_isolation
def test_probe4_style_autoincrement_still_works_after_failed_savepoint(
    engine,
    unique_name,
    force_unsupported_savepoint_sql,
):
    """
    Isolate the hypothesis of contamination from the previous failed SAVEPOINT.

    If this test also passes, the conclusion is very strong:
    - SERIAL does not fail.
    - Even a prior invalid SAVEPOINT does not leave the engine in a state that
      breaks this autoincrement round trip, provided the connection is cleaned.
    """
    savepoint_outcome = force_unsupported_savepoint_sql()
    result = _run_probe4_style_autoincrement_roundtrip(engine, unique_name())

    assert savepoint_outcome in {
        "failed_as_expected",
        "unexpectedly_supported",
    }
    assert "ID SERIAL NOT NULL" in result["compiled"], result["compiled"]
    assert result["generated_pk"] is not None
    assert int(result["generated_pk"]) > 0
    assert result["payload"] == "auto"
    assert result["autoincrement"] is True, result


@pytest.mark.optional_probe_isolation
def test_probe4_style_autoincrement_matches_existing_working_contract(engine, unique_name):
    """
    Compare the `prueba4.py` case with the contract already known to work:
    Integer(primary_key=True) must compile as SERIAL for Informix.

    This prevents confusing a SQL/DDL failure with an ORM flush failure.
    """
    table_name = unique_name()
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(50), nullable=False),
    )

    compiled = str(CreateTable(table).compile(dialect=engine.dialect)).upper()

    assert "CREATE TABLE" in compiled
    assert f"CREATE TABLE {table_name.upper()}" in compiled or f'CREATE TABLE "{table_name.upper()}"' in compiled
    assert "ID SERIAL NOT NULL" in compiled, compiled
    assert "PRIMARY KEY (ID)" in compiled, compiled


def test_probe4_explicit_pk_does_not_schedule_dbinfo_lastrowid_query(unique_name):
    table_name = unique_name()
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(50), nullable=False),
    )

    context = _LastrowidContext()
    context.isinsert = True
    context.compiled = SimpleNamespace(
        dml_compile_state=SimpleNamespace(dml_table=table),
        effective_returning=None,
        inline=False,
    )
    context.compiled_parameters = [{"id": 1001, "payload": "manual"}]
    context.executemany = False

    context.pre_exec()

    assert context._select_lastrowid is False
    assert context._lastrowid_query is None


def test_lastrowid_uses_statement_table_fallback_when_dml_compile_state_missing(
    unique_name,
):
    table_name = unique_name()
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(50), nullable=False),
    )

    insert_stmt = table.insert()

    context = _LastrowidContext()
    context.isinsert = True
    context.compiled = SimpleNamespace(
        statement=insert_stmt,
        effective_returning=None,
        inline=False,
    )
    context.compiled_parameters = [{"payload": "auto"}]
    context.executemany = False

    context.pre_exec()

    assert context._select_lastrowid is True
    assert context._lastrowid_query is not None
    assert "DBINFO" in context._lastrowid_query
