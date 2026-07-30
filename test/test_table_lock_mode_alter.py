from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, inspect
from sqlalchemy.exc import ArgumentError, CompileError
from sqlalchemy.sql import quoted_name

from IfxAlchemy import SetTableLockMode
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _table(
    name="customers",
    *,
    schema=None,
    lock_level=None,
):
    options = {}
    if lock_level is not None:
        options["informix_lock_level"] = lock_level

    return Table(
        name,
        MetaData(),
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        schema=schema,
        **options,
    )


def _compile(statement, dialect):
    return " ".join(str(statement.compile(dialect=dialect)).split())


@pytest.mark.ddl_compiler
def test_set_table_lock_mode_compiles_row(dialect):
    statement = SetTableLockMode(_table(), " row ")

    assert _compile(statement, dialect) == (
        "ALTER TABLE customers LOCK MODE (ROW)"
    )


@pytest.mark.ddl_compiler
def test_set_table_lock_mode_compiles_page(dialect):
    statement = SetTableLockMode(_table(), "PAGE")

    assert _compile(statement, dialect) == (
        "ALTER TABLE customers LOCK MODE (PAGE)"
    )


@pytest.mark.ddl_compiler
def test_set_table_lock_mode_quotes_owner_and_table_name(dialect):
    table = _table(
        quoted_name("Order Owner", True),
        schema=quoted_name("Data Owner", True),
    )

    assert _compile(SetTableLockMode(table, "ROW"), dialect) == (
        'ALTER TABLE "Data Owner"."Order Owner" LOCK MODE (ROW)'
    )


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    "lock_mode",
    ["TABLE", "PAGE_AND_ROW", "", "   ", 1, None, object()],
)
def test_set_table_lock_mode_rejects_non_writable_values(
    dialect,
    lock_mode,
):
    with pytest.raises(
        CompileError,
        match="lock_mode must be either 'PAGE' or 'ROW'",
    ):
        _compile(SetTableLockMode(_table(), lock_mode), dialect)


@pytest.mark.ddl_compiler
def test_set_table_lock_mode_requires_table_object():
    with pytest.raises(
        ArgumentError,
        match="table must be a sqlalchemy.schema.Table",
    ):
        SetTableLockMode("customers", "ROW")


@pytest.mark.ddl_execute
def test_table_lock_mode_create_reflect_and_alter_round_trip(
    engine,
    name_factory,
):
    table_name = name_factory("sa_lock_")
    table = _table(table_name, lock_level="PAGE")

    try:
        with engine.begin() as connection:
            table.create(connection)

            reflected = inspect(connection).get_table_options(table_name)
            assert reflected["informix_lock_level"] == "PAGE"

            connection.execute(SetTableLockMode(table, "ROW"))
            reflected = inspect(connection).get_table_options(table_name)
            assert reflected["informix_lock_level"] == "ROW"

            connection.execute(SetTableLockMode(table, "PAGE"))
            reflected = inspect(connection).get_table_options(table_name)
            assert reflected["informix_lock_level"] == "PAGE"
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)
