from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, inspect
from sqlalchemy.exc import ArgumentError, CompileError
from sqlalchemy.sql import quoted_name

from IfxAlchemy import ModifyTableExtents
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _table(name="movements", *, schema=None, **table_options):
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
        **table_options,
    )


def _compile(statement, dialect):
    return " ".join(str(statement.compile(dialect=dialect)).split())


@pytest.mark.ddl_compiler
def test_modify_table_extents_compiles_first_and_next(dialect):
    statement = ModifyTableExtents(
        _table(),
        first_extent=128,
        next_extent=64,
    )

    assert _compile(statement, dialect) == (
        "ALTER TABLE movements MODIFY EXTENT SIZE 128 NEXT SIZE 64"
    )


@pytest.mark.ddl_compiler
def test_modify_table_extents_compiles_first_only(dialect):
    statement = ModifyTableExtents(
        _table(),
        first_extent=128,
    )

    assert _compile(statement, dialect) == (
        "ALTER TABLE movements MODIFY EXTENT SIZE 128"
    )


@pytest.mark.ddl_compiler
def test_modify_table_extents_compiles_next_only(dialect):
    statement = ModifyTableExtents(
        _table(),
        next_extent=64,
    )

    assert _compile(statement, dialect) == (
        "ALTER TABLE movements MODIFY NEXT SIZE 64"
    )


@pytest.mark.ddl_compiler
def test_modify_table_extents_quotes_owner_and_table_name(dialect):
    table = _table(
        quoted_name("Movement Ledger", True),
        schema=quoted_name("Data Owner", True),
    )

    assert _compile(
        ModifyTableExtents(
            table,
            first_extent=128,
            next_extent=64,
        ),
        dialect,
    ) == (
        'ALTER TABLE "Data Owner"."Movement Ledger" '
        "MODIFY EXTENT SIZE 128 NEXT SIZE 64"
    )


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("parameter_name", "invalid_value", "message"),
    [
        ("first_extent", 0, "first_extent must be greater than zero"),
        ("first_extent", -1, "first_extent must be greater than zero"),
        (
            "first_extent",
            True,
            "first_extent must be a positive integer",
        ),
        (
            "next_extent",
            False,
            "next_extent must be a positive integer",
        ),
        (
            "next_extent",
            64.0,
            "next_extent must be a positive integer",
        ),
        (
            "next_extent",
            "64",
            "next_extent must be a positive integer",
        ),
        (
            "next_extent",
            "64; DROP TABLE movements",
            "next_extent must be a positive integer",
        ),
        (
            "next_extent",
            object(),
            "next_extent must be a positive integer",
        ),
    ],
)
def test_modify_table_extents_rejects_invalid_values(
    dialect,
    parameter_name,
    invalid_value,
    message,
):
    with pytest.raises(CompileError, match=message):
        _compile(
            ModifyTableExtents(
                _table(),
                **{parameter_name: invalid_value},
            ),
            dialect,
        )


@pytest.mark.ddl_compiler
def test_modify_table_extents_requires_at_least_one_size():
    with pytest.raises(
        ArgumentError,
        match="requires first_extent and/or next_extent",
    ):
        ModifyTableExtents(_table())


@pytest.mark.ddl_compiler
def test_modify_table_extents_requires_table_object():
    with pytest.raises(
        ArgumentError,
        match="table must be a sqlalchemy.schema.Table",
    ):
        ModifyTableExtents("movements", first_extent=128)


@pytest.mark.ddl_execute
def test_table_extents_create_reflect_and_modify_round_trip(
    engine,
    name_factory,
):
    table_name = name_factory("sa_extent_")
    table = _table(
        table_name,
        informix_first_extent=96,
        informix_next_extent=96,
    )

    try:
        with engine.begin() as connection:
            table.create(connection)

            reflected = inspect(connection).get_table_options(table_name)
            assert reflected["informix_first_extent"] == 96
            assert reflected["informix_next_extent"] == 96

            connection.execute(
                ModifyTableExtents(
                    table,
                    first_extent=128,
                    next_extent=64,
                )
            )
            reflected = inspect(connection).get_table_options(table_name)
            assert reflected["informix_first_extent"] == 128
            assert reflected["informix_next_extent"] == 64

            connection.execute(
                ModifyTableExtents(table, first_extent=192)
            )
            reflected = inspect(connection).get_table_options(table_name)
            assert reflected["informix_first_extent"] == 192
            assert reflected["informix_next_extent"] == 64

            connection.execute(
                ModifyTableExtents(table, next_extent=128)
            )
            reflected = inspect(connection).get_table_options(table_name)
            assert reflected["informix_first_extent"] == 192
            assert reflected["informix_next_extent"] == 128
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)
