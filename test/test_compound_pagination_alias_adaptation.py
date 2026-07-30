from __future__ import annotations

import re

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy import union

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def _table() -> Table:
    metadata = MetaData()
    return Table(
        "some_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("x", Integer),
        Column("y", Integer),
    )


def _ordered_by_compound_result_column(compound):
    return compound.order_by(compound.selected_columns.id)


def _compiled_sql(statement) -> str:
    dialect = IfxDialect_pyodbc(paramstyle="qmark")
    return " ".join(str(statement.compile(dialect=dialect)).split())


def _assert_outer_order_by_uses_outer_compound_alias(statement) -> str:
    sql = _compiled_sql(statement)

    match = re.match(
        r"^SELECT (?:SKIP \S+ )?FIRST \S+ "
        r"(?P<outer>anon_\d+)\.id, (?P=outer)\.x, (?P=outer)\.y "
        r"FROM \(.*\) AS (?P=outer) ORDER BY (?P=outer)\.id$",
        sql,
    )

    assert match is not None, sql
    return sql


def test_distinct_selectable_in_unions_uses_current_outer_alias():
    table = _table()
    first = select(table).where(table.c.id == 2).distinct()
    second = select(table).where(table.c.id == 3).distinct()

    statement = _ordered_by_compound_result_column(
        union(first, second).limit(2)
    )
    sql = _assert_outer_order_by_uses_outer_compound_alias(statement)

    assert sql.count("SELECT DISTINCT") == 2


def test_limit_offset_aliased_selectable_in_unions_uses_current_outer_alias():
    table = _table()
    first = (
        select(table)
        .where(table.c.id == 2)
        .limit(1)
        .order_by(table.c.id)
        .alias()
        .select()
    )
    second = (
        select(table)
        .where(table.c.id == 3)
        .limit(1)
        .order_by(table.c.id)
        .alias()
        .select()
    )

    statement = _ordered_by_compound_result_column(
        union(first, second).limit(2)
    )
    sql = _assert_outer_order_by_uses_outer_compound_alias(statement)

    assert sql.count("FIRST") == 3


def test_limit_offset_selectable_in_unions_uses_current_outer_alias():
    table = _table()
    first = (
        select(table)
        .where(table.c.id == 2)
        .limit(1)
        .order_by(table.c.id)
    )
    second = (
        select(table)
        .where(table.c.id == 3)
        .limit(1)
        .order_by(table.c.id)
    )

    statement = _ordered_by_compound_result_column(
        union(first, second).limit(2)
    )
    sql = _assert_outer_order_by_uses_outer_compound_alias(statement)

    assert sql.count("FIRST") == 3


def test_order_by_selectable_in_unions_uses_current_outer_alias():
    table = _table()
    first = select(table).where(table.c.id == 2).order_by(table.c.id)
    second = select(table).where(table.c.id == 3).order_by(table.c.id)

    statement = _ordered_by_compound_result_column(
        union(first, second).limit(2)
    )
    sql = _assert_outer_order_by_uses_outer_compound_alias(statement)

    assert sql.count("FIRST") == 1


def test_skip_first_compound_uses_current_outer_alias():
    table = _table()
    first = select(table).where(table.c.id <= 2)
    second = select(table).where(table.c.id >= 3)

    statement = _ordered_by_compound_result_column(
        union(first, second).limit(2).offset(1)
    )
    sql = _assert_outer_order_by_uses_outer_compound_alias(statement)

    assert sql.startswith("SELECT SKIP "), sql
    assert " FIRST " in sql
    assert "ROW_NUMBER" not in sql
