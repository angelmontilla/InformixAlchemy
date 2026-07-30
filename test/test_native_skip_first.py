from __future__ import annotations

from sqlalchemy import Integer
from sqlalchemy import bindparam
from sqlalchemy import column
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy import union

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def _normalized(statement, *, paramstyle="named") -> str:
    dialect = IfxDialect_pyodbc(paramstyle=paramstyle)
    return " ".join(str(statement.compile(dialect=dialect)).split())


def test_native_skip_first_is_cache_safe_for_python_integer_values():
    source = table("source", column("id"))
    first = select(source.c.id).order_by(source.c.id).limit(5).offset(2)
    second = select(source.c.id).order_by(source.c.id).limit(10).offset(4)
    dialect = IfxDialect_pyodbc()

    first_compiled = first.compile(dialect=dialect)
    second_compiled = second.compile(dialect=dialect)

    assert first._generate_cache_key().key == second._generate_cache_key().key
    assert str(first_compiled) == str(second_compiled)
    assert str(first_compiled).startswith(
        "SELECT SKIP __[POSTCOMPILE_"
    )
    assert " FIRST __[POSTCOMPILE_" in str(first_compiled)
    assert first_compiled.params != second_compiled.params


def test_native_skip_first_preserves_qmark_host_parameter_order_and_types():
    source = table("source", column("id"))
    statement = (
        select(source.c.id)
        .order_by(source.c.id)
        .limit(bindparam("limit_count"))
        .offset(bindparam("offset_count"))
    )
    dialect = IfxDialect_pyodbc(paramstyle="qmark")
    compiled = statement.compile(dialect=dialect)

    assert " ".join(str(compiled).split()).startswith(
        "SELECT SKIP ? FIRST ?"
    )
    assert compiled.positiontup == ["offset_count", "limit_count"]
    assert isinstance(compiled.binds["offset_count"].type, Integer)
    assert isinstance(compiled.binds["limit_count"].type, Integer)


def test_arbitrary_limit_expression_retains_row_number_fallback():
    source = table("source", column("id"))
    expression = literal_column("1") + literal_column("1")
    sql = _normalized(
        select(source.c.id)
        .order_by(source.c.id)
        .limit(expression)
        .offset(2)
    )

    assert 'ROW_NUMBER() OVER (ORDER BY "source".id)' in sql
    assert "SELECT SKIP" not in sql
    assert "SELECT FIRST" not in sql


def test_distinct_offset_retains_two_level_row_number_fallback():
    source = table("source", column("name"))
    sql = _normalized(
        select(source.c.name)
        .distinct()
        .order_by(source.c.name)
        .limit(5)
        .offset(2)
    )

    assert 'SELECT DISTINCT "source".name AS name' in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "SELECT SKIP" not in sql


def test_union_result_pagination_uses_outer_native_table_expression():
    source = table("source", column("id"))
    statement = (
        union(select(source.c.id), select(source.c.id))
        .order_by("id")
        .limit(5)
        .offset(2)
    )
    sql = _normalized(statement)

    assert sql.startswith("SELECT SKIP ")
    assert " FIRST " in sql
    assert 'FROM (SELECT "source".id AS id FROM "source" UNION ' in sql
    assert "ORDER BY anon_1.id" in sql
    assert "ROW_NUMBER" not in sql
