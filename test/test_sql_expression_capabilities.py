from __future__ import annotations

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    bindparam,
    except_,
    intersect,
    literal,
    literal_column,
    select,
    union,
)
from sqlalchemy.sql import and_

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


@pytest.fixture
def tables():
    metadata = MetaData()
    source = Table(
        "some_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
        Column("parent_id", ForeignKey("some_table.id")),
        Column("enabled", Boolean),
    )
    target = Table(
        "some_other_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
        Column("parent_id", Integer),
    )
    return source, target


def _normalized(statement, dialect):
    return " ".join(str(statement.compile(dialect=dialect)).upper().split())


def test_intersect_compiles_with_native_operator(dialect, tables):
    source, target = tables

    sql = _normalized(
        intersect(select(source.c.id), select(target.c.id)),
        dialect,
    )

    assert " INTERSECT " in sql


def test_except_compiles_with_native_operator(dialect, tables):
    source, target = tables

    sql = _normalized(
        except_(select(source.c.id), select(target.c.id)),
        dialect,
    )

    assert " EXCEPT " in sql


def test_nonrecursive_cte_uses_informix_with_preamble(dialect, tables):
    source, _ = tables
    cte = select(source).where(source.c.id > 1).cte("some_cte")

    sql = _normalized(select(cte.c.id), dialect)

    assert sql.startswith("WITH SOME_CTE AS")
    assert "WITH RECURSIVE" not in sql
    assert "SELECT SOME_CTE.ID FROM SOME_CTE" in sql


def test_recursive_cte_omits_recursive_keyword(dialect, tables):
    source, _ = tables
    cte = select(source).where(source.c.id == 1).cte(
        "some_cte",
        recursive=True,
    )
    cte_alias = cte.alias("parent_rows")
    source_alias = source.alias("child_rows")
    cte = cte.union_all(
        select(source_alias).where(
            source_alias.c.parent_id == cte_alias.c.id
        )
    )

    sql = _normalized(select(cte.c.id), dialect)

    assert sql.startswith("WITH SOME_CTE(")
    assert "WITH RECURSIVE" not in sql
    assert "UNION ALL" in sql


def test_cte_update_is_rewritten_to_correlated_exists(dialect, tables):
    source, target = tables
    cte = select(source).where(source.c.id > 1).cte("some_cte")
    statement = (
        target.update()
        .values(parent_id=5)
        .where(target.c.data == cte.c.data)
    )

    sql = _normalized(statement, dialect)

    assert sql.startswith("WITH SOME_CTE AS")
    assert "UPDATE SOME_OTHER_TABLE SET PARENT_ID=" in sql
    assert "WHERE EXISTS (SELECT 1 FROM SOME_CTE" in sql
    assert 'SOME_OTHER_TABLE."DATA" = SOME_CTE."DATA"' in sql
    assert " UPDATE FROM " not in sql


def test_cte_delete_is_rewritten_to_correlated_exists(dialect, tables):
    source, target = tables
    cte = select(source).where(source.c.id > 1).cte("some_cte")
    statement = target.delete().where(target.c.data == cte.c.data)

    sql = _normalized(statement, dialect)

    assert sql.startswith("WITH SOME_CTE AS")
    assert "DELETE FROM SOME_OTHER_TABLE WHERE EXISTS" in sql
    assert "FROM SOME_CTE" in sql
    assert 'SOME_OTHER_TABLE."DATA" = SOME_CTE."DATA"' in sql
    assert " USING " not in sql


def test_regular_multitable_update_uses_same_exists_emulation(
    dialect,
    tables,
):
    source, target = tables
    statement = (
        target.update()
        .values(parent_id=5)
        .where(target.c.data == source.c.data)
    )

    sql = _normalized(statement, dialect)

    assert "UPDATE SOME_OTHER_TABLE SET PARENT_ID=" in sql
    assert "WHERE EXISTS (SELECT 1 FROM SOME_TABLE" in sql


def test_boolean_predicate_projection_uses_case(dialect, tables):
    source, _ = tables

    sql = _normalized(select(source.c.id == 1), dialect)

    assert "SELECT CASE WHEN (SOME_TABLE.ID =" in sql
    assert "THEN 1 ELSE 0 END AS" in sql


def test_boolean_clause_list_projection_uses_single_case(dialect, tables):
    source, _ = tables
    predicate = and_(source.c.id > 1, source.c.id < 4)

    sql = _normalized(select(predicate), dialect)

    assert sql.count("CASE WHEN") == 1
    assert "SOME_TABLE.ID >" in sql
    assert "AND SOME_TABLE.ID <" in sql


def test_native_boolean_column_projection_is_not_rewritten(dialect, tables):
    source, _ = tables

    sql = _normalized(select(source.c.enabled), dialect)

    assert "SELECT SOME_TABLE.ENABLED" in sql
    assert "CASE WHEN" not in sql


def test_limited_union_branches_use_derived_tables(dialect, tables):
    source, _ = tables
    first = (
        select(source.c.id)
        .where(source.c.id == 2)
        .order_by(source.c.id)
        .limit(1)
    )
    second = (
        select(source.c.id)
        .where(source.c.id == 3)
        .order_by(source.c.id)
        .limit(1)
    )

    sql = _normalized(union(first, second).order_by(source.c.id), dialect)

    assert sql.startswith("SELECT ANON_1.ID FROM (SELECT FIRST")
    assert " AS ANON_1 UNION SELECT ANON_2.ID FROM (SELECT FIRST" in sql
    assert not sql.startswith("(SELECT")


def test_ordered_union_branches_without_limit_use_derived_tables(
    dialect,
    tables,
):
    source, _ = tables
    first = select(source.c.id).where(source.c.id == 2).order_by(source.c.id)
    second = select(source.c.id).where(source.c.id == 3).order_by(source.c.id)

    sql = _normalized(union(first, second).order_by(source.c.id), dialect)

    assert sql.startswith("SELECT ANON_1.ID FROM (SELECT")
    assert " AS ANON_1 UNION SELECT ANON_2.ID FROM (SELECT" in sql
    assert "ORDER BY SOME_TABLE.ID" in sql


def test_order_by_expression_uses_projected_label(dialect, tables):
    source, _ = tables
    label = source.c.data.label("foo")

    sql = _normalized(
        select(label).order_by(label + literal("bar")),
        dialect,
    )

    assert 'SOME_TABLE."DATA" AS FOO' in sql
    assert "ORDER BY FOO ||" in sql
    assert "ORDER BY SOME_TABLE.DATA ||" not in sql


def test_nonprojected_label_in_order_expression_keeps_source_expression(
    dialect,
    tables,
):
    source, _ = tables
    label = source.c.data.label("foo")

    sql = _normalized(
        select(source.c.id).order_by(label + literal("bar")),
        dialect,
    )

    assert 'ORDER BY SOME_TABLE."DATA" ||' in sql
    assert "ORDER BY FOO ||" not in sql


def test_fetch_first_maps_to_first(dialect, tables):
    source, _ = tables

    sql = _normalized(
        select(source).order_by(source.c.id).fetch(2),
        dialect,
    )

    assert sql.startswith("SELECT FIRST")
    assert "FETCH FIRST" not in sql


def test_fetch_without_order_by_maps_to_first(dialect, tables):
    source, _ = tables

    sql = _normalized(select(source).fetch(10), dialect)

    assert sql.startswith("SELECT FIRST")
    assert "ORDER BY" not in sql


def test_bound_fetch_uses_row_number_emulation(dialect, tables):
    source, _ = tables

    sql = _normalized(
        select(source).order_by(source.c.id).fetch(bindparam("row_count")),
        dialect,
    )

    assert "ROW_NUMBER() OVER (ORDER BY SOME_TABLE.ID)" in sql
    assert "IFX_RN <= :ROW_COUNT" in sql
    assert "SELECT FIRST" not in sql


def test_fetch_expression_with_offset_uses_row_number_bounds(
    dialect,
    tables,
):
    source, _ = tables
    expression = literal_column("1") + literal_column("1")
    statement = (
        select(source)
        .order_by(source.c.id)
        .fetch(expression)
        .offset(expression)
    )

    sql = _normalized(statement, dialect)

    assert "ROW_NUMBER() OVER (ORDER BY SOME_TABLE.ID)" in sql
    assert "IFX_RN > 1 + 1" in sql
    assert "IFX_RN <= 1 + 1 + 1 + 1" in sql
    assert "SELECT FIRST" not in sql


def test_cte_insert_from_select_compiles(dialect, tables):
    source, target = tables
    cte = select(source).where(source.c.id > 1).cte("some_cte")
    statement = target.insert().from_select(
        ["id", "data", "parent_id"],
        select(cte.c.id, cte.c.data, cte.c.parent_id),
    )

    sql = _normalized(statement, dialect)

    assert sql.startswith("WITH SOME_CTE AS")
    assert "INSERT INTO SOME_OTHER_TABLE (ID, \"DATA\", PARENT_ID)" in sql
    assert "SELECT SOME_CTE.ID, SOME_CTE.\"DATA\", SOME_CTE.PARENT_ID" in sql


def test_cte_delete_with_scalar_subquery_compiles(dialect, tables):
    source, target = tables
    cte = select(source).where(source.c.id > 1).cte("some_cte")
    statement = target.delete().where(
        target.c.data
        == select(cte.c.data)
        .where(cte.c.id == target.c.id)
        .scalar_subquery()
    )

    sql = _normalized(statement, dialect)

    assert sql.startswith("WITH SOME_CTE AS")
    assert "DELETE FROM SOME_OTHER_TABLE WHERE" in sql
    assert "= (SELECT SOME_CTE.\"DATA\" FROM SOME_CTE" in sql
    assert "SOME_CTE.ID = SOME_OTHER_TABLE.ID" in sql


def test_bound_fetch_with_offset_uses_row_number_bounds(dialect, tables):
    source, _ = tables
    statement = (
        select(source)
        .order_by(source.c.id)
        .fetch(bindparam("fetch_count"))
        .offset(bindparam("offset_count"))
    )

    sql = _normalized(statement, dialect)

    assert "ROW_NUMBER() OVER (ORDER BY SOME_TABLE.ID)" in sql
    assert "IFX_RN > :OFFSET_COUNT" in sql
    assert "IFX_RN <= :FETCH_COUNT + :OFFSET_COUNT" in sql
    assert "SELECT FIRST" not in sql


@pytest.mark.parametrize("operation", ["update", "delete"])
def test_cte_dml_uses_literal_execute_only_inside_cte(
    dialect,
    tables,
    operation,
):
    """Mirror the parameter shape used by SQLAlchemy's official CTETest."""
    source, target = tables
    cte = (
        select(source)
        .where(source.c.data.in_(["d2", "d3", "d4"]))
        .cte("some_cte")
    )

    if operation == "update":
        statement = (
            target.update()
            .values(parent_id=5)
            .where(target.c.data == cte.c.data)
        )
    else:
        statement = target.delete().where(target.c.data == cte.c.data)

    compiled = statement.compile(dialect=dialect)
    sql_text = str(compiled)

    assert "__[POSTCOMPILE_data_1]" in sql_text
    assert compiled.literal_execute_params
    assert any(
        "data" in str(parameter.key)
        for parameter in compiled.literal_execute_params
    )

    if operation == "update":
        assert ":parent_id" in sql_text
        assert all(
            parameter.key != "parent_id"
            for parameter in compiled.literal_execute_params
        )


def test_cte_scalar_delete_uses_literal_execute_for_cte_values(
    dialect,
    tables,
):
    source, target = tables
    cte = (
        select(source)
        .where(source.c.data.in_(["d2", "d3", "d4"]))
        .cte("some_cte")
    )
    statement = target.delete().where(
        target.c.data
        == select(cte.c.data)
        .where(cte.c.id == target.c.id)
        .scalar_subquery()
    )

    compiled = statement.compile(dialect=dialect)

    assert "__[POSTCOMPILE_data_1]" in str(compiled)
    assert compiled.literal_execute_params
