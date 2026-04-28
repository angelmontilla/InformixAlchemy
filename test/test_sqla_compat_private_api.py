from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy import column, insert, literal_column, select, table

from IfxAlchemy import sqla_compat
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def test_sqlalchemy_version_guard_is_available():
    assert sqla_compat.SQLALCHEMY_VERSION >= sqla_compat.Version("2.0.0")
    assert isinstance(sqla_compat.SQLALCHEMY_IS_21_PLUS, bool)


def test_get_limit_clause_and_value():
    stmt = select(literal_column("1")).limit(5)

    assert sqla_compat.get_limit_clause(stmt) is not None
    assert sqla_compat.get_limit_value(stmt) == 5


def test_get_offset_clause_and_value():
    stmt = select(literal_column("1")).offset(2)

    assert sqla_compat.get_offset_clause(stmt) is not None
    assert sqla_compat.get_offset_value(stmt) == 2


def test_get_limit_and_offset_together():
    stmt = select(literal_column("1")).limit(5).offset(2)

    assert sqla_compat.get_limit_clause(stmt) is not None
    assert sqla_compat.get_limit_value(stmt) == 5
    assert sqla_compat.get_offset_clause(stmt) is not None
    assert sqla_compat.get_offset_value(stmt) == 2


def test_get_fetch_clause_and_default_options():
    stmt = select(literal_column("1")).fetch(5)

    assert sqla_compat.get_fetch_clause(stmt) is not None
    assert sqla_compat.get_fetch_clause_options(stmt) == {
        "percent": False,
        "with_ties": False,
    }


def test_get_fetch_clause_percent_option():
    stmt = select(literal_column("1")).fetch(5, percent=True)

    assert sqla_compat.get_fetch_clause(stmt) is not None
    assert sqla_compat.get_fetch_clause_options(stmt)["percent"] is True


def test_get_fetch_clause_with_ties_option():
    stmt = select(literal_column("1")).fetch(5, with_ties=True)

    assert sqla_compat.get_fetch_clause(stmt) is not None
    assert sqla_compat.get_fetch_clause_options(stmt)["with_ties"] is True


def test_get_distinct():
    stmt = select(literal_column("1")).distinct()

    assert sqla_compat.get_distinct(stmt) is True


def test_get_order_by_clauses():
    stmt = select(literal_column("1")).order_by(literal_column("1"))

    assert tuple(sqla_compat.get_order_by_clauses(stmt))


def test_get_select_for_update():
    tbl = table("t", column("id"))
    stmt = select(tbl.c.id).with_for_update()

    assert sqla_compat.get_select_for_update(stmt) is not None


def test_clone_select():
    stmt = select(literal_column("1")).limit(5)

    cloned = sqla_compat.clone_select(stmt)

    assert cloned is not stmt
    assert sqla_compat.get_limit_value(cloned) == 5


def test_simple_int_clause_and_offset_or_limit_clause_asint():
    stmt = select(literal_column("1")).limit(5).offset(2)

    limit_clause = sqla_compat.get_limit_clause(stmt)
    offset_clause = sqla_compat.get_offset_clause(stmt)

    assert sqla_compat.simple_int_clause(stmt, limit_clause) is True
    assert sqla_compat.simple_int_clause(stmt, offset_clause) is True
    assert sqla_compat.offset_or_limit_clause_asint(
        stmt, limit_clause, "limit"
    ) == 5
    assert sqla_compat.offset_or_limit_clause_asint(
        stmt, offset_clause, "offset"
    ) == 2


def test_get_table_autoincrement_column_and_sorted_constraints():
    metadata = MetaData()
    tbl = Table("t", metadata, Column("id", Integer, primary_key=True))

    assert sqla_compat.get_table_autoincrement_column(tbl) is tbl.c.id
    assert tuple(sqla_compat.get_table_sorted_constraints(tbl))


def test_identifier_requires_quotes():
    preparer = IfxDialect_pyodbc().identifier_preparer

    assert sqla_compat.identifier_requires_quotes(preparer, "select") is True
    assert sqla_compat.identifier_requires_quotes(preparer, "plain_name") is False


def test_get_dml_compile_state_and_statement_returning():
    metadata = MetaData()
    tbl = Table("t", metadata, Column("id", Integer, primary_key=True))
    stmt = insert(tbl).returning(tbl.c.id)

    compiled = stmt.compile(dialect=IfxDialect_pyodbc())

    assert sqla_compat.get_dml_compile_state(compiled) is not None
    assert sqla_compat.compiled_returns_rows(compiled) is True
    assert tuple(sqla_compat.get_statement_returning(stmt)) == (tbl.c.id,)


def test_get_limit_state():
    stmt = (
        select(literal_column("1"))
        .distinct()
        .order_by(literal_column("1"))
        .limit(5)
        .offset(2)
    )

    state = sqla_compat.get_limit_state(stmt)

    assert state.limit_value == 5
    assert state.offset_value == 2
    assert state.fetch_clause is None
    assert state.distinct is True
    assert tuple(state.order_by_clauses)


@pytest.mark.parametrize(
    "stmt",
    [
        select(literal_column("1")).limit(5),
        select(literal_column("1")).offset(2),
        select(literal_column("1")).limit(5).offset(2),
        select(literal_column("1")).fetch(5),
        select(literal_column("1")).fetch(5, percent=True),
        select(literal_column("1")).fetch(5, with_ties=True),
        select(literal_column("1")).distinct(),
        select(literal_column("1")).order_by(literal_column("1")),
    ],
)
def test_get_limit_state_accepts_representative_selects(stmt):
    state = sqla_compat.get_limit_state(stmt)

    assert state.fetch_options["percent"] in (True, False)
    assert state.fetch_options["with_ties"] in (True, False)
