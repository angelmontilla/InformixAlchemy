from __future__ import annotations

import re

import pytest
from sqlalchemy import bindparam
from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy import union

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def _compile(stmt) -> str:
    return str(stmt.compile(dialect=IfxDialect_pyodbc()))


def _has_word(sql: str, word: str) -> bool:
    return re.search(rf"\b{word}\b", sql, re.IGNORECASE) is not None


def _assert_no_standard_limit_offset_fetch(sql: str) -> None:
    assert not _has_word(sql, "LIMIT"), sql
    assert not _has_word(sql, "OFFSET"), sql
    assert not _has_word(sql, "FETCH"), sql


def _assert_limit_contract(
    stmt,
    *,
    skip: bool,
    first: bool,
    row_number: bool,
) -> str:
    sql = _compile(stmt)

    _assert_no_standard_limit_offset_fetch(sql)
    assert _has_word(sql, "SKIP") is skip, sql
    assert _has_word(sql, "FIRST") is first, sql
    assert _has_word(sql, "ROW_NUMBER") is row_number, sql
    return sql


def test_plain_select_has_no_limit_offset_fetch_rewrite():
    t = table("t", column("id"))

    _assert_limit_contract(
        select(t.c.id),
        skip=False,
        first=False,
        row_number=False,
    )


def test_limit_uses_first_without_offset():
    t = table("t", column("id"))

    _assert_limit_contract(
        select(t.c.id).limit(5),
        skip=False,
        first=True,
        row_number=False,
    )


def test_offset_uses_native_skip():
    t = table("t", column("id"))

    _assert_limit_contract(
        select(t.c.id).offset(2),
        skip=True,
        first=False,
        row_number=False,
    )


def test_limit_offset_uses_native_skip_first():
    t = table("t", column("id"))

    sql = _assert_limit_contract(
        select(t.c.id).limit(5).offset(2),
        skip=True,
        first=True,
        row_number=False,
    )

    assert sql.startswith("SELECT SKIP "), sql
    assert " FIRST " in sql, sql


def test_order_by_limit_offset_keeps_native_order_by():
    t = table("t", column("id"))
    sql = _assert_limit_contract(
        select(t.c.id).order_by(t.c.id).limit(5).offset(2),
        skip=True,
        first=True,
        row_number=False,
    )

    assert sql.endswith("ORDER BY t.id"), sql


def test_integer_host_variables_use_native_skip_first():
    t = table("t", column("id"))
    sql = _assert_limit_contract(
        select(t.c.id)
        .order_by(t.c.id)
        .limit(bindparam("limit_count"))
        .offset(bindparam("offset_count")),
        skip=True,
        first=True,
        row_number=False,
    )

    assert "SKIP :offset_count FIRST :limit_count" in sql, sql


def test_expression_offset_uses_row_number_fallback():
    t = table("t", column("id"))
    expression = literal_column("1") + literal_column("1")
    sql = _assert_limit_contract(
        select(t.c.id).order_by(t.c.id).offset(expression),
        skip=False,
        first=False,
        row_number=True,
    )

    assert "ifx_rn > 1 + 1" in sql, sql


def test_expression_limit_with_simple_offset_uses_row_number_fallback():
    t = table("t", column("id"))
    expression = literal_column("1") + literal_column("1")
    sql = _assert_limit_contract(
        select(t.c.id).order_by(t.c.id).limit(expression).offset(2),
        skip=False,
        first=False,
        row_number=True,
    )

    assert "ifx_rn <= 1 + 1 + __[POSTCOMPILE_" in sql, sql


def test_distinct_order_by_offset_keeps_distinct_row_number_rewrite():
    t = table("t", column("id"))
    sql = _assert_limit_contract(
        select(t.c.id).distinct().order_by(t.c.id).limit(5).offset(2),
        skip=False,
        first=False,
        row_number=True,
    )

    assert _has_word(sql, "DISTINCT"), sql


def test_fetch_uses_first_without_offset():
    t = table("t", column("id"))

    _assert_limit_contract(
        select(t.c.id).fetch(5),
        skip=False,
        first=True,
        row_number=False,
    )


def test_fetch_offset_uses_native_skip_first():
    t = table("t", column("id"))

    _assert_limit_contract(
        select(t.c.id).fetch(5).offset(2),
        skip=True,
        first=True,
        row_number=False,
    )


def test_compound_limit_offset_uses_outer_native_pagination():
    t = table("t", column("id"))
    stmt = (
        union(select(t.c.id), select(t.c.id))
        .order_by(t.c.id)
        .limit(5)
        .offset(2)
    )
    sql = _assert_limit_contract(
        stmt,
        skip=True,
        first=True,
        row_number=False,
    )

    assert sql.startswith("SELECT SKIP "), sql
    assert "FROM (SELECT t.id AS id" in sql, sql
    assert " UNION SELECT t.id AS id" in sql, sql
    assert "ORDER BY anon_1.id" in sql, sql


def test_union_branches_retain_native_first_in_derived_tables():
    t = table("t", column("id"))
    stmt = union(
        select(t.c.id).order_by(t.c.id).limit(1),
        select(t.c.id).order_by(t.c.id).limit(1),
    ).limit(2)
    sql = _compile(stmt)

    _assert_no_standard_limit_offset_fetch(sql)
    assert len(re.findall(r"\bFIRST\b", sql, re.IGNORECASE)) == 3, sql
    assert "ROW_NUMBER" not in sql.upper(), sql


def test_fetch_percent_fails():
    t = table("t", column("id"))

    with pytest.raises(exc.CompileError, match="FETCH PERCENT"):
        _compile(select(t.c.id).fetch(5, percent=True))


def test_fetch_with_ties_fails():
    t = table("t", column("id"))

    with pytest.raises(exc.CompileError, match="FETCH WITH TIES"):
        _compile(select(t.c.id).fetch(5, with_ties=True))
