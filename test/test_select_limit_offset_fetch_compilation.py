from __future__ import annotations

import re

import pytest
from sqlalchemy import exc
from sqlalchemy import column, select, table

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def _compile(stmt) -> str:
    return str(stmt.compile(dialect=IfxDialect_pyodbc()))


def _has_word(sql: str, word: str) -> bool:
    return re.search(rf"\b{word}\b", sql, re.IGNORECASE) is not None


def _assert_no_standard_limit_offset_fetch(sql: str) -> None:
    assert not _has_word(sql, "LIMIT"), sql
    assert not _has_word(sql, "OFFSET"), sql
    assert not _has_word(sql, "FETCH"), sql


def _assert_limit_contract(stmt, *, first: bool, row_number: bool) -> None:
    sql = _compile(stmt)

    _assert_no_standard_limit_offset_fetch(sql)
    assert _has_word(sql, "FIRST") is first, sql
    assert _has_word(sql, "ROW_NUMBER") is row_number, sql


def test_plain_select_has_no_limit_offset_fetch_rewrite():
    t = table("t", column("id"))

    _assert_limit_contract(select(t.c.id), first=False, row_number=False)


def test_limit_uses_first_without_offset():
    t = table("t", column("id"))

    _assert_limit_contract(select(t.c.id).limit(5), first=True, row_number=False)


def test_offset_uses_row_number_without_first():
    t = table("t", column("id"))

    _assert_limit_contract(select(t.c.id).offset(2), first=False, row_number=True)


def test_limit_offset_uses_row_number_without_first():
    t = table("t", column("id"))

    _assert_limit_contract(
        select(t.c.id).limit(5).offset(2),
        first=False,
        row_number=True,
    )


def test_order_by_offset_uses_row_number_ordering():
    t = table("t", column("id"))
    sql = _compile(select(t.c.id).order_by(t.c.id).offset(2))

    _assert_no_standard_limit_offset_fetch(sql)
    assert not _has_word(sql, "FIRST"), sql
    assert _has_word(sql, "ROW_NUMBER"), sql
    assert "OVER (ORDER BY t.id)" in sql


def test_order_by_limit_offset_uses_row_number_upper_bound():
    t = table("t", column("id"))
    sql = _compile(select(t.c.id).order_by(t.c.id).limit(5).offset(2))

    _assert_no_standard_limit_offset_fetch(sql)
    assert not _has_word(sql, "FIRST"), sql
    assert _has_word(sql, "ROW_NUMBER"), sql
    assert "OVER (ORDER BY t.id)" in sql
    assert "<= 7" in sql


def test_distinct_order_by_offset_keeps_distinct_inside_row_number_rewrite():
    t = table("t", column("id"))
    sql = _compile(select(t.c.id).distinct().order_by(t.c.id).offset(2))

    _assert_no_standard_limit_offset_fetch(sql)
    assert not _has_word(sql, "FIRST"), sql
    assert _has_word(sql, "ROW_NUMBER"), sql
    assert _has_word(sql, "DISTINCT"), sql


def test_fetch_uses_first_without_offset():
    t = table("t", column("id"))

    _assert_limit_contract(select(t.c.id).fetch(5), first=True, row_number=False)


def test_fetch_offset_uses_row_number_without_first():
    t = table("t", column("id"))

    _assert_limit_contract(
        select(t.c.id).fetch(5).offset(2),
        first=False,
        row_number=True,
    )


def test_fetch_percent_fails():
    t = table("t", column("id"))

    with pytest.raises(exc.CompileError, match="FETCH PERCENT"):
        _compile(select(t.c.id).fetch(5, percent=True))


def test_fetch_with_ties_fails():
    t = table("t", column("id"))

    with pytest.raises(exc.CompileError, match="FETCH WITH TIES"):
        _compile(select(t.c.id).fetch(5, with_ties=True))
