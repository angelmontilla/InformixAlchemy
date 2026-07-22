from __future__ import annotations

import pytest
from sqlalchemy import Integer, column, literal, select

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _normalized(sql):
    return " ".join(str(sql).upper().split())


def test_is_distinct_from_is_emulated_with_null_safe_case(dialect):
    left = column("left_value", Integer)
    right = column("right_value", Integer)

    sql = _normalized(
        select(left.is_distinct_from(right)).compile(dialect=dialect)
    )

    assert "CASE WHEN LEFT_VALUE IS NULL AND RIGHT_VALUE IS NULL THEN 0" in sql
    assert "WHEN LEFT_VALUE IS NULL OR RIGHT_VALUE IS NULL THEN 1" in sql
    assert "WHEN LEFT_VALUE = RIGHT_VALUE THEN 0" in sql
    assert "ELSE 1 END = 1" in sql


def test_is_not_distinct_from_is_emulated_with_null_safe_case(dialect):
    left = column("left_value", Integer)
    right = column("right_value", Integer)

    sql = _normalized(
        select(left.is_not_distinct_from(right)).compile(dialect=dialect)
    )

    assert "CASE WHEN LEFT_VALUE IS NULL AND RIGHT_VALUE IS NULL THEN 1" in sql
    assert "WHEN LEFT_VALUE IS NULL OR RIGHT_VALUE IS NULL THEN 0" in sql
    assert "WHEN LEFT_VALUE = RIGHT_VALUE THEN 1" in sql
    assert "ELSE 0 END = 1" in sql


def test_projection_binds_use_literal_execute(dialect):
    compiled = select(literal(1)).compile(dialect=dialect)

    assert compiled.literal_execute_params
    assert "POSTCOMPILE" in str(compiled)

    rendered = select(literal(1)).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )
    assert "SELECT 1 AS" in _normalized(rendered)


def test_concatenation_projection_renders_typed_literals(dialect):
    rendered = select(literal("a") + "b").compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    assert "SELECT 'A' || 'B' AS" in _normalized(rendered)
