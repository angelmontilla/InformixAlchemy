from __future__ import annotations

import datetime
import inspect
from decimal import Decimal

import pytest
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    column,
    literal_column,
    literal,
    select,
    table,
    values,
)
from sqlalchemy import exc
from sqlalchemy.sql.sqltypes import NullType

from IfxAlchemy.base import IfxCompiler
from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.requirements import Requirements


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _values_cte_statement(
    *,
    literal_binds=False,
    cte_named=False,
    values_named=False,
    rows=(("a", 2), ("b", 3)),
):
    value_rows = values(
        column("col1", String),
        column("col2", Integer),
        literal_binds=literal_binds,
        name="some name" if values_named else None,
    ).data(list(rows))
    cte = value_rows.cte("cte1" if cte_named else None)
    return select(cte)


@pytest.mark.parametrize("literal_binds", [False, True])
@pytest.mark.parametrize("cte_named", [False, True])
@pytest.mark.parametrize("values_named", [False, True])
def test_values_cte_compiles_to_union_all_selects(
    dialect,
    literal_binds,
    cte_named,
    values_named,
):
    statement = _values_cte_statement(
        literal_binds=literal_binds,
        cte_named=cte_named,
        values_named=values_named,
    )

    compiled = statement.compile(dialect=dialect)
    sql = " ".join(str(compiled).split())

    assert sql.startswith("WITH ")
    assert "(col1, col2) AS" in sql
    assert "VALUES (" not in sql
    assert sql.count("SELECT") == 3
    assert sql.count("FROM sysmaster:informix.sysdual") == 2
    assert sql.count("UNION ALL") == 1

    if cte_named:
        assert sql.startswith("WITH cte1(col1, col2) AS")
        assert "FROM cte1" in sql

    if literal_binds:
        assert compiled.params == {}
        assert "SELECT 'a', 2" in sql
        assert "SELECT 'b', 3" in sql
    else:
        assert list(compiled.params.values()) == ["a", 2, "b", 3]
        assert (
            "SELECT CAST(:param_1 AS LVARCHAR), "
            "CAST(:param_2 AS INTEGER)"
        ) in sql
        assert (
            "SELECT CAST(:param_3 AS LVARCHAR), "
            "CAST(:param_4 AS INTEGER)"
        ) in sql


def test_values_outside_cte_keeps_sqlalchemy_values_compilation(dialect):
    value_rows = values(
        column("col1", String),
        column("col2", Integer),
        name="value_rows",
    ).data([("a", 2), ("b", 3)])

    sql = " ".join(str(select(value_rows).compile(dialect=dialect)).split())

    assert "VALUES (" in sql
    assert "UNION ALL" not in sql


def test_regular_select_cte_is_unchanged(dialect):
    cte = select(literal_column("1").label("value")).cte("regular_cte")

    sql = " ".join(str(select(cte).compile(dialect=dialect)).split())

    assert sql.startswith("WITH regular_cte AS")
    assert "UNION ALL" not in sql
    assert sql.count("FROM sysmaster:informix.sysdual") == 1


def test_values_cte_flattens_multiple_data_calls_in_order(dialect):
    value_rows = (
        values(
            column("col1", String),
            column("col2", Integer),
        )
        .data([("a", 1)])
        .data([("b", 2), ("c", 3)])
    )
    compiled = select(value_rows.cte("cte1")).compile(dialect=dialect)
    sql = " ".join(str(compiled).split())

    assert sql.count("FROM sysmaster:informix.sysdual") == 3
    assert sql.count("UNION ALL") == 2
    assert list(compiled.params.values()) == ["a", 1, "b", 2, "c", 3]


def test_values_cte_preserves_declared_types_for_literal_rendering(dialect):
    value_rows = values(
        column("text_value", String(20)),
        column("integer_value", Integer),
        column("boolean_value", Boolean),
        column("date_value", Date),
        column("datetime_value", DateTime),
        column("numeric_value", Numeric(10, 2)),
        column("null_value", NullType()),
        literal_binds=True,
    ).data(
        [
            (
                "quoted ' value",
                3,
                True,
                datetime.date(2026, 7, 30),
                datetime.datetime(2026, 7, 30, 9, 15),
                Decimal("12.34"),
                None,
            )
        ]
    )

    compiled = select(value_rows.cte("typed_values")).compile(
        dialect=dialect
    )
    sql = " ".join(str(compiled).split())

    assert "'quoted '' value'" in sql
    assert ", 3, 't'," in sql
    assert "'2026-07-30'" in sql
    assert "'2026-07-30 09:15:00'" in sql
    assert "12.34" in sql
    assert "NULL" in sql
    assert compiled.params == {}


def test_parameterized_values_ctes_keep_same_sql_and_distinct_params(dialect):
    first = _values_cte_statement(rows=(("a", 2), ("b", 3)))
    second = _values_cte_statement(rows=(("x", 8), ("y", 9)))

    first_compiled = first.compile(dialect=dialect)
    second_compiled = second.compile(dialect=dialect)

    assert str(first_compiled) == str(second_compiled)
    assert first_compiled.params != second_compiled.params
    assert list(first_compiled.params.values()) == ["a", 2, "b", 3]
    assert list(second_compiled.params.values()) == ["x", 8, "y", 9]
    # SQLAlchemy 2.0 deliberately leaves Values.data() statements uncached.
    assert first._generate_cache_key() is None
    assert second._generate_cache_key() is None


def test_values_cte_preserves_pyodbc_qmark_parameter_order():
    dialect = IfxDialect_pyodbc(paramstyle="qmark")
    statement = _values_cte_statement(cte_named=True)

    compiled = statement.compile(dialect=dialect)
    sql = " ".join(str(compiled).split())

    assert "SELECT CAST(? AS LVARCHAR), CAST(? AS INTEGER)" in sql
    assert (
        sql.count("SELECT CAST(? AS LVARCHAR), CAST(? AS INTEGER)")
        == 2
    )
    assert tuple(
        compiled.params[key]
        for key in compiled.positiontup
    ) == ("a", 2, "b", 3)


def test_parameterized_values_cte_uses_declared_cast_targets(dialect):
    value_rows = values(
        column("generic_text", String),
        column("bounded_text", String(20)),
        column("integer_value", Integer),
        column("boolean_value", Boolean),
        column("date_value", Date),
        column("datetime_value", DateTime),
        column("numeric_value", Numeric(10, 2)),
    ).data(
        [
            (
                "text",
                "bounded",
                3,
                True,
                datetime.date(2026, 7, 30),
                datetime.datetime(2026, 7, 30, 9, 15),
                Decimal("12.34"),
            )
        ]
    )

    compiled = select(value_rows.cte("typed_values")).compile(
        dialect=dialect
    )
    sql = " ".join(str(compiled).split())

    assert "CAST(:param_1 AS LVARCHAR)" in sql
    assert "CAST(:param_2 AS VARCHAR(20))" in sql
    assert "CAST(:param_3 AS INTEGER)" in sql
    assert "CAST(:param_4 AS BOOLEAN)" in sql
    assert "CAST(:param_5 AS DATE)" in sql
    assert "CAST(:param_6 AS DATETIME YEAR TO FRACTION(5))" in sql
    assert "CAST(:param_7 AS DECIMAL(10, 2))" in sql


def test_untyped_null_values_cte_renders_native_null_without_cast(dialect):
    value_rows = values(column("unknown_value", NullType())).data([(None,)])

    compiled = select(value_rows.cte("untyped_values")).compile(
        dialect=dialect
    )
    sql = " ".join(str(compiled).split())

    assert "SELECT NULL FROM sysmaster:informix.sysdual" in sql
    assert compiled.params == {}


def test_values_cte_does_not_cast_sql_expressions(dialect):
    value_rows = values(column("value", Integer)).data(
        [(literal_column("42"),)]
    )

    sql = " ".join(
        str(select(value_rows.cte("expression_values")).compile(
            dialect=dialect
        )).split()
    )

    assert "SELECT 42 FROM sysmaster:informix.sysdual" in sql
    assert "CAST(42" not in sql


def test_empty_values_cte_fails_with_explicit_compile_error(dialect):
    value_rows = values(column("id", Integer)).cte("empty_values")

    with pytest.raises(
        exc.CompileError,
        match="requires at least one row",
    ):
        select(value_rows).compile(dialect=dialect)


def test_mismatched_values_row_fails_with_explicit_compile_error(dialect):
    value_rows = values(
        column("col1", String),
        column("col2", Integer),
    ).data([("a",)])

    with pytest.raises(
        exc.CompileError,
        match="1 values for 2 columns",
    ):
        select(value_rows.cte("bad_values")).compile(dialect=dialect)


def test_values_cte_inside_dml_keeps_existing_literal_execute_protection(
    dialect,
):
    target = table(
        "target",
        column("id", Integer),
        column("name", String),
    )
    value_rows = values(
        column("id", Integer),
        column("name", String),
    ).data([(1, "a")]).cte("value_rows")
    statement = (
        target.update()
        .values(name="updated")
        .where(target.c.id == 2)
        .add_cte(value_rows)
    )

    compiled = statement.compile(dialect=dialect)
    sql = " ".join(str(compiled).split())

    assert "CAST(__[POSTCOMPILE_param_1] AS INTEGER)" in sql
    assert "CAST(__[POSTCOMPILE_param_2] AS LVARCHAR)" in sql
    assert "SET name=:name" in sql
    assert "target.id = :id_1" in sql


def test_lateral_values_cte_keeps_sqlalchemy_compile_error(dialect):
    value_rows = values(
        column("id", Integer),
    ).data([(1,)]).lateral().cte("lateral_values")

    with pytest.raises(
        exc.CompileError,
        match="LATERAL VALUES expression",
    ):
        select(value_rows).compile(dialect=dialect)


def test_values_cte_preserves_independent_ctes(dialect):
    supporting_cte = select(literal(1).label("value")).cte("supporting")
    value_rows = (
        values(column("id", Integer))
        .data([(1,)])
        .add_cte(supporting_cte)
        .cte("value_rows")
    )

    sql = " ".join(
        str(select(value_rows).compile(dialect=dialect)).split()
    )

    assert sql.startswith("WITH supporting AS")
    assert "value_rows(id) AS" in sql
    assert "UNION ALL" not in sql


def test_ctes_with_values_requirement_is_enabled():
    assert Requirements().ctes_with_values.enabled is True


def test_values_compiler_visitor_accepts_compiler_kwargs():
    signature = inspect.signature(IfxCompiler.visit_values)

    assert any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
