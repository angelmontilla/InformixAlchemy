from __future__ import annotations

import pytest
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Column,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    bindparam,
    column,
    func,
    literal,
    null,
    select,
    tuple_,
)
from sqlalchemy.exc import CompileError

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


def test_integer_scalar_empty_in_uses_typed_null(dialect):
    simple_col = column("simple_col", Integer())

    rendered = select(simple_col).where(simple_col.in_([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql = _normalized(rendered)

    assert "SIMPLE_COL IN (CAST(NULL AS INTEGER))" in sql
    assert "AND (1 = 0)" in sql
    assert "SELECT 1 FROM" not in sql


def test_string_scalar_empty_in_uses_typed_null(dialect):
    simple_col = column("simple_col", String(50))

    rendered = select(simple_col).where(simple_col.in_([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql = _normalized(rendered)

    assert "SIMPLE_COL IN (CAST(NULL AS VARCHAR(50)))" in sql
    assert "AND (1 = 0)" in sql


def test_date_scalar_empty_in_uses_typed_null(dialect):
    simple_col = column("simple_col", Date())

    rendered = select(simple_col).where(simple_col.in_([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql = _normalized(rendered)

    assert "SIMPLE_COL IN (CAST(NULL AS DATE))" in sql


def test_datetime_scalar_empty_in_uses_typed_null(dialect):
    simple_col = column("simple_col", DateTime())

    rendered = select(simple_col).where(simple_col.in_([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql = _normalized(rendered)

    assert (
        "SIMPLE_COL IN "
        "(CAST(NULL AS DATETIME YEAR TO FRACTION(5)))"
        in sql
    )


def test_numeric_scalar_empty_in_uses_typed_null(dialect):
    simple_col = column("simple_col", Numeric(10, 2))

    rendered = select(simple_col).where(simple_col.in_([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql = _normalized(rendered)

    assert "SIMPLE_COL IN (CAST(NULL AS DECIMAL(10, 2)))" in sql


def test_boolean_scalar_empty_in_uses_smallint(dialect):
    simple_col = column("simple_col", Boolean())

    rendered = select(simple_col).where(simple_col.in_([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql = _normalized(rendered)

    assert "SIMPLE_COL IN (CAST(NULL AS SMALLINT))" in sql


def test_scalar_empty_not_in_uses_typed_null(dialect):
    simple_col = column("simple_col", Integer())

    rendered = select(simple_col).where(simple_col.not_in([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql = _normalized(rendered)

    assert "SIMPLE_COL NOT IN (CAST(NULL AS INTEGER))" in sql
    assert "OR (1 = 1)" in sql
    assert "SELECT 1 FROM" not in sql


def test_expanding_empty_bind_uses_typed_null(dialect):
    simple_col = column("simple_col", Integer())

    statement = select(simple_col).where(
        simple_col.in_(
            bindparam(
                "values",
                value=[],
                expanding=True,
            )
        )
    )

    rendered = statement.compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql_text = _normalized(rendered)

    assert (
        "IN (CAST(NULL AS INTEGER)) AND (1 = 0)"
        in sql_text
    )
    assert "IN (SELECT" not in sql_text


def test_tuple_empty_in_remains_explicitly_unsupported(dialect):
    statement = select(
        tuple_(
            literal(1),
            literal(2),
        ).in_([])
    )

    with pytest.raises(
        CompileError,
        match="tuple-valued empty sets",
    ):
        statement.compile(
            dialect=dialect,
            compile_kwargs={"render_postcompile": True},
        )


def test_untyped_null_empty_in_casts_both_sides(dialect):
    rendered = select(null().in_([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql_text = _normalized(rendered)

    assert (
        "CAST(NULL AS SMALLINT) "
        "IN (CAST(NULL AS SMALLINT)) "
        "AND (1 = 0)"
        in sql_text
    )
    assert "SELECT 1 FROM" not in sql_text


def test_untyped_null_empty_not_in_casts_both_sides(dialect):
    rendered = select(null().not_in([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql_text = _normalized(rendered)

    assert (
        "CAST(NULL AS SMALLINT) "
        "NOT IN (CAST(NULL AS SMALLINT)) "
        "OR (1 = 1)"
        in sql_text
    )
    assert "SELECT 1 FROM" not in sql_text


def test_typed_left_operand_is_not_cast_again(dialect):
    simple_col = column("simple_col", Integer())

    rendered = select(simple_col).where(simple_col.in_([])).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql_text = _normalized(rendered)

    assert "SIMPLE_COL IN (CAST(NULL AS INTEGER))" in sql_text
    assert "CAST(SIMPLE_COL AS INTEGER)" not in sql_text


def test_typed_expanding_bind_types_untyped_left(dialect):
    statement = select(
        null().in_(
            bindparam(
                "values",
                value=[],
                expanding=True,
                type_=String(20),
            )
        )
    )

    rendered = statement.compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    sql_text = _normalized(rendered)

    assert (
        "CAST(NULL AS VARCHAR(20)) "
        "IN (CAST(NULL AS VARCHAR(20))) "
        "AND (1 = 0)"
        in sql_text
    )


def test_integer_floor_division_uses_floor(dialect):
    rendered = select(literal(15) // 10).compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )

    assert "FLOOR(15 / 10)" in _normalized(rendered)



def _insert_from_select_tables():
    metadata = MetaData()
    source = Table(
        "manual_pk",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("data", String(50)),
    )
    serial_target = Table(
        "autoinc_pk",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
    )
    defaults_target = Table(
        "includes_defaults",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
        Column("x", Integer, default=5),
        Column(
            "y",
            Integer,
            default=literal(2) + literal(2),
        ),
    )
    return source, serial_target, defaults_target


def test_insert_from_select_compiles_standard_informix_form(dialect):
    source, _, _ = _insert_from_select_tables()
    statement = source.insert().inline().from_select(
        ("id", "data"),
        select(source.c.id + 5, source.c.data),
    )

    compiled = statement.compile(
        dialect=dialect,
        compile_kwargs={"render_postcompile": True},
    )
    sql_text = _normalized(compiled)

    assert sql_text.startswith(
        'INSERT INTO MANUAL_PK (ID, "DATA") SELECT '
    )
    assert "MANUAL_PK.ID + 5" in sql_text
    assert 'MANUAL_PK."DATA"' in sql_text
    assert "FROM MANUAL_PK" in sql_text


def test_insert_from_select_omits_serial_target_column(dialect):
    source, serial_target, _ = _insert_from_select_tables()
    statement = serial_target.insert().from_select(
        ("data",),
        select(source.c.data),
    )

    compiled = statement.compile(dialect=dialect)
    sql_text = _normalized(compiled)

    assert sql_text.startswith(
        'INSERT INTO AUTOINC_PK ("DATA") SELECT '
    )
    assert "(ID," not in sql_text
    assert compiled.inline is True
    assert compiled.insert_prefetch == []


def test_insert_from_select_expands_column_defaults(dialect):
    _, _, defaults_target = _insert_from_select_tables()
    statement = defaults_target.insert().inline().from_select(
        ("id", "data"),
        select(defaults_target.c.id + 5, defaults_target.c.data),
    )

    compiled = statement.compile(dialect=dialect)
    sql_text = _normalized(compiled)

    assert sql_text.startswith(
        'INSERT INTO INCLUDES_DEFAULTS (ID, "DATA", X, Y) SELECT '
    )
    assert [column.name for column in compiled.insert_prefetch] == ["x"]
    assert "POSTCOMPILE_X" in sql_text
    assert "POSTCOMPILE_PARAM_1" in sql_text
    assert "POSTCOMPILE_PARAM_2" in sql_text


def test_group_by_composed_expression_compiles_for_informix(dialect):
    metadata = MetaData()
    table = Table(
        "some_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("x", Integer),
        Column("y", Integer),
    )
    expression = (table.c.x + table.c.y).label("lx")
    statement = (
        select(func.count(table.c.id), expression)
        .group_by(expression)
        .order_by(expression)
    )

    sql_text = _normalized(statement.compile(dialect=dialect))

    assert (
        "SELECT COUNT(SOME_TABLE.ID) AS COUNT_1, "
        "SOME_TABLE.X + SOME_TABLE.Y AS LX"
        in sql_text
    )
    assert "GROUP BY LX" in sql_text
    assert "ORDER BY LX" in sql_text



def test_group_by_non_projected_label_keeps_expression(dialect):
    metadata = MetaData()
    table = Table(
        "some_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("x", Integer),
        Column("y", Integer),
    )
    expression = (table.c.x + table.c.y).label("lx")
    statement = (
        select(func.count(table.c.id))
        .group_by(expression)
    )

    sql_text = _normalized(statement.compile(dialect=dialect))

    assert "GROUP BY SOME_TABLE.X + SOME_TABLE.Y" in sql_text
    assert "GROUP BY LX" not in sql_text

def test_empty_insert_uses_zero_for_serial_autoincrement(dialect):
    metadata = MetaData()
    table = Table(
        "autoinc_pk",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
    )

    compiled = table.insert().compile(
        dialect=dialect,
        column_keys=[],
    )

    assert _normalized(compiled) == (
        "INSERT INTO AUTOINC_PK (ID) VALUES (:ID)"
    )
    assert compiled.params == {"id": 0}


def test_empty_insert_compilation_is_reusable_for_executemany(dialect):
    metadata = MetaData()
    table = Table(
        "autoinc_pk",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
    )

    compiled = table.insert().compile(
        dialect=dialect,
        column_keys=[],
    )

    parameter_sets = [
        compiled.construct_params({})
        for _ in range(3)
    ]

    assert _normalized(compiled) == (
        "INSERT INTO AUTOINC_PK (ID) VALUES (:ID)"
    )
    assert parameter_sets == [
        {"id": 0},
        {"id": 0},
        {"id": 0},
    ]


def test_empty_insert_without_autoincrement_remains_unsupported(dialect):
    metadata = MetaData()
    table = Table(
        "no_generated_defaults",
        metadata,
        Column("data", String(50)),
    )

    with pytest.raises(CompileError, match="does not support empty inserts"):
        table.insert().compile(
            dialect=dialect,
            column_keys=[],
        )

