from __future__ import annotations

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    MetaData,
    Table,
    false,
    inspect,
    literal,
    select,
    text,
    true,
)
from sqlalchemy.schema import CreateTable

from IfxAlchemy import BOOLEAN
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def _normalized(sql) -> str:
    return " ".join(str(sql).upper().split())


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def test_generic_boolean_adapts_to_native_informix_type(dialect):
    implementation = Boolean().dialect_impl(dialect)

    assert type(implementation) is BOOLEAN
    assert dialect.supports_native_boolean is True


def test_native_boolean_ddl_and_defaults(dialect):
    table = Table(
        "ifx_native_boolean",
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("enabled", Boolean, nullable=False, server_default=true()),
        Column("disabled", BOOLEAN, nullable=False, server_default=false()),
    )

    sql = _normalized(CreateTable(table).compile(dialect=dialect))

    assert "ENABLED BOOLEAN DEFAULT 'T' NOT NULL" in sql
    assert "DISABLED BOOLEAN DEFAULT 'F' NOT NULL" in sql
    assert "SMALLINT" not in sql


def test_native_boolean_bind_processor_is_strict(dialect):
    process = BOOLEAN().bind_processor(dialect)

    assert process(True) == "t"
    assert process(False) == "f"
    assert process(1) == "t"
    assert process(0) == "f"
    assert process(None) is None

    with pytest.raises(ValueError, match="not None, True, or False"):
        process(2)

    with pytest.raises(TypeError, match="Not a boolean value"):
        process("true")


@pytest.mark.parametrize(
    ("database_value", "expected"),
    [
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        ("t", True),
        ("T ", True),
        ("true", True),
        ("f", False),
        (" F ", False),
        ("false", False),
        (b"t", True),
        (b"f", False),
        (b"t\x00", True),
        (b" f \x00", False),
        (b"\x01", True),
        (b"\x00", False),
        (memoryview(b"t"), True),
        (bytearray(b"f"), False),
        (None, None),
    ],
)
def test_native_boolean_result_processor(dialect, database_value, expected):
    process = BOOLEAN().result_processor(dialect, None)

    assert process(database_value) is expected


def test_native_boolean_result_processor_rejects_unknown_values(dialect):
    process = BOOLEAN().result_processor(dialect, None)

    with pytest.raises(ValueError, match="integer other than 0 or 1"):
        process(2)

    with pytest.raises(ValueError, match="unsupported value"):
        process("unknown")


def test_native_boolean_literals_predicates_and_comparisons(dialect):
    table = Table(
        "ifx_bool_predicates",
        MetaData(),
        Column("id", Integer),
        Column("enabled", Boolean),
    )

    statements = {
        "true_literal": select(true()),
        "false_literal": select(false()),
        "bound_true": select(literal(True, type_=Boolean())),
        "bound_false": select(literal(False, type_=Boolean())),
        "where_true": select(table.c.id).where(table.c.enabled),
        "where_false": select(table.c.id).where(~table.c.enabled),
        "equals_true": select(table.c.id).where(table.c.enabled == True),  # noqa: E712
        "equals_false": select(table.c.id).where(table.c.enabled == False),  # noqa: E712
        "is_true": select(table.c.id).where(table.c.enabled.is_(True)),
        "is_false": select(table.c.id).where(table.c.enabled.is_(False)),
        "is_not_true": select(table.c.id).where(table.c.enabled.is_not(True)),
        "is_not_false": select(table.c.id).where(table.c.enabled.is_not(False)),
    }
    compiled = {
        name: _normalized(
            statement.compile(
                dialect=dialect,
                compile_kwargs={"literal_binds": True},
            )
        )
        for name, statement in statements.items()
    }

    assert "SELECT 'T' AS" in compiled["true_literal"]
    assert "SELECT 'F' AS" in compiled["false_literal"]
    assert "SELECT 'T' AS" in compiled["bound_true"]
    assert "SELECT 'F' AS" in compiled["bound_false"]
    assert "WHERE IFX_BOOL_PREDICATES.ENABLED = 'T'" in compiled["where_true"]
    assert "WHERE IFX_BOOL_PREDICATES.ENABLED = 'F'" in compiled["where_false"]
    assert "WHERE IFX_BOOL_PREDICATES.ENABLED = 'T'" in compiled["equals_true"]
    assert "WHERE IFX_BOOL_PREDICATES.ENABLED = 'F'" in compiled["equals_false"]
    assert "WHERE IFX_BOOL_PREDICATES.ENABLED = 'T'" in compiled["is_true"]
    assert "WHERE IFX_BOOL_PREDICATES.ENABLED = 'F'" in compiled["is_false"]
    assert "ENABLED != 'T' OR IFX_BOOL_PREDICATES.ENABLED IS NULL" in compiled[
        "is_not_true"
    ]
    assert "ENABLED != 'F' OR IFX_BOOL_PREDICATES.ENABLED IS NULL" in compiled[
        "is_not_false"
    ]


def test_boolean_comparison_projection_returns_native_boolean(dialect):
    table = Table(
        "ifx_bool_projection",
        MetaData(),
        Column("id", Integer),
    )

    sql = _normalized(
        select((table.c.id > 5).label("is_high")).compile(
            dialect=dialect,
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "CASE WHEN (IFX_BOOL_PROJECTION.ID > 5)" in sql
    assert "THEN 'T' ELSE 'F' END AS IS_HIGH" in sql


def test_native_boolean_reflection_decodes_direct_and_opaque_codes(dialect):
    reflector = dialect._reflector

    direct, direct_autoincrement, direct_nullable = reflector._decode_ifx_type(
        45,
        1,
    )
    opaque, opaque_autoincrement, opaque_nullable = reflector._decode_ifx_type(
        41,
        1,
        extended_id=3,
        extended_type_name="boolean",
    )

    assert type(direct) is BOOLEAN
    assert direct_autoincrement is False
    assert direct_nullable is True
    assert type(opaque) is BOOLEAN
    assert opaque_autoincrement is False
    assert opaque_nullable is True


def test_native_boolean_default_reflection_preserves_catalog_literal(dialect):
    reflector = dialect._reflector

    assert reflector._decode_default("L", "t", 45) == "t"
    assert reflector._decode_default("L", "f", 45) == "f"


def test_reflected_boolean_defaults_recompile_to_informix_literals(dialect):
    table = Table(
        "ifx_reflected_boolean_defaults",
        MetaData(),
        Column("enabled", BOOLEAN, server_default=text("true")),
        Column("disabled", BOOLEAN, server_default=text("false")),
    )

    sql = _normalized(CreateTable(table).compile(dialect=dialect))

    assert "ENABLED BOOLEAN DEFAULT 'T'" in sql
    assert "DISABLED BOOLEAN DEFAULT 'F'" in sql


@pytest.mark.requires_informix
def test_native_boolean_round_trip_defaults_predicates_and_reflection(
    engine,
    name_factory,
):
    table_name = name_factory("sa_boolean_")
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("enabled", Boolean, nullable=True),
        Column(
            "default_enabled",
            BOOLEAN,
            nullable=False,
            server_default=true(),
        ),
        Column(
            "default_disabled",
            BOOLEAN,
            nullable=False,
            server_default=false(),
        ),
    )

    metadata.create_all(engine)
    try:
        with engine.begin() as connection:
            connection.execute(
                table.insert(),
                [
                    {"id": 1, "enabled": True},
                    {"id": 2, "enabled": False},
                    {"id": 3, "enabled": None},
                ],
            )

        with engine.connect() as connection:
            rows = connection.execute(
                select(
                    table.c.id,
                    table.c.enabled,
                    table.c.default_enabled,
                    table.c.default_disabled,
                    (table.c.enabled == True).label("comparison"),  # noqa: E712
                ).order_by(table.c.id)
            ).all()

            assert rows == [
                (1, True, True, False, True),
                (2, False, True, False, False),
                (3, None, True, False, False),
            ]
            assert all(
                value is None or isinstance(value, bool)
                for row in rows
                for value in row[1:]
            )

            assert connection.execute(
                select(table.c.id).where(table.c.enabled).order_by(table.c.id)
            ).scalars().all() == [1]
            assert connection.execute(
                select(table.c.id).where(~table.c.enabled).order_by(table.c.id)
            ).scalars().all() == [2]
            assert connection.execute(
                select(table.c.id)
                .where(table.c.enabled.is_not(True))
                .order_by(table.c.id)
            ).scalars().all() == [2, 3]

        columns = {
            str(column["name"]): column
            for column in inspect(engine).get_columns(table_name)
        }

        assert type(columns["enabled"]["type"]) is BOOLEAN
        assert type(columns["default_enabled"]["type"]) is BOOLEAN
        assert type(columns["default_disabled"]["type"]) is BOOLEAN

        assert (
            columns["default_enabled"]["default"].casefold()
            == "true"
        )
        assert (
            columns["default_disabled"]["default"].casefold()
            == "false"
        )
    finally:
        metadata.drop_all(engine, checkfirst=True)
