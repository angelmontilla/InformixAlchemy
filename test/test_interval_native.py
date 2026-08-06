from __future__ import annotations

import datetime

import pytest
from alembic.migration import MigrationContext
from sqlalchemy import Column, MetaData, Table, inspect
from sqlalchemy.schema import CreateTable

from IfxAlchemy import INTERVAL, YearMonthInterval
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def _collength(storage_length: int, first: int, last: int) -> int:
    return (storage_length * 256) + (first * 16) + last


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


@pytest.mark.parametrize(
    ("type_", "expected"),
    [
        (INTERVAL("YEAR", "MONTH"), "INTERVAL YEAR TO MONTH"),
        (INTERVAL("DAY", "SECOND"), "INTERVAL DAY TO SECOND"),
        (
            INTERVAL(
                "DAY",
                "FRACTION",
                leading_precision=5,
                fractional_precision=3,
            ),
            "INTERVAL DAY(5) TO FRACTION(3)",
        ),
        (INTERVAL("HOUR", "SECOND"), "INTERVAL HOUR TO SECOND"),
    ],
)
def test_interval_ddl_compilation_preserves_qualifiers(dialect, type_, expected):
    table = Table("interval_ddl", MetaData(), Column("duration", type_))

    compiled = str(CreateTable(table).compile(dialect=dialect))

    assert expected in compiled
    assert "DATETIME" not in compiled


def test_interval_year_month_bind_result_and_literal(dialect):
    type_ = INTERVAL("YEAR", "MONTH")
    value = YearMonthInterval.from_years_months(3, 6)

    assert type_.bind_processor(dialect)(value) == "3-06"
    assert type_.bind_processor(dialect)(42) == "3-06"
    assert type_.result_processor(dialect, None)("-3-06") == YearMonthInterval(
        -42
    )
    assert type_.literal_processor(dialect)(value) == (
        "INTERVAL (3-06) YEAR TO MONTH"
    )


def test_interval_day_time_bind_result_and_literal(dialect):
    type_ = INTERVAL(
        "DAY",
        "FRACTION",
        leading_precision=5,
        fractional_precision=3,
    )
    value = datetime.timedelta(
        days=12,
        hours=3,
        minutes=4,
        seconds=5,
        microseconds=678_999,
    )

    assert type_.bind_processor(dialect)(value) == "12 03:04:05.678"
    assert type_.result_processor(dialect, None)(
        b"12 03:04:05.678"
    ) == datetime.timedelta(
        days=12,
        hours=3,
        minutes=4,
        seconds=5,
        microseconds=678_000,
    )
    assert type_.literal_processor(dialect)(value) == (
        "INTERVAL (12 03:04:05.678) DAY(5) TO FRACTION(3)"
    )


def test_interval_negative_day_time_is_canonical(dialect):
    type_ = INTERVAL("HOUR", "SECOND", leading_precision=3)
    value = -datetime.timedelta(hours=27, minutes=2, seconds=3)

    assert type_.bind_processor(dialect)(value) == "-27:02:03"
    assert type_.result_processor(dialect, None)("-27:02:03") == value


def test_interval_validation_rejects_mixed_classes_and_bad_subfields():
    with pytest.raises(ValueError, match="cannot mix"):
        INTERVAL("YEAR", "DAY")

    with pytest.raises(ValueError, match="fractional_precision"):
        INTERVAL("DAY", "SECOND", fractional_precision=3)

    with pytest.raises(ValueError, match="hour"):
        INTERVAL("DAY", "SECOND").normalize_python_value("2 24:00:00")

    with pytest.raises(OverflowError, match="leading field"):
        INTERVAL("DAY", "SECOND").format_bind_value(
            datetime.timedelta(days=100)
        )


def test_interval_odbc_metadata_enrichment_is_best_effort(dialect):
    class Row:
        column_name = "duration"
        type_name = "INTERVAL DAY TO FRACTION"
        column_size = 18
        decimal_digits = 3
        sql_data_type = 10
        sql_datetime_sub = 10

    class Cursor:
        closed = False

        def columns(self, **kwargs):
            assert kwargs == {"table": "events", "schema": "owner"}
            return [Row()]

        def close(self):
            self.closed = True

    cursor = Cursor()
    dbapi = type("DBAPIConnection", (), {"cursor": lambda self: cursor})()
    proxy = type("Proxy", (), {"driver_connection": dbapi})()
    connection = type("Connection", (), {"connection": proxy})()

    metadata = dialect._reflector._odbc_column_metadata(
        connection,
        "events",
        "owner",
    )

    assert metadata["duration"]["column_size"] == 18
    assert metadata["duration"]["decimal_digits"] == 3
    assert cursor.closed is True


def test_interval_reflection_uses_native_type_and_exact_odbc_precision(dialect):
    type_, autoincrement, nullable = dialect._reflector._decode_ifx_type(
        coltype=14,
        collength=_collength(8, 4, 13),
        interval_metadata={"column_size": 18, "decimal_digits": 3},
    )

    assert isinstance(type_, INTERVAL)
    assert (type_.start_field, type_.end_field) == ("DAY", "FRACTION")
    assert type_.leading_precision == 5
    assert type_.fractional_precision == 3
    assert type_._informix_precision_exact is True
    assert type_._informix_precision_source == "odbc-sqlcolumns"
    assert dialect.type_compiler.process(type_) == (
        "INTERVAL DAY(5) TO FRACTION(3)"
    )
    assert autoincrement is False
    assert nullable is True


@pytest.mark.parametrize(
    ("collength", "metadata", "expected"),
    [
        (
            _collength(4, 0, 2),
            {"column_size": 7, "decimal_digits": 0},
            "INTERVAL YEAR TO MONTH",
        ),
        (
            _collength(4, 6, 10),
            {"column_size": 8, "decimal_digits": 0},
            "INTERVAL HOUR TO SECOND",
        ),
    ],
)
def test_interval_reflection_round_trip_for_required_qualifiers(
    dialect,
    collength,
    metadata,
    expected,
):
    type_, _, _ = dialect._reflector._decode_ifx_type(
        coltype=14,
        collength=collength,
        interval_metadata=metadata,
    )

    assert isinstance(type_, INTERVAL)
    assert dialect.type_compiler.process(type_) == expected


def test_interval_reflection_catalog_fallback_never_becomes_datetime(dialect):
    type_, _, _ = dialect._reflector._decode_ifx_type(
        coltype=14,
        collength=_collength(8, 4, 13),
    )

    assert isinstance(type_, INTERVAL)
    assert type_.leading_precision == 5
    assert type_.fractional_precision == 3
    assert type_._informix_precision_source == "syscolumns-collength"
    assert type_._informix_precision_exact is False
    assert dialect.type_compiler.process(type_) == (
        "INTERVAL DAY(5) TO FRACTION(3)"
    )


def test_interval_uses_varchar_dbapi_transport():
    dbapi = type("DBAPI", (), {"SQL_VARCHAR": 12})
    assert INTERVAL("DAY", "SECOND").get_dbapi_type(dbapi) == 12


def test_interval_static_cache_key_contains_all_semantic_fields():
    type_ = INTERVAL(
        "DAY",
        "FRACTION",
        leading_precision=5,
        fractional_precision=3,
    )

    cache_key = repr(type_._static_cache_key)
    assert "DAY" in cache_key
    assert "FRACTION" in cache_key
    assert "leading_precision" in cache_key
    assert "fractional_precision" in cache_key


def test_alembic_detects_interval_qualifier_precision_changes(dialect):
    context = MigrationContext.configure(dialect=dialect)
    reflected = Column(
        "duration",
        INTERVAL(
            "DAY",
            "FRACTION",
            leading_precision=5,
            fractional_precision=3,
        ),
    )
    same = Column(
        "duration",
        INTERVAL(
            "DAY",
            "FRACTION",
            leading_precision=5,
            fractional_precision=3,
        ),
    )
    changed = Column(
        "duration",
        INTERVAL(
            "DAY",
            "FRACTION",
            leading_precision=4,
            fractional_precision=3,
        ),
    )

    assert context.impl.compare_type(reflected, same) is False
    assert context.impl.compare_type(reflected, changed) is True


def test_alembic_renders_importable_interval_type(dialect):
    context = MigrationContext.configure(dialect=dialect)

    class AutogenContext:
        imports: set[str] = set()

    rendered = context.impl.render_type(
        INTERVAL("DAY", "FRACTION", 5, 3),
        AutogenContext,
    )

    assert rendered == (
        "INTERVAL('DAY', 'FRACTION', leading_precision=5, "
        "fractional_precision=3)"
    )
    assert "from IfxAlchemy import INTERVAL" in AutogenContext.imports


@pytest.mark.requires_informix

def test_interval_live_reflection_round_trip(conn, name_factory, qident):
    table_name = name_factory("sa_interval_")
    quoted = qident(table_name)
    conn.exec_driver_sql(
        f"""
        CREATE TABLE {quoted} (
            ym INTERVAL YEAR TO MONTH,
            ds INTERVAL DAY TO SECOND,
            df INTERVAL DAY(5) TO FRACTION(3),
            hs INTERVAL HOUR TO SECOND
        )
        """
    )
    conn.commit()
    try:
        columns = {
            column["name"]: column["type"]
            for column in inspect(conn).get_columns(table_name)
        }
        assert all(isinstance(type_, INTERVAL) for type_ in columns.values())
        assert conn.dialect.type_compiler.process(columns["ym"]) == (
            "INTERVAL YEAR TO MONTH"
        )
        assert conn.dialect.type_compiler.process(columns["ds"]) == (
            "INTERVAL DAY TO SECOND"
        )
        assert conn.dialect.type_compiler.process(columns["df"]) == (
            "INTERVAL DAY(5) TO FRACTION(3)"
        )
        assert conn.dialect.type_compiler.process(columns["hs"]) == (
            "INTERVAL HOUR TO SECOND"
        )
    finally:
        conn.exec_driver_sql(f"DROP TABLE {quoted}")
        conn.commit()
