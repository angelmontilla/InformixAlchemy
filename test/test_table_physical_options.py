from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, inspect
from sqlalchemy.exc import CompileError
from sqlalchemy.schema import CreateTable

from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.reflection import IfxReflector


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _compile_table(dialect, **table_options) -> str:
    table = Table(
        "ifx_physical_options",
        MetaData(),
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        **table_options,
    )
    return " ".join(
        str(CreateTable(table).compile(dialect=dialect)).split()
    )


@pytest.mark.ddl_compiler
def test_create_table_emits_native_physical_options(dialect):
    compiled = _compile_table(
        dialect,
        informix_lock_level="row",
        informix_first_extent=64,
        informix_next_extent=32,
    )

    assert compiled.endswith(
        ") EXTENT SIZE 64 NEXT SIZE 32 LOCK MODE ROW"
    )


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("option_name", "option_value", "expected_clause"),
    [
        ("informix_lock_level", "PAGE", "LOCK MODE PAGE"),
        ("informix_first_extent", 64, "EXTENT SIZE 64"),
        ("informix_next_extent", 32, "NEXT SIZE 32"),
    ],
)
def test_create_table_physical_options_can_be_used_independently(
    dialect,
    option_name,
    option_value,
    expected_clause,
):
    compiled = _compile_table(
        dialect,
        **{option_name: option_value},
    )

    assert compiled.endswith(f") {expected_clause}")


@pytest.mark.ddl_compiler
@pytest.mark.parametrize("lock_level", ["ROW", "row", " Row ", "PAGE"])
def test_lock_level_normalizes_supported_values(dialect, lock_level):
    compiled = _compile_table(
        dialect,
        informix_lock_level=lock_level,
    )

    assert compiled.endswith(
        f") LOCK MODE {lock_level.strip().upper()}"
    )


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    "lock_level",
    ["TABLE", "PAGE_AND_ROW", "", 1, object()],
)
def test_invalid_lock_level_is_rejected(dialect, lock_level):
    with pytest.raises(
        CompileError,
        match="informix_lock_level must be either 'PAGE' or 'ROW'",
    ):
        _compile_table(
            dialect,
            informix_lock_level=lock_level,
        )


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("option_name", "invalid_value", "message"),
    [
        ("informix_first_extent", 0, "must be greater than zero"),
        ("informix_first_extent", -1, "must be greater than zero"),
        (
            "informix_first_extent",
            True,
            "must be a positive integer",
        ),
        (
            "informix_next_extent",
            1.5,
            "must be a positive integer",
        ),
        (
            "informix_next_extent",
            "32",
            "must be a positive integer",
        ),
    ],
)
def test_invalid_extent_values_are_rejected(
    dialect,
    option_name,
    invalid_value,
    message,
):
    with pytest.raises(CompileError, match=message):
        _compile_table(
            dialect,
            **{option_name: invalid_value},
        )


@pytest.mark.ddl_compiler
def test_user_authored_page_size_is_rejected(dialect):
    with pytest.raises(
        CompileError,
        match="informix_page_size is reflection-only",
    ):
        _compile_table(
            dialect,
            informix_page_size=4096,
        )


@pytest.mark.ddl_compiler
def test_reflected_page_size_is_preserved_but_not_emitted(dialect):
    reflector = IfxReflector(dialect)
    reflected_options = reflector._table_options_from_catalog_row(
        ("T", "R", 64, 32, 4096)
    )

    assert reflected_options["informix_page_size"] == 4096
    assert isinstance(reflected_options["informix_page_size"], int)

    compiled = _compile_table(dialect, **reflected_options)

    assert compiled.endswith(
        ") EXTENT SIZE 64 NEXT SIZE 32 LOCK MODE ROW"
    )
    assert "PAGE SIZE" not in compiled
    assert "PAGESIZE" not in compiled


@pytest.mark.ddl_compiler
def test_reflected_page_and_row_lock_state_is_not_re_emitted(dialect):
    reflector = IfxReflector(dialect)
    reflected_options = reflector._table_options_from_catalog_row(
        ("T", "B", 64, 32, 4096)
    )

    assert reflected_options["informix_lock_level"] == "PAGE_AND_ROW"

    compiled = _compile_table(dialect, **reflected_options)

    assert compiled.endswith(") EXTENT SIZE 64 NEXT SIZE 32")
    assert "LOCK MODE" not in compiled


@pytest.mark.ddl_execute
def test_physical_table_options_create_reflect_round_trip(
    engine,
    name_factory,
):
    table_name = name_factory("sa_phys_")
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        informix_lock_level="ROW",
        # 96 KB is divisible by every dbspace page size supported by the
        # Informix 14.10 storage engine and exceeds the four-page minimum.
        informix_first_extent=96,
        informix_next_extent=96,
    )

    try:
        with engine.begin() as connection:
            table.create(connection)

            reflected = inspect(connection).get_table_options(table_name)

            assert reflected["informix_lock_level"] == "ROW"
            assert reflected["informix_first_extent"] == 96
            assert reflected["informix_next_extent"] == 96
            assert reflected["informix_page_size"] > 0
            assert isinstance(reflected["informix_page_size"], int)
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)
