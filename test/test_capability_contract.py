from __future__ import annotations

from sqlalchemy import Column, Integer, MetaData, Table, select

from IfxAlchemy.capabilities import CAPABILITY_MATRIX
from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.requirements import Requirements


_OPEN_REQUIREMENTS = {
    "savepoints",
    "autocommit",
    "foreign_key_constraint_name_reflection",
    "emulated_lastrowid",
    "nullsordering",
}

_CLOSED_REQUIREMENTS = {
    "datetime_interval",
    "datetime_literals",
    "json_type",
    "regexp_match",
    "regexp_replace",
    "tuple_in",
    "fetch_percent",
    "fetch_ties",
}


def test_p0_suite_requirements_are_explicit_and_aligned():
    requirements = Requirements()
    for name in _OPEN_REQUIREMENTS:
        assert getattr(requirements, name).enabled is True, name
        assert CAPABILITY_MATRIX[name].suite == "open"

    for name in _CLOSED_REQUIREMENTS:
        assert getattr(requirements, name).enabled is False, name
        assert CAPABILITY_MATRIX[name].suite == "closed"


def test_capability_matrix_distinguishes_native_interval_from_generic_interval():
    assert CAPABILITY_MATRIX["native_interval"].dialect == "native INTERVAL type"
    assert CAPABILITY_MATRIX["datetime_interval"].suite == "closed"


def test_null_ordering_compiles_to_native_syntax():
    table = Table("ordering", MetaData(), Column("value", Integer))
    dialect = IfxDialect_pyodbc()

    first = str(select(table.c.value).order_by(table.c.value.nulls_first()).compile(
        dialect=dialect
    ))
    last = str(select(table.c.value).order_by(table.c.value.nulls_last()).compile(
        dialect=dialect
    ))

    assert "NULLS FIRST" in first
    assert "NULLS LAST" in last
