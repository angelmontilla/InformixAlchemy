from __future__ import annotations

import inspect as pyinspect

import pytest
from sqlalchemy import literal_column, select
from sqlalchemy.testing.provision import temp_table_keyword_args

from IfxAlchemy.IfxPy import IfxDialect_IfxPy
from IfxAlchemy.base import IfxDialect
from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.requirements import Requirements


@pytest.mark.sqlalchemy_suite
def test_statement_cache_stays_disabled_until_suite_passes():
    for dialect_cls in (IfxDialect, IfxDialect_IfxPy, IfxDialect_pyodbc):
        dialect = dialect_cls()
        assert getattr(dialect, "supports_statement_cache", None) is False
        assert getattr(dialect, "supports_schemas", None) is False


@pytest.mark.sqlalchemy_suite
def test_dialect_exposes_do_ping():
    dialect = IfxDialect_pyodbc()
    assert callable(getattr(dialect, "do_ping", None))


@pytest.mark.sqlalchemy_suite
def test_has_table_signature_accepts_kwargs():
    sig = pyinspect.signature(IfxDialect_pyodbc.has_table)
    assert any(
        param.kind is pyinspect.Parameter.VAR_KEYWORD
        for param in sig.parameters.values()
    )


@pytest.mark.sqlalchemy_suite
def test_has_sequence_signature_accepts_kwargs():
    sig = pyinspect.signature(IfxDialect.has_sequence)
    assert any(
        param.kind is pyinspect.Parameter.VAR_KEYWORD
        for param in sig.parameters.values()
    )


@pytest.mark.sqlalchemy_suite
@pytest.mark.parametrize(
    "method_name",
    [
        "get_materialized_view_names",
        "get_check_constraints",
        "get_table_comment",
        "get_table_options",
    ],
)
def test_reflection_surface_methods_exist(method_name):
    method = getattr(IfxDialect, method_name, None)
    assert callable(method)


@pytest.mark.sqlalchemy_suite
def test_official_sqlalchemy_suite_module_is_available():
    __import__("sqlalchemy.testing.suite")


@pytest.mark.sqlalchemy_suite
def test_sqlalchemy_provision_exposes_temp_table_keyword_args(engine):
    IfxDialect_pyodbc.load_provisioning()

    assert temp_table_keyword_args(engine, engine) == {"prefixes": ["TEMP"]}


@pytest.mark.sqlalchemy_suite
def test_sqlalchemy_suite_temp_table_reflection_requirement_is_closed():
    requirements = Requirements()

    assert requirements.temp_table_reflection.enabled is False


@pytest.mark.sqlalchemy_suite
def test_sqlalchemy_suite_temp_table_name_listing_requirement_is_closed():
    requirements = Requirements()

    assert requirements.temp_table_names.enabled is False


@pytest.mark.sqlalchemy_suite
def test_select_private_api_contract_used_by_informix_compiler():
    stmt = (
        select(literal_column("1"))
        .order_by(literal_column("1"))
        .limit(5)
        .offset(2)
    )

    for attr_name in (
        "_limit_clause",
        "_offset_clause",
        "_order_by_clauses",
    ):
        assert hasattr(stmt, attr_name), attr_name

    assert hasattr(stmt, "_generate")
    assert callable(stmt._generate)

    assert hasattr(stmt, "_simple_int_clause")
    assert callable(stmt._simple_int_clause)

    assert hasattr(stmt, "_offset_or_limit_clause_asint")
    assert callable(stmt._offset_or_limit_clause_asint)

    fetch_stmt = select(literal_column("1")).fetch(5)

    assert hasattr(fetch_stmt, "_fetch_clause")
    assert hasattr(fetch_stmt, "_fetch_clause_options")


@pytest.mark.sqlalchemy_suite
@pytest.mark.parametrize(
    "requirement_name",
    [
        "temp_table_names",
        "temp_table_reflection",
        "temporary_views",
        "on_update_cascade",
        "datetime_microseconds",
        "time_microseconds",
        "unbounded_varchar",
    ],
)
def test_current_closed_requirements_are_part_of_contract(requirement_name):
    requirements = Requirements()

    assert getattr(requirements, requirement_name).enabled is False
