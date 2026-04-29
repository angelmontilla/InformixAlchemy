from __future__ import annotations

import inspect as pyinspect

import pytest
import sqlalchemy
from sqlalchemy import exc
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy import column, literal_column, select, table
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope
from sqlalchemy.testing.provision import temp_table_keyword_args

from IfxAlchemy.IfxPy import IfxDialect_IfxPy
from IfxAlchemy.base import IfxDialect
from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.requirements import Requirements


@pytest.mark.sqlalchemy_suite
def test_statement_cache_is_enabled_for_dialects():
    assert IfxDialect().supports_statement_cache is True
    assert IfxDialect_pyodbc().supports_statement_cache is True
    assert IfxDialect_IfxPy().supports_statement_cache is True


@pytest.mark.sqlalchemy_suite
def test_supported_dialect_contract():
    dialect = IfxDialect_pyodbc()

    assert dialect.name == "informix"
    assert dialect.driver == "pyodbc" or getattr(dialect, "driver", None) in (
        None,
        "pyodbc",
    )
    assert dialect.supports_schemas is False


@pytest.mark.legacy_ifxpy
def test_legacy_ifxpy_keeps_statement_cache_contract():
    dialect = IfxDialect_IfxPy()

    assert dialect.supports_statement_cache is True


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
        "get_sequence_names",
        "get_table_comment",
        "get_table_options",
        "get_multi_check_constraints",
        "get_multi_table_comment",
        "get_multi_table_options",
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
@pytest.mark.parametrize(
    "requirement_name",
    [
        "check_constraint_reflection",
        "inline_check_constraint_reflection",
        "materialized_views",
    ],
)
def test_unimplemented_reflection_requirements_are_closed(requirement_name):
    requirements = Requirements()

    assert getattr(requirements, requirement_name).enabled is False


@pytest.mark.sqlalchemy_suite
def test_select_private_api_contract_used_by_informix_compat_layer():
    stmt = (
        select(literal_column("1"))
        .order_by(literal_column("1"))
        .limit(5)
        .offset(2)
    )

    from IfxAlchemy import sqla_compat

    state = sqla_compat.get_limit_state(stmt)

    assert state.limit_clause is not None
    assert state.offset_clause is not None
    assert state.offset_value == 2
    assert tuple(state.order_by_clauses)

    fetch_stmt = select(literal_column("1")).fetch(5)
    fetch_state = sqla_compat.get_limit_state(fetch_stmt)

    assert fetch_state.fetch_clause is not None
    assert fetch_state.fetch_options["percent"] is False
    assert fetch_state.fetch_options["with_ties"] is False


@pytest.mark.sqlalchemy_suite
def test_private_sqlalchemy_helpers_are_centralized_contract():
    from IfxAlchemy import sqla_compat

    metadata = MetaData()
    tbl = Table("t", metadata, Column("id", Integer, primary_key=True))
    for_update_stmt = select(tbl.c.id).with_for_update()
    insert_stmt = tbl.insert().returning(tbl.c.id)
    compiled = insert_stmt.compile(dialect=IfxDialect_pyodbc())
    preparer = IfxDialect_pyodbc().identifier_preparer

    assert sqla_compat.get_select_for_update_arg(for_update_stmt) is not None
    assert sqla_compat.get_table_autoincrement_column(tbl) is tbl.c.id
    assert tuple(sqla_compat.get_table_sorted_constraints(tbl))
    assert sqla_compat.identifier_requires_quotes(preparer, "select") is True
    assert sqla_compat.get_dml_compile_state(compiled) is not None
    assert tuple(sqla_compat.get_statement_returning(insert_stmt))


@pytest.mark.sqlalchemy_suite
@pytest.mark.parametrize(
    ("helper_name", "args", "missing_name"),
    [
        ("clone_select", (), "_generate"),
        ("simple_int_clause", (literal_column("1"),), "_simple_int_clause"),
        (
            "offset_or_limit_clause_asint",
            (literal_column("1"), "limit"),
            "_offset_or_limit_clause_asint",
        ),
        ("get_order_by_clauses", (), "_order_by_clauses"),
    ],
)
def test_select_private_api_contract_fails_explicitly(
    helper_name, args, missing_name
):
    from IfxAlchemy import sqla_compat

    helper = getattr(sqla_compat, helper_name)

    with pytest.raises(exc.CompileError, match=missing_name):
        helper(object(), *args)


@pytest.mark.sqlalchemy_suite
def test_multi_reflection_signatures_expose_kind_and_scope():
    sig = pyinspect.signature(IfxDialect.get_multi_columns)

    assert sig.parameters["kind"].default is ObjectKind.TABLE
    assert sig.parameters["scope"].default is ObjectScope.DEFAULT


@pytest.mark.sqlalchemy_suite
@pytest.mark.parametrize(
    "method_name",
    [
        "get_multi_columns",
        "get_multi_pk_constraint",
        "get_multi_foreign_keys",
        "get_multi_indexes",
        "get_multi_unique_constraints",
        "get_multi_check_constraints",
        "get_multi_table_comment",
        "get_multi_table_options",
    ],
)
def test_multi_reflection_methods_exist(method_name):
    assert callable(getattr(IfxDialect, method_name))


@pytest.mark.sqlalchemy_suite
@pytest.mark.parametrize(
    "method_name",
    [
        "get_multi_columns",
        "get_multi_pk_constraint",
        "get_multi_foreign_keys",
        "get_multi_indexes",
        "get_multi_unique_constraints",
        "get_multi_check_constraints",
        "get_multi_table_comment",
        "get_multi_table_options",
    ],
)
def test_multi_reflection_method_signature_contract(method_name):
    sig = pyinspect.signature(getattr(IfxDialect, method_name))

    assert "schema" in sig.parameters
    assert "filter_names" in sig.parameters
    assert "kind" in sig.parameters
    assert "scope" in sig.parameters
    assert sig.parameters["kind"].default is ObjectKind.TABLE
    assert sig.parameters["scope"].default is ObjectScope.DEFAULT
    assert any(
        param.kind is pyinspect.Parameter.VAR_KEYWORD
        for param in sig.parameters.values()
    )


@pytest.mark.sqlalchemy_suite
def test_dml_capability_flags_are_explicit():
    dialect = IfxDialect_pyodbc()

    assert dialect.insert_returning is False
    assert dialect.update_returning is False
    assert dialect.delete_returning is False
    assert dialect.use_insertmanyvalues is False
    assert dialect.supports_identity_columns is False


@pytest.mark.sqlalchemy_suite
def test_representative_select_has_cache_key():
    tbl = table("t", column("id"), column("name"))

    stmt = (
        select(tbl.c.id, tbl.c.name)
        .where(tbl.c.id == 5)
        .order_by(tbl.c.id)
        .limit(10)
        .offset(2)
    )

    cache_key = stmt._generate_cache_key()

    assert cache_key is not None


@pytest.mark.sqlalchemy_suite
def test_limit_offset_compilation_is_statement_cache_safe():
    tbl = table("t", column("id"), column("name"))
    dialect = IfxDialect_pyodbc()

    first_stmt = (
        select(tbl.c.id, tbl.c.name)
        .order_by(tbl.c.id)
        .limit(5)
        .offset(2)
    )
    second_stmt = (
        select(tbl.c.id, tbl.c.name)
        .order_by(tbl.c.id)
        .limit(10)
        .offset(4)
    )

    first_compiled = first_stmt.compile(dialect=dialect)
    second_compiled = second_stmt.compile(dialect=dialect)

    assert first_stmt._generate_cache_key().key == (
        second_stmt._generate_cache_key().key
    )
    assert str(first_compiled) == str(second_compiled)
    assert "__[POSTCOMPILE_" in str(first_compiled)
    assert first_compiled.params != second_compiled.params


@pytest.mark.sqlalchemy_suite
@pytest.mark.parametrize(
    "requirement_name",
    [
        "temp_table_names",
        "temp_table_reflection",
        "temporary_views",
        "schemas",
        "materialized_views",
        "check_constraint_reflection",
        "inline_check_constraint_reflection",
        "on_update_cascade",
        "datetime_microseconds",
        "time_microseconds",
        "unbounded_varchar",
    ],
)
def test_current_closed_requirements_are_part_of_contract(requirement_name):
    requirements = Requirements()

    assert getattr(requirements, requirement_name).enabled is False


@pytest.mark.sqlalchemy_suite
@pytest.mark.parametrize(
    "requirement_name",
    [
        "has_temp_table",
        "window_functions",
        "precision_numerics_enotation_small",
        "precision_numerics_retains_significant_digits",
    ],
)
def test_current_open_requirements_are_part_of_contract(requirement_name):
    requirements = Requirements()

    assert getattr(requirements, requirement_name).enabled is True


@pytest.mark.sqlalchemy_suite
def test_multi_check_constraints_requirement_contract():
    assert Requirements().check_constraint_reflection.enabled is False


@pytest.mark.sqlalchemy_suite
def test_check_constraint_reflection_returns_stable_empty_structure():
    dialect = IfxDialect()

    assert dialect.get_check_constraints(object(), "tabla") == []


@pytest.mark.sqlalchemy_suite
def test_sqlalchemy_version_contract_for_current_validation_lane():
    version = sqlalchemy.__version__

    major, minor, patch = version.split(".", 2)

    if (major, minor) == ("2", "1"):
        return

    assert (major, minor) == ("2", "0")
    assert int(patch) >= 45

def test_legacy_full_returning_flag_is_not_declared():
    assert "full_returning" not in IfxDialect.__dict__
    assert "full_returning" not in IfxDialect_pyodbc.__dict__
    assert "full_returning" not in IfxDialect_IfxPy.__dict__
