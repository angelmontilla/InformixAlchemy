from __future__ import annotations

import inspect as pyinspect

import pytest
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
