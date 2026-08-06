from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess
import sys

import alembic
import pytest
from packaging.version import Version
from sqlalchemy import testing
from sqlalchemy.testing import config as sqla_config

import run_alembic_tests
from run_alembic_tests import _AlembicRequirementsPlugin
from test.alembic_requirements import (
    Requirements,
)


pytestmark = pytest.mark.alembic_suite


def test_supported_alembic_version() -> None:
    version = Version(alembic.__version__)

    assert Version("1.18.0") <= version < Version("1.19.0")


def test_alembic_suite_is_installed() -> None:
    specification = importlib.util.find_spec(
        "alembic.testing.suite"
    )

    assert specification is not None
    assert specification.origin is not None


def test_combined_requirements_contract() -> None:
    requirements = Requirements()

    assert hasattr(
        requirements,
        "comments",
    )
    assert hasattr(
        requirements,
        "alter_column",
    )
    assert hasattr(
        requirements,
        "computed_columns",
    )
    assert hasattr(
        requirements,
        "identity_columns",
    )
    assert (
        requirements.fk_onupdate.enabled
        is False
    )


def test_runner_plugin_restores_alembic_requirements_after_sqla_setup(
    monkeypatch,
) -> None:
    sqlalchemy_requirements = object()

    monkeypatch.setattr(
        sqla_config,
        "requirements",
        sqlalchemy_requirements,
        raising=False,
    )
    monkeypatch.setattr(
        testing,
        "requires",
        sqlalchemy_requirements,
        raising=False,
    )

    _AlembicRequirementsPlugin().pytest_sessionstart(None)

    assert isinstance(
        sqla_config.requirements,
        Requirements,
    )
    assert testing.requires is sqla_config.requirements
    assert hasattr(
        sqla_config.requirements,
        "comments",
    )


def test_runner_does_not_rely_on_overwritten_requirements_option(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        run_alembic_tests,
        "_assert_required_files_exist",
        lambda: None,
    )
    monkeypatch.setattr(
        run_alembic_tests,
        "load_official_suite_environment",
        lambda: "test.env",
    )
    monkeypatch.setattr(
        run_alembic_tests,
        "official_suite_dburi",
        lambda: "informix+pyodbc://u:p@localhost/test",
    )
    monkeypatch.setattr(
        run_alembic_tests,
        "ensure_required_test_databases",
        lambda: {
            "non_ansi": {
                "database": "ifxalchemy_test",
                "created": False,
            },
            "ansi": {
                "database": "ifxalchemy_test_ansi",
                "created": False,
            },
        },
    )
    verification = {}

    def _verify_official_suite_database(
        dburi,
        *,
        require_empty=None,
    ):
        verification["dburi"] = dburi
        verification["require_empty"] = require_empty
        return {
            "safe_url": dburi,
            "has_objects": False,
            "dirty_inventory": {},
        }

    monkeypatch.setattr(
        run_alembic_tests,
        "verify_official_suite_database",
        _verify_official_suite_database,
    )
    monkeypatch.setattr(
        run_alembic_tests,
        "resolve_junit_file",
        lambda value, default: default,
    )

    invocation = {}

    def _pytest_main(args, *, plugins):
        invocation["args"] = list(args)
        invocation["plugins"] = list(plugins)
        return 0

    monkeypatch.setattr(
        run_alembic_tests.pytest,
        "main",
        _pytest_main,
    )

    assert run_alembic_tests.main([]) == 0
    assert verification == {
        "dburi": "informix+pyodbc://u:p@localhost/test",
        "require_empty": False,
    }
    assert "--dropfirst" in invocation["args"]
    assert "--requirements" not in invocation["args"]
    assert len(invocation["plugins"]) == 1
    assert isinstance(
        invocation["plugins"][0],
        _AlembicRequirementsPlugin,
    )


def test_alembic_suite_collects_with_runner_requirement_override() -> None:
    script = """
import pytest
from run_alembic_tests import _AlembicRequirementsPlugin

raise SystemExit(
    pytest.main(
        [
            "-c",
            "pytest.ini",
            "-p",
            "sqlalchemy.testing.plugin.pytestplugin",
            "--collect-only",
            "-q",
            "test/test_suite_alembic.py",
            "--dburi",
            "sqlite:///:memory:",
        ],
        plugins=[_AlembicRequirementsPlugin()],
    )
)
"""
    environment = os.environ.copy()
    environment.pop("PYTEST_ADDOPTS", None)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, output
    assert "AttributeError" not in output
    assert "tests collected" in output


def test_alembic_suite_stub_exists() -> None:
    assert Path(
        "test/test_suite_alembic.py"
    ).is_file()


def test_alembic_runner_exists() -> None:
    assert Path(
        "run_alembic_tests.py"
    ).is_file()


def test_alembic_requirements_exist() -> None:
    assert Path(
        "test/alembic_requirements.py"
    ).is_file()
