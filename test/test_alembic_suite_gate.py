from __future__ import annotations

import importlib.util
from pathlib import Path

import alembic
import pytest

from test.alembic_requirements import (
    Requirements,
)


pytestmark = pytest.mark.alembic_suite


def test_supported_alembic_version() -> None:
    assert alembic.__version__ == "1.18.5"


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
