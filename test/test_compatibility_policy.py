from __future__ import annotations

import json
from pathlib import Path

from packaging.requirements import Requirement
from packaging.version import Version
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def _sqlalchemy_requirement() -> Requirement:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = project["project"]["dependencies"]
    raw_requirement = next(
        item for item in dependencies if Requirement(item).name.lower() == "sqlalchemy"
    )
    return Requirement(raw_requirement)


def test_pyproject_supports_sqlalchemy_20_and_excludes_21():
    specifier = _sqlalchemy_requirement().specifier

    assert Version("2.0.45") in specifier
    assert Version("2.0.99") in specifier
    assert Version("2.1.0") not in specifier


def test_constraint_files_exist_and_match_supported_profiles():
    constraints = ROOT / "constraints"
    minimum = (constraints / "sqlalchemy-min.txt").read_text().strip()
    stable = (constraints / "sqlalchemy-stable.txt").read_text().strip()
    preview = (constraints / "sqlalchemy-next.txt").read_text().strip()

    assert minimum == "SQLAlchemy==2.0.45"
    assert stable == "SQLAlchemy>=2.0.45,<2.1"
    assert preview == "SQLAlchemy>=2.1,<2.2"


def test_compatibility_matrix_contains_minimum_current_and_experimental_preview():
    workflow = (ROOT / ".github/workflows/compatibility.yml").read_text()
    assert "minimum" in workflow
    assert "current" in workflow
    assert "next-preview" in workflow
    assert "continue-on-error: ${{ matrix.sqlalchemy == 'next-preview' }}" in workflow
    assert "python -m tools.build_clean_archive" in workflow
    assert "python -m tools.check_secrets --release" in workflow
    for version in ("3.10", "3.11", "3.12", "3.13", "3.14"):
        assert f"'{version}'" in workflow


def test_primary_ci_marks_sqlalchemy_21_as_experimental():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    profile_blocks = workflow.split('sqlalchemy: "SQLAlchemy>=2.1.0b2,<2.2"')[1:]

    assert profile_blocks
    assert all("experimental: true" in block.split("label:", 1)[0] for block in profile_blocks)


def test_certification_matrix_covers_all_required_p0_axes():
    matrix = json.loads((ROOT / "certification-matrix.json").read_text())
    axes = matrix["required_axes"]

    assert set(axes) == {
        "server",
        "database",
        "odbc",
        "python",
        "sqlalchemy",
        "identifiers",
        "locale",
        "json_bson",
    }
    assert axes["server"] == ["14.10-latest-fixpack", "15.x-current"]
    assert axes["database"] == ["non-ansi", "ansi"]
