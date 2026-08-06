from __future__ import annotations

from pathlib import Path

import tomllib

import IfxAlchemy


ROOT = Path(__file__).resolve().parents[1]


def _project_metadata() -> dict:
    return tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]


def test_distribution_declares_sqlalchemy_2045_to_20x_range():
    requirements = _project_metadata().get("dependencies", [])

    assert any(
        requirement.startswith("SQLAlchemy")
        and ">=2.0.45" in requirement
        and "<2.1" in requirement
        for requirement in requirements
    )


def test_distribution_name_is_ifxalchemy():
    assert _project_metadata()["name"] == "IfxAlchemy"


def test_runtime_version_matches_pyproject():
    assert IfxAlchemy.__version__ == _project_metadata()["version"]


def test_source_tree_version_wins_over_stale_installed_metadata(monkeypatch):
    monkeypatch.setattr(
        IfxAlchemy,
        "distribution_version",
        lambda _distribution_name: "1.1.0",
    )

    assert IfxAlchemy._runtime_version() == _project_metadata()["version"]


def test_sdist_manifest_excludes_internal_workflows_and_tools():
    manifest = (ROOT / "MANIFEST.in").read_text(encoding="utf-8")

    assert "recursive-include .github" not in manifest
    assert "recursive-include tools" not in manifest
    assert "recursive-include test" not in manifest
    assert "prune test" in manifest

