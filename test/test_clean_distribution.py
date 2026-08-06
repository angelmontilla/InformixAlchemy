from __future__ import annotations

import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

import tools.check_secrets as secret_scanner

from tools.build_clean_archive import (
    build_archive,
    default_destination,
    read_project_version,
    validate_required_paths,
)
from tools.check_secrets import scan_release_tree, scan_secrets


ROOT = Path(__file__).resolve().parents[1]


def test_secret_scanner_rejects_real_environment_files(tmp_path):
    (tmp_path / ".env.informix").write_text("INFORMIX_PASSWORD=real-value\n")

    issues = scan_secrets(tmp_path)

    assert any("forbidden environment file" in issue for issue in issues)


def test_example_environment_requires_placeholders(tmp_path):
    example = tmp_path / ".env.informix.example"
    example.write_text("INFORMIX_PASSWORD=not-a-placeholder\n")

    issues = scan_secrets(tmp_path)

    assert any("non-placeholder sensitive value" in issue for issue in issues)


def test_working_tree_allows_gitignored_local_environment_files(tmp_path):
    (tmp_path / ".env.informix").write_text(
        "INFORMIX_PASSWORD=developer-secret\n",
        encoding="utf-8",
    )

    assert scan_secrets(
        tmp_path,
        allow_local_environment_files=True,
    ) == []


def test_project_source_contains_no_nonlocal_secret_material():
    assert scan_secrets(
        ROOT,
        allow_local_environment_files=True,
    ) == []


def test_clean_archive_is_allowlisted_and_reproducible(tmp_path):
    first = build_archive(tmp_path / "first.zip")
    second = build_archive(tmp_path / "second.zip")

    assert first.read_bytes() == second.read_bytes()
    with zipfile.ZipFile(first) as archive:
        names = set(archive.namelist())
        assert "InformixAlchemy/.env.informix" not in names
        assert "InformixAlchemy/.env.official-suites" not in names
        assert "InformixAlchemy/.env.informix.example" in names
        assert "InformixAlchemy/.env.official-suites.example" in names
        assert "InformixAlchemy/constraints/sqlalchemy-min.txt" in names
        assert not any(".egg-info/" in name for name in names)
        assert not any(".pytest_cache/" in name for name in names)
        assert not any(name.endswith(".pyc") for name in names)
        assert not any("/artifacts/" in name for name in names)


def test_archive_builder_cli_supports_script_and_module_execution(tmp_path):
    direct = tmp_path / "direct.zip"
    module = tmp_path / "module.zip"

    subprocess.run(
        [sys.executable, "tools/build_clean_archive.py", str(direct)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [sys.executable, "-m", "tools.build_clean_archive", str(module)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert direct.read_bytes() == module.read_bytes()


def test_required_release_paths_are_validated(tmp_path):
    with pytest.raises(FileNotFoundError, match="constraints/sqlalchemy-min.txt"):
        validate_required_paths(tmp_path)


def test_default_archive_name_comes_from_pyproject_version():
    version = read_project_version(ROOT)

    assert default_destination(ROOT).name == f"InformixAlchemy-{version}.zip"


def test_secret_scanner_reads_zip_and_wheel_members(tmp_path):
    archive_path = tmp_path / "unsafe.whl"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("package/.env.informix", "INFORMIX_PASSWORD=real-value\n")

    issues = scan_release_tree(archive_path)

    assert any("forbidden environment file" in issue for issue in issues)
    assert any("unsafe.whl!package/.env.informix" in issue for issue in issues)


def test_secret_scanner_reads_tar_members(tmp_path):
    source = tmp_path / "token.txt"
    token = "ghp_" + ("1" * 36)
    source.write_text(f"token={token}\n")
    archive_path = tmp_path / "unsafe.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source, arcname="package/token.txt")

    issues = scan_release_tree(archive_path)

    assert any("credential-like token" in issue for issue in issues)


def test_secret_scanner_rejects_unsafe_archive_member_path(tmp_path):
    archive_path = tmp_path / "traversal.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../.env", "PASSWORD=secret\n")

    issues = scan_release_tree(archive_path)

    assert any("unsafe archive member path" in issue for issue in issues)


def test_release_tree_scanner_rejects_generated_artifacts(tmp_path):
    generated = tmp_path / "IfxAlchemy.egg-info" / "PKG-INFO"
    generated.parent.mkdir()
    generated.write_text("generated")

    issues = scan_release_tree(tmp_path)

    assert any("generated package metadata" in issue for issue in issues)

def test_secret_scanner_rejects_oversized_archive_member(tmp_path, monkeypatch):
    monkeypatch.setattr(secret_scanner, "MAX_MEMBER_SIZE", 4)
    archive_path = tmp_path / "oversized.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("package/data.txt", "12345")

    issues = scan_release_tree(archive_path)

    assert any("archive member too large" in issue for issue in issues)

