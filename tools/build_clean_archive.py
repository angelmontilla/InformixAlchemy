"""Build a deterministic, allowlisted source archive for IfxAlchemy."""
from __future__ import annotations

import argparse
import sys
import tempfile
import zipfile
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

try:  # Support both ``python -m tools...`` and direct script execution.
    from .check_secrets import scan_release_tree
except ImportError:  # pragma: no cover - exercised by the subprocess CLI test
    from check_secrets import scan_release_tree


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_ROOT_NAME = "InformixAlchemy"
ROOT_FILES = {
    ".env.informix.example",
    ".env.official-suites.example",
    ".gitignore",
    "CHANGELOG.md",
    "LICENSE",
    "MANIFEST.in",
    "README.md",
    "README.rst",
    "certification-matrix.json",
    "pyproject.toml",
    "pytest.ini",
    "requirements-dev.txt",
    "run_alembic_tests.py",
    "run_sql_expression_certification.py",
    "run_tests.py",
    "setup.cfg",
    "tox.ini",
}
ROOT_DIRECTORIES = {
    ".github",
    "IfxAlchemy",
    "constraints",
    "test",
    "tools",
}
REQUIRED_PATHS = (
    "pyproject.toml",
    "README.md",
    "LICENSE",
    "IfxAlchemy",
    "tools/check_secrets.py",
    "constraints/sqlalchemy-min.txt",
    "constraints/sqlalchemy-stable.txt",
    "constraints/sqlalchemy-next.txt",
)
EXCLUDED_PARTS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    "artifacts",
    "build",
    "dist",
    "htmlcov",
}
EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def validate_required_paths(project_root: str | Path = PROJECT_ROOT) -> None:
    """Fail before building when a required release component is missing."""
    root = Path(project_root).resolve()
    missing = [relative for relative in REQUIRED_PATHS if not (root / relative).exists()]
    if missing:
        formatted = "\n".join(f"- {path}" for path in missing)
        raise FileNotFoundError(f"Required release files are missing:\n{formatted}")


def read_project_version(project_root: str | Path = PROJECT_ROOT) -> str:
    """Read the release version from the single source in ``pyproject.toml``."""
    root = Path(project_root).resolve()
    with (root / "pyproject.toml").open("rb") as file:
        return str(tomllib.load(file)["project"]["version"])


def default_destination(project_root: str | Path = PROJECT_ROOT) -> Path:
    root = Path(project_root).resolve()
    version = read_project_version(root)
    return root.parent / f"InformixAlchemy-{version}.zip"


def _included(path: Path, project_root: Path) -> bool:
    relative = path.relative_to(project_root)
    if any(
        part in EXCLUDED_PARTS or part.endswith(".egg-info")
        for part in relative.parts
    ):
        return False
    if path.suffix.lower() in EXCLUDED_SUFFIXES:
        return False
    if len(relative.parts) == 1:
        return relative.name in ROOT_FILES
    return relative.parts[0] in ROOT_DIRECTORIES


def release_files(project_root: str | Path = PROJECT_ROOT) -> tuple[Path, ...]:
    root = Path(project_root).resolve()
    validate_required_paths(root)
    return tuple(
        sorted(
            (
                path
                for path in root.rglob("*")
                if path.is_file() and _included(path, root)
            ),
            key=lambda item: item.relative_to(root).as_posix(),
        )
    )


def build_archive(
    destination: str | Path,
    *,
    project_root: str | Path = PROJECT_ROOT,
) -> Path:
    """Create a reproducible ZIP from an explicit source allowlist."""
    root = Path(project_root).resolve()
    destination = Path(destination).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    files = release_files(root)
    if not files:
        raise RuntimeError("Release allowlist selected no files")

    with tempfile.TemporaryDirectory(prefix="ifxalchemy-release-") as temp_dir:
        staging = Path(temp_dir) / ARCHIVE_ROOT_NAME
        for source in files:
            relative = source.relative_to(root)
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())

        issues = scan_release_tree(staging)
        if issues:
            raise RuntimeError("Unsafe release tree:\n" + "\n".join(issues))

        with zipfile.ZipFile(
            destination,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            for source in sorted(staging.rglob("*")):
                if not source.is_file():
                    continue
                archive_name = source.relative_to(staging.parent).as_posix()
                info = zipfile.ZipInfo(
                    archive_name,
                    date_time=(1980, 1, 1, 0, 0, 0),
                )
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                archive.writestr(info, source.read_bytes())
    return destination


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", nargs="?", default=None)
    args = parser.parse_args(argv)
    destination = args.destination or str(default_destination())
    print(build_archive(destination))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
