"""Verify that built release artifacts contain only expected project files."""
from __future__ import annotations

import argparse
import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath


FORBIDDEN_PARTS = {
    ".git",
    ".github",
    ".pytest_cache",
    "__pycache__",
    "artifacts",
    "build",
    "dist",
    "htmlcov",
    "test",
    "tests",
    "tools",
}
FORBIDDEN_NAMES = {
    ".coverage",
    ".env",
    ".env.informix",
    ".env.official-suites",
    "coverage.xml",
}


def _member_names(path: Path) -> list[PurePosixPath]:
    lowered = path.name.lower()
    if lowered.endswith((".whl", ".zip")):
        with zipfile.ZipFile(path) as archive:
            return [
                PurePosixPath(name)
                for name in archive.namelist()
                if name and not name.endswith("/")
            ]
    if lowered.endswith((".tar.gz", ".tgz")):
        with tarfile.open(path, mode="r:*") as archive:
            return [PurePosixPath(member.name) for member in archive if member.isfile()]
    raise ValueError(f"Unsupported release artifact: {path}")


def _logical_parts(member: PurePosixPath) -> tuple[str, ...]:
    parts = member.parts
    if parts and (
        parts[0].lower().startswith("ifxalchemy-")
        or parts[0] == "InformixAlchemy"
    ):
        return parts[1:]
    return parts


def verify_artifact(path: str | Path) -> list[str]:
    artifact = Path(path)
    issues: list[str] = []
    members = _member_names(artifact)
    if not members:
        return [f"empty release artifact: {artifact.name}"]

    is_wheel = artifact.name.lower().endswith(".whl")
    for member in members:
        parts = member.parts if is_wheel else _logical_parts(member)
        if not parts:
            continue
        name = parts[-1]
        lowered_name = name.casefold()
        lowered_parts = {part.casefold() for part in parts}
        if lowered_name in FORBIDDEN_NAMES or lowered_name.startswith(".env"):
            issues.append(f"forbidden file in {artifact.name}: {member.as_posix()}")
        if lowered_parts & FORBIDDEN_PARTS:
            issues.append(f"forbidden path in {artifact.name}: {member.as_posix()}")
        if any(part.casefold().endswith(".egg-info") for part in parts) and is_wheel:
            issues.append(f"egg-info in wheel {artifact.name}: {member.as_posix()}")
        if is_wheel and not (
            parts[0] == "IfxAlchemy"
            or parts[0].casefold().endswith(".dist-info")
        ):
            issues.append(f"unexpected wheel path in {artifact.name}: {member.as_posix()}")

    return sorted(set(issues))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+")
    args = parser.parse_args(argv)

    issues: list[str] = []
    for value in args.artifacts:
        issues.extend(verify_artifact(value))
    if issues:
        for issue in sorted(set(issues)):
            print(f"ERROR: {issue}", file=sys.stderr)
        return 1
    print("Release artifact contents are valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
