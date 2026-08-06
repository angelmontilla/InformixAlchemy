"""Scan source trees and release archives for secrets and unsafe files."""
from __future__ import annotations

import argparse
import re
import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator


FORBIDDEN_ENV_NAMES = {
    ".env",
    ".env.informix",
    ".env.official-suites",
}
FORBIDDEN_GENERATED_PARTS = {
    ".pytest_cache",
    "__pycache__",
    "htmlcov",
    "build",
    "dist",
}
FORBIDDEN_GENERATED_SUFFIXES = {".pyc", ".pyo"}
FORBIDDEN_GENERATED_NAMES = {".coverage", "coverage.xml", "salida.txt"}
BINARY_SUFFIXES = {
    ".bmp",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".pdf",
    ".png",
    ".so",
}
PLACEHOLDERS = {
    "",
    "change-me",
    "changeme",
    "example",
    "example-only",
    "replace-me",
    "your-password",
    "<password>",
    "${informix_password}",
}
MAX_ARCHIVE_SIZE = 100 * 1024 * 1024
MAX_MEMBER_SIZE = 5 * 1024 * 1024
MAX_TOTAL_UNCOMPRESSED_SIZE = 200 * 1024 * 1024

_ASSIGNMENT = re.compile(
    r"^(?P<key>[A-Z0-9_]*(?:PASSWORD|PASSWD|TOKEN|SECRET|API_KEY)[A-Z0-9_]*)"
    r"\s*=\s*(?P<value>.*)$",
    re.IGNORECASE,
)
_PRIVATE_KEY = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")
_COMMON_TOKEN = re.compile(
    r"(?:AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9_]{30,}|sk-[A-Za-z0-9]{32,})"
)


class ArchiveSafetyError(ValueError):
    """Raised when an archive exceeds limits or contains unsafe members."""


def _iter_files(root: Path) -> Iterable[Path]:
    if root.is_file():
        yield root
        return
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def _relative(path: Path, root: Path) -> Path:
    if root.is_file():
        return Path(path.name)
    try:
        return path.relative_to(root)
    except ValueError:
        return Path(path.name)


def _is_archive(path: Path) -> bool:
    lowered = path.name.lower()
    return lowered.endswith((".zip", ".whl", ".tar.gz", ".tgz"))


def _safe_member_path(name: str) -> PurePosixPath:
    normalized = name.replace("\\", "/")
    if "\x00" in normalized:
        raise ArchiveSafetyError(f"archive member contains NUL: {name!r}")
    member = PurePosixPath(normalized)
    if (
        member.is_absolute()
        or ".." in member.parts
        or (member.parts and member.parts[0].endswith(":"))
    ):
        raise ArchiveSafetyError(f"unsafe archive member path: {name}")
    return member


def _iter_zip_members(path: Path) -> Iterator[tuple[PurePosixPath, bytes]]:
    total_size = 0
    with zipfile.ZipFile(path) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            member = _safe_member_path(info.filename)
            if info.file_size > MAX_MEMBER_SIZE:
                raise ArchiveSafetyError(f"archive member too large: {info.filename}")
            total_size += info.file_size
            if total_size > MAX_TOTAL_UNCOMPRESSED_SIZE:
                raise ArchiveSafetyError("archive expands beyond the allowed size")
            with archive.open(info) as member_file:
                yield member, member_file.read(MAX_MEMBER_SIZE + 1)


def _iter_tar_members(path: Path) -> Iterator[tuple[PurePosixPath, bytes]]:
    total_size = 0
    with tarfile.open(path, mode="r:*") as archive:
        for info in archive.getmembers():
            if info.isdir():
                continue
            member = _safe_member_path(info.name)
            if not info.isfile():
                raise ArchiveSafetyError(
                    f"unsupported non-regular archive member: {info.name}"
                )
            if info.size > MAX_MEMBER_SIZE:
                raise ArchiveSafetyError(f"archive member too large: {info.name}")
            total_size += info.size
            if total_size > MAX_TOTAL_UNCOMPRESSED_SIZE:
                raise ArchiveSafetyError("archive expands beyond the allowed size")
            extracted = archive.extractfile(info)
            if extracted is None:
                raise ArchiveSafetyError(f"could not read archive member: {info.name}")
            yield member, extracted.read(MAX_MEMBER_SIZE + 1)


def _iter_archive_members(path: Path) -> Iterator[tuple[PurePosixPath, bytes]]:
    if path.stat().st_size > MAX_ARCHIVE_SIZE:
        raise ArchiveSafetyError(f"archive is larger than {MAX_ARCHIVE_SIZE} bytes")
    if path.name.lower().endswith((".zip", ".whl")):
        yield from _iter_zip_members(path)
    else:
        yield from _iter_tar_members(path)


def _release_path_issues(
    parts: tuple[str, ...],
    display_name: str,
    *,
    allow_package_metadata: bool = False,
) -> list[str]:
    issues: list[str] = []
    file_name = parts[-1] if parts else display_name
    suffix = Path(file_name).suffix.lower()
    if (
        not allow_package_metadata
        and any(part.endswith(".egg-info") for part in parts)
    ):
        issues.append(f"generated package metadata: {display_name}")
    if any(part in FORBIDDEN_GENERATED_PARTS for part in parts):
        issues.append(f"generated directory content: {display_name}")
    if file_name in FORBIDDEN_GENERATED_NAMES:
        issues.append(f"generated artifact: {display_name}")
    if suffix in FORBIDDEN_GENERATED_SUFFIXES:
        issues.append(f"compiled Python artifact: {display_name}")
    if "artifacts" in parts and file_name != ".gitkeep":
        issues.append(f"test artifact: {display_name}")
    return issues


def _secret_content_issues(
    data: bytes,
    logical_path: PurePosixPath,
    display_name: str,
    *,
    allow_local_environment_files: bool,
) -> list[str]:
    issues: list[str] = []
    logical_name = logical_path.name.casefold()
    if logical_name in FORBIDDEN_ENV_NAMES:
        if not allow_local_environment_files:
            issues.append(f"forbidden environment file: {display_name}")
        return issues
    if logical_path.suffix.lower() in BINARY_SUFFIXES:
        return issues
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return issues

    if _PRIVATE_KEY.search(text):
        issues.append(f"private key material: {display_name}")
    if _COMMON_TOKEN.search(text):
        issues.append(f"credential-like token: {display_name}")
    if logical_name.endswith(".example") or logical_name.startswith(".env"):
        for line_number, line in enumerate(text.splitlines(), 1):
            match = _ASSIGNMENT.match(line.strip())
            if match is None:
                continue
            value = match.group("value").strip().strip('"\'').casefold()
            if value not in PLACEHOLDERS:
                issues.append(
                    "non-placeholder sensitive value: "
                    f"{display_name}:{line_number}"
                )
    return issues


def _archive_issues(path: Path, relative: Path, *, release: bool) -> list[str]:
    issues: list[str] = []
    allow_package_metadata = path.name.lower().endswith((".tar.gz", ".tgz", ".whl"))
    try:
        for member, data in _iter_archive_members(path):
            display_name = f"{relative.as_posix()}!{member.as_posix()}"
            issues.extend(
                _secret_content_issues(
                    data,
                    member,
                    display_name,
                    allow_local_environment_files=False,
                )
            )
            if release:
                issues.extend(
                    _release_path_issues(
                        member.parts,
                        display_name,
                        allow_package_metadata=allow_package_metadata,
                    )
                )
    except (ArchiveSafetyError, OSError, tarfile.TarError, zipfile.BadZipFile) as error:
        issues.append(f"unsafe or unreadable archive: {relative.as_posix()}: {error}")
    return issues


def _scan(
    root: str | Path,
    *,
    release: bool,
    allow_local_environment_files: bool,
) -> list[str]:
    root_path = Path(root).resolve()
    issues: list[str] = []
    for path in _iter_files(root_path):
        relative = _relative(path.resolve(), root_path)
        if any(part in {".git", ".venv", "venv"} for part in relative.parts):
            continue
        if _is_archive(path):
            issues.extend(_archive_issues(path, relative, release=release))
            if release:
                issues.extend(_release_path_issues(relative.parts, relative.as_posix()))
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        issues.extend(
            _secret_content_issues(
                data,
                PurePosixPath(relative.as_posix()),
                relative.as_posix(),
                allow_local_environment_files=allow_local_environment_files,
            )
        )
        if release:
            issues.extend(_release_path_issues(relative.parts, relative.as_posix()))
    return sorted(set(issues))


def scan_secrets(
    root: str | Path,
    *,
    allow_local_environment_files: bool = False,
) -> list[str]:
    """Find environment files and common credential material."""
    return _scan(
        root,
        release=False,
        allow_local_environment_files=allow_local_environment_files,
    )


def scan_release_tree(root: str | Path) -> list[str]:
    """Find files that must never appear in a release artifact."""
    return _scan(
        root,
        release=True,
        allow_local_environment_files=False,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", nargs="?", default=".")
    parser.add_argument(
        "--release",
        action="store_true",
        help="also reject generated files and test output",
    )
    parser.add_argument(
        "--allow-local-env",
        action="store_true",
        help=(
            "allow Git-ignored local .env files while scanning a developer "
            "working tree; incompatible with --release"
        ),
    )
    args = parser.parse_args(argv)
    if args.release and args.allow_local_env:
        parser.error("--allow-local-env cannot be combined with --release")
    issues = (
        scan_release_tree(args.path)
        if args.release
        else scan_secrets(
            args.path,
            allow_local_environment_files=args.allow_local_env,
        )
    )
    if issues:
        for issue in issues:
            print(f"ERROR: {issue}", file=sys.stderr)
        return 1
    print("No forbidden secrets or environment files found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
