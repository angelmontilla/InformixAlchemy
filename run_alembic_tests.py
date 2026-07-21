from __future__ import annotations

import os
import sys

import alembic
import pytest
from sqlalchemy.dialects import registry

from tools.official_suite_support import (
    PROJECT_ROOT,
    load_official_suite_environment,
    official_suite_dburi,
    resolve_junit_file,
    verify_official_suite_database,
)


PYTEST_CONFIG_FILE = (
    PROJECT_ROOT / "pytest.ini"
)

ALEMBIC_SUITE_TARGET = (
    PROJECT_ROOT
    / "test"
    / "test_suite_alembic.py"
)

ALEMBIC_REQUIREMENTS_CLASS = (
    "test.alembic_requirements:Requirements"
)


def _assert_required_files_exist() -> None:
    missing = [
        path
        for path in (
            PYTEST_CONFIG_FILE,
            ALEMBIC_SUITE_TARGET,
        )
        if not path.is_file()
    ]

    requirements_file = (
        PROJECT_ROOT
        / "test"
        / "alembic_requirements.py"
    )

    if not requirements_file.is_file():
        missing.append(requirements_file)

    if missing:
        formatted = "\n".join(
            f"  - {path}"
            for path in missing
        )

        raise RuntimeError(
            "Faltan ficheros necesarios para "
            "la suite de Alembic:\n"
            f"{formatted}"
        )


def main(
    argv: list[str] | None = None,
) -> int:
    registry.register(
        "informix",
        "IfxAlchemy.pyodbc",
        "IfxDialect_pyodbc",
    )

    registry.register(
        "informix.pyodbc",
        "IfxAlchemy.pyodbc",
        "IfxDialect_pyodbc",
    )

    try:
        _assert_required_files_exist()

        env_file = (
            load_official_suite_environment()
        )

        dburi = official_suite_dburi()

        target = (
            verify_official_suite_database(
                dburi
            )
        )

    except Exception as exc:
        print(
            f"ERROR: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2

    print(
        "Running the official Alembic "
        "external dialect suite",
        file=sys.stderr,
    )

    print(
        f"Alembic: {alembic.__version__}",
        file=sys.stderr,
    )

    print(
        f"Environment: {env_file}",
        file=sys.stderr,
    )

    print(
        f"Target: {target['safe_url']}",
        file=sys.stderr,
    )

    print(
        "Requirements: "
        f"{ALEMBIC_REQUIREMENTS_CLASS}",
        file=sys.stderr,
    )

    args = [
        "-c",
        str(PYTEST_CONFIG_FILE),
        "-p",
        "sqlalchemy.testing.plugin.pytestplugin",
        "--requirements",
        ALEMBIC_REQUIREMENTS_CLASS,
        str(ALEMBIC_SUITE_TARGET),
        "--dburi",
        dburi,
        "-ra",
    ]

    junit_file = resolve_junit_file(
        os.getenv(
            "ALEMBIC_SUITE_JUNIT",
            "",
        ),
        "alembic-suite.xml",
    )

    args.extend(
        [
            "--junitxml",
            str(junit_file),
        ]
    )

    if argv:
        args.extend(argv)

    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(
        main(sys.argv[1:])
    )
