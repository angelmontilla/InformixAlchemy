from __future__ import annotations

import os
import sys

import alembic
import pytest
from sqlalchemy.dialects import registry

from tools.official_suite_support import (
    PROJECT_ROOT,
    ensure_required_test_databases,
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

ALEMBIC_REQUIRED_REQUIREMENTS = (
    "comments",
    "alter_column",
    "computed_columns",
    "identity_columns",
)


class _AlembicRequirementsPlugin:
    """Install the Alembic-specific requirement contract after SQLAlchemy.

    SQLAlchemy's pytest plugin reloads ``requirement_cls`` from ``setup.cfg``
    during ``pytest_sessionstart``.  That late step overwrites the value
    supplied through ``--requirements``.  A try-last session-start hook is
    therefore required so the external Alembic suite sees the combined
    Informix/Alembic requirements object during test-module collection.
    """

    @pytest.hookimpl(trylast=True)
    def pytest_sessionstart(self, session) -> None:
        del session

        from sqlalchemy import testing
        from sqlalchemy.testing import config as sqla_config

        from test.alembic_requirements import Requirements

        requirements = Requirements()
        missing = [
            name
            for name in ALEMBIC_REQUIRED_REQUIREMENTS
            if not hasattr(requirements, name)
        ]

        if missing:
            formatted = ", ".join(sorted(missing))
            raise pytest.UsageError(
                "El contrato de requisitos de Alembic está incompleto: "
                f"{formatted}"
            )

        sqla_config.requirements = requirements
        testing.requires = requirements


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

        provisioned = ensure_required_test_databases()

        # The official SQLAlchemy provisioning layer owns destructive
        # cleanup through --dropfirst and the Informix-specific hooks in
        # IfxAlchemy.provision.  Preflight must validate the exact ANSI
        # target without rejecting objects that those hooks are designed to
        # remove (including the dialect comment sidecar catalogs).
        target = (
            verify_official_suite_database(
                dburi,
                require_empty=False,
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
        "Test databases: "
        f"non-ANSI={provisioned['non_ansi']['database']} "
        f"({'created' if provisioned['non_ansi']['created'] else 'present'}), "
        f"ANSI={provisioned['ansi']['database']} "
        f"({'created' if provisioned['ansi']['created'] else 'present'})",
        file=sys.stderr,
    )

    print(
        "Requirements: "
        f"{ALEMBIC_REQUIREMENTS_CLASS}",
        file=sys.stderr,
    )

    if target["has_objects"]:
        print(
            "Stale suite objects detected; --dropfirst will remove them: "
            f"{target['dirty_inventory']}",
            file=sys.stderr,
        )

    args = [
        "-c",
        str(PYTEST_CONFIG_FILE),
        "-p",
        "sqlalchemy.testing.plugin.pytestplugin",
        str(ALEMBIC_SUITE_TARGET),
        "--dburi",
        dburi,
        "--dropfirst",
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

    return pytest.main(
        args,
        plugins=[_AlembicRequirementsPlugin()],
    )


if __name__ == "__main__":
    raise SystemExit(
        main(sys.argv[1:])
    )
