from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy.dialects import registry

from tools.official_suite_support import (
    load_official_suite_environment,
    official_suite_dburi,
    resolve_junit_file,
    verify_official_suite_database,
)


def main(
    argv: list[str] | None = None,
) -> int:
    registry.register("informix", "IfxAlchemy.pyodbc", "IfxDialect_pyodbc",)

    registry.register("informix.pyodbc", "IfxAlchemy.pyodbc", "IfxDialect_pyodbc",)

    try:
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
        "Running the official SQLAlchemy "
        "dialect compliance suite",
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

    suite_targets = [
        "test/test_suite.py",
    ]

    include_outparams = os.getenv(
        "IFXALCHEMY_INCLUDE_OUTPARAMS",
        "0",
    ).casefold() not in {
        "0",
        "false",
        "no",
    }

    if include_outparams:
        suite_targets.append(
            "test/test_out_parameters.py"
        )

    args = [
        "-c",
        "pytest.ini",
        "-p",
        "sqlalchemy.testing.plugin.pytestplugin",
        *suite_targets,
        "--dburi",
        dburi,
        "-ra",
    ]

    junit_file = resolve_junit_file(
        os.getenv(
            "SQLALCHEMY_SUITE_JUNIT",
            "",
        ),
        "sqlalchemy-suite.xml",
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
