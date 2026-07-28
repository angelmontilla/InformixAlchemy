from __future__ import annotations

"""Certify the SQL-expression capability block against Informix.

The runner executes three independent gates and stops on the first failure:

1. Pure compiler, compatibility and requirement-contract regressions.
2. Nine direct round-trip integration tests against Informix.
3. Fifteen exact SQLAlchemy 2.0 dialect-suite tests unlocked by the block.

The two database gates emit JUnit files.  Certification succeeds only when
all expected test cases are present and none is failed, errored or skipped.
"""

import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "artifacts"
INTEGRATION_JUNIT = ARTIFACTS / "sql-expression-integration.xml"
OFFICIAL_JUNIT = ARTIFACTS / "sql-expression-official-suite.xml"

OFFICIAL_FILTER = (
    "(CTETest and ("
    "test_select_nonrecursive_round_trip or "
    "test_select_recursive_round_trip or "
    "test_insert_from_select_round_trip or "
    "test_update_from_round_trip or "
    "test_delete_from_round_trip or "
    "test_delete_scalar_subq_round_trip"
    ")) or "
    "(CompoundSelectTest and ("
    "test_limit_offset_selectable_in_unions or "
    "test_order_by_selectable_in_unions or "
    "test_limit_offset_in_unions_from_alias"
    ")) or "
    "(FetchLimitOffsetTest and ("
    "test_simple_fetch or "
    "test_simple_fetch_offset or "
    "test_fetch_offset_no_order or "
    "test_fetch_offset_nobinds or "
    "test_bound_fetch_offset or "
    "test_expr_fetch_offset"
    "))"
)


def _run(
    label: str,
    command: list[str],
    *,
    env: dict[str, str] | None = None,
) -> None:
    print(f"\n=== {label} ===", flush=True)
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=False,
    )
    if completed.returncode:
        raise SystemExit(completed.returncode)


def _assert_junit(path: Path, expected_tests: int) -> None:
    if not path.is_file():
        raise SystemExit(f"Certification JUnit file was not created: {path}")

    root = ET.parse(path).getroot()
    cases = list(root.iter("testcase"))
    failures = [case for case in cases if case.find("failure") is not None]
    errors = [case for case in cases if case.find("error") is not None]
    skipped = [case for case in cases if case.find("skipped") is not None]

    if len(cases) != expected_tests:
        raise SystemExit(
            f"Expected {expected_tests} certified tests in {path.name}, "
            f"but found {len(cases)}."
        )

    if failures or errors or skipped:
        raise SystemExit(
            f"Certification failed for {path.name}: "
            f"failures={len(failures)}, errors={len(errors)}, "
            f"skipped={len(skipped)}."
        )


def main() -> int:
    python = sys.executable
    ARTIFACTS.mkdir(parents=True, exist_ok=True)

    _run(
        "Compiler and contract regressions",
        [
            python,
            "-m",
            "pytest",
            "test/test_sql_expression_capabilities.py",
            "test/test_sqla_compat_private_api.py",
            "test/test_sqlalchemy_suite_gate.py",
            "test/test_sql_expression_certification_runner.py",
            "-k",
            (
                "sql_expression or cte or fetch or union or "
                "intersect or test_except"
            ),
            "-vv",
            "-s",
            "--tb=short",
        ],
    )

    if not os.getenv("INFORMIX_SQLALCHEMY_URL"):
        print(
            "INFORMIX_SQLALCHEMY_URL is not set; the integration fixture "
            "will use its documented local default.",
            flush=True,
        )

    INTEGRATION_JUNIT.unlink(missing_ok=True)
    _run(
        "Direct Informix round-trip certification",
        [
            python,
            "-m",
            "pytest",
            "test/test_sql_expression_informix_integration.py",
            "-vv",
            "-s",
            "--tb=short",
            "--junitxml",
            str(INTEGRATION_JUNIT),
        ],
    )
    _assert_junit(INTEGRATION_JUNIT, expected_tests=9)

    OFFICIAL_JUNIT.unlink(missing_ok=True)
    official_env = os.environ.copy()
    official_env["SQLALCHEMY_SUITE_JUNIT"] = str(OFFICIAL_JUNIT)
    _run(
        "Official SQLAlchemy dialect-suite certification",
        [
            python,
            "run_tests.py",
            "-k",
            OFFICIAL_FILTER,
            "-vv",
            "-s",
            "--tb=short",
        ],
        env=official_env,
    )
    _assert_junit(OFFICIAL_JUNIT, expected_tests=15)

    print(
        "\nSQL-expression certification completed successfully: "
        "9 integration tests and 15 official tests passed without skips.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
