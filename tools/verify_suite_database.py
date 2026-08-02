from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# flake8: noqa: E402
from tools.official_suite_support import (
    ensure_required_test_databases,
    load_official_suite_environment,
    official_suite_dburi,
    verify_official_suite_database,
)


def main() -> int:
    try:
        env_file = load_official_suite_environment()
        databases = ensure_required_test_databases()
        details = verify_official_suite_database(
            official_suite_dburi(),
            require_empty=False,
        )
    except Exception as exc:
        print(
            f"ERROR: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2

    print(f"Configuración: {env_file}")
    print(
        "Base no ANSI: "
        f"{databases['non_ansi']['database']} "
        f"(created={databases['non_ansi']['created']})"
    )
    print(
        "Base ANSI: "
        f"{databases['ansi']['database']} "
        f"(created={databases['ansi']['created']})"
    )
    print(f"Suite oficial: {details['safe_url']}")
    print(f"Tablas: {details['tables']}")
    print(f"Vistas: {details['views']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
