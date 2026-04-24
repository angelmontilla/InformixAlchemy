from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy.dialects import registry
from sqlalchemy.engine import make_url


DEFAULT_INFORMIX_SQLALCHEMY_URL = (
    "informix+pyodbc://informix:in4mix@127.0.0.1/prueba4db"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&protocol=onsoctcp"
    "&server=informix"
    "&service=9088"
    "&DELIMIDENT=Y"
)


def _normalized_dburi() -> str:
    url = (
        os.getenv("INFORMIX_SQLALCHEMY_SUITE_URL")
        or os.getenv("INFORMIX_SQLALCHEMY_URL")
        or DEFAULT_INFORMIX_SQLALCHEMY_URL
    )
    if "delimident=" not in url.lower():
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}DELIMIDENT=Y"
    return url


def main(argv: list[str] | None = None) -> int:
    registry.register("informix", "IfxAlchemy.pyodbc", "IfxDialect_pyodbc")
    registry.register("informix.pyodbc", "IfxAlchemy.pyodbc", "IfxDialect_pyodbc")

    dburi = _normalized_dburi()
    safe_dburi = make_url(dburi).render_as_string(hide_password=True)
    print(
        "Running the official SQLAlchemy suite for informix+pyodbc "
        f"against {safe_dburi}",
        file=sys.stderr,
    )

    args = [
        "-c",
        "pytest.ini",
        "-p",
        "sqlalchemy.testing.plugin.pytestplugin",
        "test/test_suite.py",
        "--dburi",
        dburi,
    ]

    if argv:
        args.extend(argv)

    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
