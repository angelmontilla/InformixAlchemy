from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy.dialects import registry


DEFAULT_INFORMIX_SQLALCHEMY_URL = (
    "informix+pyodbc://ctl:magogo@192.168.11.64/faempre999"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&protocol=onsoctcp"
    "&server=pru_famadesa_s9"
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
    registry.register("informix.ifxpy", "IfxAlchemy.IfxPy", "IfxDialect_IfxPy")

    args = [
        "-c",
        "pytest.ini",
        "-p",
        "sqlalchemy.testing.plugin.pytestplugin",
        "test/test_suite.py",
        "--dburi",
        _normalized_dburi(),
    ]

    if argv:
        args.extend(argv)

    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
