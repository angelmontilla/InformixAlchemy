from __future__ import annotations

import argparse
import datetime as dt
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, make_url


PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SAFE_ENV_KEYS = (
    "INFORMIXSERVER",
    "INFORMIXDIR",
    "DB_LOCALE",
    "CLIENT_LOCALE",
    "SERVER_LOCALE",
    "DELIMIDENT",
    "GL_USEGLU",
)
_LABEL_ENV_KEYS = {
    "server": "IFXALCHEMY_SERVER_PROFILE",
    "database": "IFXALCHEMY_DATABASE_PROFILE",
    "odbc": "IFXALCHEMY_ODBC_PROFILE",
    "python": "IFXALCHEMY_PYTHON_PROFILE",
    "sqlalchemy": "IFXALCHEMY_SQLALCHEMY_PROFILE",
    "identifiers": "IFXALCHEMY_DELIMIDENT_PROFILE",
    "locale": "IFXALCHEMY_LOCALE_PROFILE",
    "json_bson": "IFXALCHEMY_SBSPACE_PROFILE",
}


def _package_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def normalise_dburi(value: object) -> str | URL | None:
    """Return one SQLAlchemy URL from pytest's ``--dburi`` value.

    SQLAlchemy's pytest plugin stores ``--dburi`` values in a sequence even
    when the command line contains only one URL.  ``create_engine()`` accepts
    a string or :class:`~sqlalchemy.engine.URL`, so callers must unwrap that
    sequence before constructing an engine.
    """
    if value is None:
        return None

    if isinstance(value, (str, URL)):
        return value

    if isinstance(value, Sequence):
        urls = [candidate for candidate in value if candidate]
        if not urls:
            return None
        if len(urls) != 1:
            raise ValueError(
                "Informix certification requires exactly one --dburi value "
                f"per pytest run; received {len(urls)}."
            )

        selected = urls[0]
        if not isinstance(selected, (str, URL)):
            raise TypeError(
                "Unsupported --dburi item returned by pytest: "
                f"{type(selected).__name__}."
            )
        return selected

    raise TypeError(
        "Unsupported --dburi value returned by pytest: "
        f"{type(value).__name__}."
    )


def render_safe_url(url: str | URL | None) -> str | None:
    """Render a SQLAlchemy URL without exposing its password."""
    if not url:
        return None
    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:
        return "<invalid-or-non-sqlalchemy-url>"


def _safe_url(url: str | URL | None) -> str | None:
    return render_safe_url(url)


def _sql_scalar(connection: Any, sql: str, params: tuple[Any, ...] = ()):
    try:
        return connection.exec_driver_sql(sql, params).scalar()
    except Exception as exc:  # provenance must not hide a successful test run
        return {"error": f"{type(exc).__name__}: {exc}"}


def _database_metadata(connection: Any) -> dict[str, Any]:
    database_name = _sql_scalar(
        connection,
        "SELECT DBINFO('dbname') FROM systables WHERE tabid = 1",
    )
    server_version = _sql_scalar(
        connection,
        "SELECT DBINFO('version', 'full') FROM systables WHERE tabid = 1",
    )
    metadata: dict[str, Any] = {
        "name": database_name,
        "server_version_full": server_version,
    }
    if isinstance(database_name, str) and database_name.strip():
        try:
            row = connection.exec_driver_sql(
                """
                SELECT FIRST 1 d.is_ansi, d.is_logging
                FROM sysmaster:sysdatabases d
                WHERE LOWER(d.name) = LOWER(?)
                """,
                (database_name.strip(),),
            ).first()
        except Exception as exc:
            metadata["mode_error"] = f"{type(exc).__name__}: {exc}"
        else:
            if row is not None:
                metadata["ansi"] = bool(int(row[0]))
                metadata["logging"] = bool(int(row[1]))

    sbspace = _sql_scalar(
        connection,
        """
        SELECT FIRST 1 TRIM(cf_effective)
        FROM sysmaster:sysconfig
        WHERE UPPER(cf_name) = 'SBSPACENAME'
        """,
    )
    metadata["sbspace_name"] = sbspace
    metadata["sbspace_configured"] = bool(
        isinstance(sbspace, str) and sbspace.strip()
    )
    return metadata


def _dbapi_connection(connection: Any):
    try:
        proxied = connection.connection
    except AttributeError:
        return None
    return getattr(proxied, "driver_connection", proxied)


def _odbc_metadata(connection: Any) -> dict[str, Any]:
    dbapi_connection = _dbapi_connection(connection)
    if dbapi_connection is None or not hasattr(dbapi_connection, "getinfo"):
        return {"available": False}

    try:
        import pyodbc
    except ModuleNotFoundError:
        return {"available": False, "error": "pyodbc is not installed"}

    fields = {
        "driver_name": pyodbc.SQL_DRIVER_NAME,
        "driver_version": pyodbc.SQL_DRIVER_VER,
        "dbms_name": pyodbc.SQL_DBMS_NAME,
        "dbms_version": pyodbc.SQL_DBMS_VER,
        "odbc_version": pyodbc.SQL_ODBC_VER,
        "data_source_name": pyodbc.SQL_DATA_SOURCE_NAME,
    }
    result: dict[str, Any] = {"available": True}
    for name, info_type in fields.items():
        try:
            result[name] = dbapi_connection.getinfo(info_type)
        except Exception as exc:
            result[name] = {"error": f"{type(exc).__name__}: {exc}"}
    return result


def _command_version(executable: str, *arguments: str) -> dict[str, Any] | None:
    path = shutil.which(executable)
    if path is None:
        return None
    try:
        completed = subprocess.run(
            [path, *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception as exc:
        return {"path": path, "error": f"{type(exc).__name__}: {exc}"}
    output = (completed.stdout or completed.stderr).strip()
    return {
        "path": path,
        "returncode": completed.returncode,
        "output": output[:2000],
    }


def collect_runtime_provenance(
    *,
    url: str | None = None,
    connection: Any | None = None,
) -> dict[str, Any]:
    """Collect a redacted, JSON-serializable certification record."""
    from IfxAlchemy import __version__ as dialect_version

    labels = {
        axis: os.getenv(variable, "unlabelled").strip() or "unlabelled"
        for axis, variable in _LABEL_ENV_KEYS.items()
    }
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "project": {
            "name": "IfxAlchemy",
            "version": dialect_version,
        },
        "runtime": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "architecture": platform.architecture()[0],
        },
        "packages": {
            "SQLAlchemy": _package_version("SQLAlchemy"),
            "alembic": _package_version("alembic"),
            "pyodbc": _package_version("pyodbc"),
        },
        "environment": {
            key: os.getenv(key)
            for key in _SAFE_ENV_KEYS
            if os.getenv(key) is not None
        },
        "labels": labels,
        "connection": {"safe_url": _safe_url(url)},
        "client_tools": {
            "esql": _command_version("esql", "-V"),
            "dbaccess": _command_version("dbaccess", "-V"),
        },
    }

    if connection is not None:
        report["informix"] = _database_metadata(connection)
        report["odbc"] = _odbc_metadata(connection)
    else:
        report["informix"] = {"connected": False}
        report["odbc"] = {"available": False}
    return report


def write_provenance(path: str | Path, report: dict[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return destination


def junit_properties(report: dict[str, Any]) -> dict[str, str]:
    """Return compact properties suitable for a JUnit ``testsuite`` node."""
    informix = report.get("informix", {})
    odbc = report.get("odbc", {})
    labels = report.get("labels", {})
    values = {
        "ifxalchemy.version": report.get("project", {}).get("version"),
        "python.version": report.get("runtime", {}).get("python"),
        "platform": report.get("runtime", {}).get("platform"),
        "sqlalchemy.version": report.get("packages", {}).get("SQLAlchemy"),
        "alembic.version": report.get("packages", {}).get("alembic"),
        "pyodbc.version": report.get("packages", {}).get("pyodbc"),
        "informix.server_version": informix.get("server_version_full"),
        "informix.database": informix.get("name"),
        "informix.database_ansi": informix.get("ansi"),
        "informix.sbspace": informix.get("sbspace_name"),
        "odbc.driver_name": odbc.get("driver_name"),
        "odbc.driver_version": odbc.get("driver_version"),
    }
    values.update({f"matrix.{key}": value for key, value in labels.items()})
    return {
        key: str(value)
        for key, value in values.items()
        if value is not None and not isinstance(value, dict)
    }


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def validate_matrix_coverage(
    matrix: dict[str, Any],
    reports: Iterable[dict[str, Any]],
) -> dict[str, list[str]]:
    """Return missing labels for every required certification axis."""
    reports = tuple(reports)
    missing: dict[str, list[str]] = {}
    for axis, required_values in matrix["required_axes"].items():
        observed = {
            str(report.get("labels", {}).get(axis, "unlabelled"))
            for report in reports
        }
        absent = sorted(set(map(str, required_values)) - observed)
        if absent:
            missing[axis] = absent
    return missing


def _collect_cli(args: argparse.Namespace) -> int:
    url = args.url or os.getenv(args.url_env, "").strip()
    if not url:
        raise SystemExit(
            f"No connection URL supplied; set {args.url_env} or pass --url"
        )
    engine = create_engine(url, pool_pre_ping=True)
    try:
        with engine.connect() as connection:
            report = collect_runtime_provenance(url=url, connection=connection)
    finally:
        engine.dispose()
    destination = write_provenance(args.output, report)
    print(destination)
    return 0


def _validate_cli(args: argparse.Namespace) -> int:
    matrix = _load_json(args.matrix)
    report_paths = [Path(path) for path in args.reports]
    reports = [_load_json(path) for path in report_paths]
    missing = validate_matrix_coverage(matrix, reports)
    if missing:
        print(json.dumps({"missing": missing}, indent=2), file=sys.stderr)
        return 1
    print(json.dumps({"status": "complete", "reports": len(reports)}))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--url")
    collect_parser.add_argument(
        "--url-env",
        default="INFORMIX_SQLALCHEMY_URL",
    )
    collect_parser.add_argument(
        "--output",
        default="artifacts/certification/provenance.json",
    )
    collect_parser.set_defaults(func=_collect_cli)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument(
        "--matrix",
        default=str(PROJECT_ROOT / "certification-matrix.json"),
    )
    validate_parser.add_argument("reports", nargs="+")
    validate_parser.set_defaults(func=_validate_cli)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
