from __future__ import annotations

"""Probe the real Informix ODBC representation of native JSON and BSON.

The probe intentionally uses a raw DBAPI cursor obtained from SQLAlchemy so it
records the values exactly as pyodbc exposes them, before dialect result
processors transform them.

Examples
--------
python tools/diagnose_json_bson_pyodbc.py
python tools/diagnose_json_bson_pyodbc.py --url "$INFORMIX_SQLALCHEMY_URL"
python tools/diagnose_json_bson_pyodbc.py --sbspace sbspace1
"""

import argparse
import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.engine import make_url


PROJECT_ROOT = Path(__file__).resolve().parents[1]
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _load_project_environment() -> None:
    """Load .env.informix when python-dotenv is available."""
    env_file = PROJECT_ROOT / ".env.informix"
    if not env_file.is_file():
        return

    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv(env_file, override=False)


def _validated_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{label} must be an unquoted Informix identifier: {value!r}"
        )
    return value


def _description_as_dicts(description: Any) -> list[dict[str, Any]] | None:
    if description is None:
        return None

    names = (
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "null_ok",
    )
    result = []
    for column in description:
        result.append(
            {
                name: repr(column[index]) if index < len(column) else None
                for index, name in enumerate(names)
            }
        )
    return result


def _probe(cursor, label: str, sql: str, parameters: tuple[Any, ...]) -> None:
    cursor.execute(sql, parameters)
    row = cursor.fetchone()
    values = [] if row is None else list(row)
    print(f"\n[{label}]")
    print(f"SQL: {sql}")
    print(
        "cursor.description: "
        + json.dumps(
            _description_as_dicts(cursor.description),
            indent=2,
            ensure_ascii=False,
        )
    )
    if row is None:
        print("row: <no row>")
        return

    for position, value in enumerate(values):
        print(f"value[{position}].type: {type(value)!r}")
        print(f"value[{position}].repr: {value!r}")


def _insert_rows(cursor, table_name: str) -> None:
    small = json.dumps(
        {
            "object": {"active": True},
            "array": [1, 2.5, "España", None],
            "unicode": "cañón 日本語",
            "date": "2026-07-31T10:17:00+02:00",
            "binary_base64": "AP8=",
        },
        ensure_ascii=False,
    )
    # Informix stores documents larger than 4 KiB in an sbspace, while the
    # native JSON/BSON document limit is 32 KiB.
    large = json.dumps(
        {
            "kind": "large",
            "text": "x" * 6000,
            "items": list(range(128)),
            "nullable": None,
        },
        ensure_ascii=True,
    )

    inserts = (
        (
            1,
            "object-array-unicode",
            small,
            small,
        ),
        (
            2,
            "sql-null",
            None,
            None,
        ),
        (
            3,
            "large-document",
            large,
            large,
        ),
    )

    sql = (
        f"INSERT INTO {table_name} "
        "(id, case_name, json_doc, bson_doc) "
        "VALUES (?, ?, CAST(? AS JSON), CAST(CAST(? AS JSON) AS BSON))"
    )
    for row in inserts:
        cursor.execute(sql, row)


def _create_table(cursor, table_name: str, sbspace: str | None) -> None:
    storage = ""
    if sbspace:
        storage = (
            f" PUT json_doc IN ({sbspace}), "
            f"bson_doc IN ({sbspace})"
        )

    cursor.execute(
        f"CREATE TABLE {table_name} ("
        "id INTEGER NOT NULL PRIMARY KEY, "
        "case_name VARCHAR(32) NOT NULL, "
        "json_doc JSON, "
        "bson_doc BSON"
        f"){storage} LOCK MODE ROW"
    )


def _run_probe(raw_connection, table_name: str, sbspace: str | None) -> None:
    cursor = raw_connection.cursor()
    try:
        _create_table(cursor, table_name, sbspace)
        _insert_rows(cursor, table_name)
        raw_connection.commit()

        _probe(
            cursor,
            "JSON object/array/Unicode",
            f"SELECT json_doc FROM {table_name} WHERE id = ?",
            (1,),
        )
        _probe(
            cursor,
            "BSON raw driver value and BSON cast to JSON",
            (
                f"SELECT bson_doc, CAST(bson_doc AS JSON) "
                f"FROM {table_name} WHERE id = ?"
            ),
            (1,),
        )
        _probe(
            cursor,
            "SQL NULL",
            (
                f"SELECT json_doc, bson_doc FROM {table_name} "
                "WHERE id = ?"
            ),
            (2,),
        )
        print(
            "\n[native document-null limitation]\n"
            "Informix JSON/BSON opaque values must be top-level document "
            "objects. Whole-column JSON null is not representable; use SQL "
            "NULL and test JSON null inside document fields instead."
        )
        _probe(
            cursor,
            "large document (>4 KiB and <32 KiB)",
            (
                f"SELECT json_doc, bson_doc, CAST(bson_doc AS JSON) "
                f"FROM {table_name} WHERE id = ?"
            ),
            (3,),
        )

        print("\n[storage]")
        print(f"explicit_sbspace: {sbspace!r}")
        print(
            "The large-document row exercises smart-large-object storage. "
            "When --sbspace is supplied, both opaque columns are created "
            "with PUT ... IN (<sbspace>)."
        )
    finally:
        try:
            cursor.execute(f"DROP TABLE {table_name}")
            raw_connection.commit()
        except Exception as cleanup_error:
            raw_connection.rollback()
            print(
                f"WARNING: could not drop {table_name}: {cleanup_error}",
                file=sys.stderr,
            )
        cursor.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a real Informix JSON/BSON table and print raw pyodbc "
            "cursor metadata and Python value representations."
        )
    )
    parser.add_argument(
        "--url",
        default="",
        help=(
            "SQLAlchemy informix+pyodbc URL. Defaults to "
            "INFORMIX_SQLALCHEMY_URL."
        ),
    )
    parser.add_argument(
        "--sbspace",
        default="",
        help=(
            "Optional existing sbspace used in CREATE TABLE PUT clauses. "
            "The server must also have JSON compatibility requirements "
            "configured, including a default SBSPACENAME for large values."
        ),
    )
    parser.add_argument(
        "--table",
        default="",
        help="Optional temporary table name.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    _load_project_environment()

    url = args.url.strip() or os.getenv("INFORMIX_SQLALCHEMY_URL", "").strip()
    if not url:
        print(
            "ERROR: provide --url or set INFORMIX_SQLALCHEMY_URL",
            file=sys.stderr,
        )
        return 2

    sbspace = (
        _validated_identifier(args.sbspace.strip(), "sbspace")
        if args.sbspace.strip()
        else None
    )
    default_table = f"ifx_json_bson_{uuid.uuid4().hex[:10]}"
    table_name = _validated_identifier(
        args.table.strip() or default_table,
        "table",
    )

    registry.register(
        "informix.pyodbc",
        "IfxAlchemy.pyodbc",
        "IfxDialect_pyodbc",
    )
    registry.register(
        "informix",
        "IfxAlchemy.pyodbc",
        "IfxDialect_pyodbc",
    )

    engine = create_engine(url)
    print(
        "Target: "
        + make_url(url).render_as_string(hide_password=True)
    )
    print(f"Temporary table: {table_name}")

    raw_connection = engine.raw_connection()
    try:
        _run_probe(raw_connection, table_name, sbspace)
    finally:
        raw_connection.close()
        engine.dispose()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
