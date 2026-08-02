# SPDX-License-Identifier: Apache-2.0
"""Portable table and column comment storage for Informix.

Informix 14.10/15.0 does not expose persistent table/column remarks through
``systables`` or ``syscolumns`` and does not provide the ``COMMENT ON`` DDL
used by SQLAlchemy's generic compiler.  The dialect therefore stores comments
in two ordinary catalog-extension tables owned by the connection user.

Comments are encoded as UTF-8 hexadecimal text.  The stored value is ASCII,
so comments round-trip independently from the database ``DB_LOCALE`` while
identifiers continue to follow Informix's normal locale rules.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import schema as sa_schema


TABLE_COMMENT_CATALOG = "ifx_sqla_table_comments"
COLUMN_COMMENT_CATALOG = "ifx_sqla_column_comments"
COMMENT_ENCODING_PREFIX = "u8:"
# Keep the complete sidecar row comfortably below Informix's 32,767-byte
# row limit.  A single LVARCHAR can reach 32,739 bytes, but the owner/name
# columns and row overhead reduce the usable maximum in this table.
COMMENT_STORAGE_LENGTH = 28672
MAX_COMMENT_UTF8_BYTES = (
    COMMENT_STORAGE_LENGTH - len(COMMENT_ENCODING_PREFIX)
) // 2

_TABLE_COMMENT_CATALOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_COMMENT_CATALOG} (
    tabid INTEGER NOT NULL,
    object_owner LVARCHAR(128) NOT NULL,
    object_name LVARCHAR(128) NOT NULL,
    comment_value LVARCHAR({COMMENT_STORAGE_LENGTH}) NOT NULL,
    PRIMARY KEY (tabid)
)
""".strip()

_COLUMN_COMMENT_CATALOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {COLUMN_COMMENT_CATALOG} (
    tabid INTEGER NOT NULL,
    colno SMALLINT NOT NULL,
    object_owner LVARCHAR(128) NOT NULL,
    object_name LVARCHAR(128) NOT NULL,
    column_name LVARCHAR(128) NOT NULL,
    comment_value LVARCHAR({COMMENT_STORAGE_LENGTH}) NOT NULL,
    PRIMARY KEY (tabid, colno)
)
""".strip()

COMMENT_CATALOG_DDL = (
    _TABLE_COMMENT_CATALOG_DDL,
    _COLUMN_COMMENT_CATALOG_DDL,
)

_COMMENT_DDL_TYPES = (
    sa_schema.SetTableComment,
    sa_schema.DropTableComment,
    sa_schema.SetColumnComment,
    sa_schema.DropColumnComment,
)


def is_comment_ddl(statement: Any) -> bool:
    """Return whether *statement* is one of SQLAlchemy's comment DDL nodes."""

    return isinstance(statement, _COMMENT_DDL_TYPES)


def ensure_comment_catalog(cursor: Any) -> None:
    """Create the two sidecar metadata tables on the current connection.

    ``CREATE TABLE IF NOT EXISTS`` makes the operation idempotent and safe for
    pooled connections.  It is deliberately executed only when a comment DDL
    statement is about to run; ordinary connections and read-only reflection
    do not create database objects.
    """

    for ddl in COMMENT_CATALOG_DDL:
        cursor.execute(ddl)


def encode_comment(value: str) -> str:
    """Encode a Python comment as locale-independent ASCII storage text."""

    if not isinstance(value, str):
        raise TypeError(
            "Informix table and column comments must be Python strings; "
            f"received {type(value).__name__}."
        )

    payload = value.encode("utf-8")
    if len(payload) > MAX_COMMENT_UTF8_BYTES:
        raise ValueError(
            "Informix SQLAlchemy comments are limited to "
            f"{MAX_COMMENT_UTF8_BYTES} UTF-8 bytes; received {len(payload)}."
        )

    return COMMENT_ENCODING_PREFIX + payload.hex()


def decode_comment(value: Any) -> str | None:
    """Decode one stored sidecar value.

    Unprefixed values are returned as text for forward compatibility with
    manually populated catalogs or early development snapshots.
    """

    if value is None:
        return None

    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        value = value.decode("ascii")

    text = str(value)
    if not text.startswith(COMMENT_ENCODING_PREFIX):
        return text

    hexadecimal = text[len(COMMENT_ENCODING_PREFIX):]
    try:
        return bytes.fromhex(hexadecimal).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as error:
        raise ValueError(
            "Invalid UTF-8 hexadecimal value in the Informix SQLAlchemy "
            "comment catalog."
        ) from error
