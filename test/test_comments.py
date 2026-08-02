from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope
from sqlalchemy.schema import (
    DropColumnComment,
    DropTableComment,
    SetColumnComment,
    SetTableComment,
)
from sqlalchemy.sql import quoted_name

from IfxAlchemy.base import IfxDialect
from IfxAlchemy.comments import (
    COLUMN_COMMENT_CATALOG,
    COMMENT_CATALOG_DDL,
    MAX_COMMENT_UTF8_BYTES,
    TABLE_COMMENT_CATALOG,
    decode_comment,
    encode_comment,
    ensure_comment_catalog,
)
from IfxAlchemy.requirements import Requirements


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


def test_comment_codec_round_trips_full_unicode_and_control_characters():
    value = "é試蛇ẟΩ☁️✨🐍🧙‍♂️\n\r\f\v% ' \\"

    encoded = encode_comment(value)

    assert encoded.startswith("u8:")
    assert encoded.isascii()
    assert value not in encoded
    assert decode_comment(encoded) == value


def test_comment_codec_rejects_values_larger_than_catalog_capacity():
    with pytest.raises(ValueError, match="UTF-8 bytes"):
        encode_comment("x" * (MAX_COMMENT_UTF8_BYTES + 1))


def test_comment_catalog_creation_is_idempotent_sql():
    class Cursor:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(_normalize(statement))

    cursor = Cursor()
    ensure_comment_catalog(cursor)

    assert len(cursor.statements) == 2
    assert all("CREATE TABLE IF NOT EXISTS" in sql for sql in cursor.statements)
    assert TABLE_COMMENT_CATALOG in cursor.statements[0]
    assert COLUMN_COMMENT_CATALOG in cursor.statements[1]
    assert tuple(_normalize(sql) for sql in COMMENT_CATALOG_DDL) == tuple(
        cursor.statements
    )


def test_dialect_announces_standard_comment_capability():
    dialect = IfxDialect()
    requirements = Requirements()

    assert dialect.supports_comments is True
    assert dialect.inline_comments is False
    assert dialect.supports_constraint_comments is False
    assert requirements.comment_reflection.enabled is True
    assert requirements.comment_reflection_full_unicode.enabled is True
    assert requirements.temp_table_comment_reflection.enabled is False


def test_table_comment_compilation_uses_sidecar_merge_and_hides_unicode():
    metadata = MetaData()
    table = Table(
        quoted_name("Comment Table", True),
        metadata,
        Column(quoted_name("Value Column", True), Integer),
        schema=quoted_name("Owner Name", True),
        comment="試蛇ẟΩ✨ ' \\",
    )

    sql = _normalize(
        str(SetTableComment(table).compile(dialect=IfxDialect()))
    )

    assert sql.startswith(f"MERGE INTO {TABLE_COMMENT_CATALOG}")
    assert "COMMENT ON" not in sql
    assert "試蛇" not in sql
    assert "t.tabname = 'Comment Table'" in sql
    assert "t.owner = 'Owner Name'" in sql
    assert "u8:" in sql


def test_column_comment_compilation_and_drop_support_special_identifiers():
    metadata = MetaData()
    table = Table(
        quoted_name("table % ' one", True),
        metadata,
        Column(quoted_name("column % ' two", True), Integer, comment="áéí"),
    )
    column = next(iter(table.c))

    set_sql = _normalize(
        str(SetColumnComment(column).compile(dialect=IfxDialect()))
    )
    drop_sql = _normalize(
        str(DropColumnComment(column).compile(dialect=IfxDialect()))
    )

    assert set_sql.startswith(f"MERGE INTO {COLUMN_COMMENT_CATALOG}")
    assert "column % '' two" in set_sql
    assert "table % '' one" in set_sql
    assert "áéí" not in set_sql
    assert drop_sql.startswith(f"DELETE FROM {COLUMN_COMMENT_CATALOG}")
    assert "WHERE EXISTS" in drop_sql


def test_table_comment_drop_compiles_to_catalog_delete():
    table = Table("plain_table", MetaData(), Column("id", Integer))

    sql = _normalize(
        str(DropTableComment(table).compile(dialect=IfxDialect()))
    )

    assert sql.startswith(f"DELETE FROM {TABLE_COMMENT_CATALOG}")
    assert "plain_table" in sql


def test_reflector_decodes_individual_table_comment(monkeypatch):
    dialect = IfxDialect()
    dialect.default_schema_name = "informix"
    reflector = dialect._reflector
    encoded = encode_comment("comentario 試✨")

    monkeypatch.setattr(
        reflector,
        "_resolve_reflection_target",
        lambda connection, table_name, schema, kw: (table_name, schema, None),
    )
    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda connection, table_name, schema=None, tabtypes=None: (
            101,
            "sample",
            "informix",
            "T",
        ),
    )
    monkeypatch.setattr(
        reflector,
        "_comment_catalog_exists",
        lambda connection, catalog_name: True,
    )

    class Result:
        def first(self):
            return (encoded,)

    class Connection:
        def exec_driver_sql(self, statement, parameters=()):
            assert TABLE_COMMENT_CATALOG in statement
            assert parameters == (101, "informix", "sample")
            return Result()

    assert reflector.get_table_comment(Connection(), "sample") == {
        "text": "comentario 試✨"
    }


def test_multi_table_comment_is_batched_and_omits_missing_filters(monkeypatch):
    dialect = IfxDialect()
    dialect.default_schema_name = "informix"
    reflector = dialect._reflector

    monkeypatch.setattr(
        reflector,
        "_table_names_for_multi",
        lambda *args, **kwargs: ["commented", "plain", "a_view"],
    )
    monkeypatch.setattr(
        reflector,
        "_comment_catalog_exists",
        lambda connection, catalog_name: True,
    )

    class Result:
        def fetchall(self):
            return [
                ("commented", encode_comment("text ✨")),
                ("plain", None),
                ("a_view", None),
            ]

    class Connection:
        def __init__(self):
            self.calls = 0

        def exec_driver_sql(self, statement, parameters=()):
            self.calls += 1
            assert TABLE_COMMENT_CATALOG in statement
            assert parameters == ("informix",)
            return Result()

    connection = Connection()
    result = dict(
        reflector.get_multi_table_comment(
            connection,
            filter_names=["commented", "plain", "missing", "a_view"],
            kind=ObjectKind.ANY,
            scope=ObjectScope.DEFAULT,
        )
    )

    assert connection.calls == 1
    assert result == {
        (None, "commented"): {"text": "text ✨"},
        (None, "plain"): {"text": None},
        (None, "a_view"): {"text": None},
    }


def test_multi_table_comment_without_catalog_omits_missing_names(monkeypatch):
    dialect = IfxDialect()
    dialect.default_schema_name = "informix"
    reflector = dialect._reflector

    monkeypatch.setattr(
        reflector,
        "_table_names_for_multi",
        lambda *args, **kwargs: ["present", "missing"],
    )
    monkeypatch.setattr(
        reflector,
        "_comment_catalog_exists",
        lambda connection, catalog_name: False,
    )

    class Result:
        def fetchall(self):
            return [("present",)]

    class Connection:
        def exec_driver_sql(self, statement, parameters=()):
            assert "LEFT OUTER JOIN" not in statement
            assert parameters == ("informix",)
            return Result()

    result = dict(
        reflector.get_multi_table_comment(
            Connection(),
            filter_names=["present", "missing"],
            kind=ObjectKind.ANY,
            scope=ObjectScope.ANY,
        )
    )

    assert result == {(None, "present"): {"text": None}}


def test_comment_compiler_respects_explicit_unquoted_name_folding():
    metadata = MetaData()
    table = Table(
        quoted_name("MIXED_TABLE", False),
        metadata,
        Column(quoted_name("MIXED_COLUMN", False), Integer, comment="value"),
        schema=quoted_name("MIXED_OWNER", False),
        comment="table",
    )

    table_sql = _normalize(
        str(SetTableComment(table).compile(dialect=IfxDialect()))
    )
    column_sql = _normalize(
        str(SetColumnComment(next(iter(table.c))).compile(dialect=IfxDialect()))
    )

    assert "t.tabname = 'mixed_table'" in table_sql
    assert "LOWER(t.owner) = LOWER('MIXED_OWNER')" in table_sql
    assert "c.colname = 'mixed_column'" in column_sql


def test_comment_decoder_preserves_plain_legacy_whitespace():
    assert decode_comment("  legacy comment  ") == "  legacy comment  "
