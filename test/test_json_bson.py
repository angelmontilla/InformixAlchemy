from __future__ import annotations

import base64
import datetime as dt
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    Table,
    cast,
    insert,
    literal,
    null,
    select,
    update,
)
from sqlalchemy.engine.interfaces import ExecuteStyle
from sqlalchemy.exc import CompileError
from sqlalchemy.schema import CreateIndex, CreateTable

from IfxAlchemy import (
    BSON,
    JSON,
    bson_get,
    bson_size,
    bson_update,
    gen_bson,
)
from IfxAlchemy.base import ischema_names
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def _compact(statement, dialect, *, literal_binds=False) -> str:
    compiled = statement.compile(
        dialect=dialect,
        compile_kwargs={"literal_binds": literal_binds},
    )
    return " ".join(str(compiled).split())


def _document_table(name="ifx_documents"):
    return Table(
        name,
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("json_doc", JSON(), nullable=True),
        Column("bson_doc", BSON(), nullable=True),
    )


def test_document_types_are_public_native_and_registered():
    from IfxAlchemy import BSON as PublicBSON
    from IfxAlchemy import JSON as PublicJSON

    assert PublicJSON is JSON
    assert PublicBSON is BSON
    assert ischema_names["JSON"] is JSON
    assert ischema_names["BSON"] is BSON
    assert not issubclass(BSON, __import__("sqlalchemy").types.LargeBinary)


def test_document_ddl_compiles_to_native_opaque_types():
    dialect = IfxDialect_pyodbc()
    sql = _compact(CreateTable(_document_table()), dialect)

    assert "json_doc JSON" in sql
    assert "bson_doc BSON" in sql


def test_json_bind_result_and_native_null_contract():
    dialect = IfxDialect_pyodbc()
    datatype = JSON()
    bind = datatype.bind_processor(dialect)
    result = datatype.result_processor(dialect, None)

    value = {
        "object": {"active": True},
        "array": [1, 2.5, "España", None],
        "unicode": "cañón 日本語",
        "nullable": None,
    }
    encoded = bind(value)

    assert isinstance(encoded, str)
    assert result(encoded) == value
    assert result(None) is None
    assert bind(null()) is None

    with pytest.raises(ValueError, match="top-level JSON document object"):
        bind(JSON.NULL)
    with pytest.raises(ValueError, match="top-level JSON document object"):
        bind(None)

    sql_null_type = JSON(none_as_null=True)
    assert sql_null_type.bind_processor(dialect)(None) is None


def test_json_serializer_and_deserializer_are_dialect_configurable():
    calls = []

    def serializer(value):
        calls.append(("serialize", value))
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

    def deserializer(value):
        calls.append(("deserialize", value))
        return json.loads(value)

    dialect = IfxDialect_pyodbc(
        json_serializer=serializer,
        json_deserializer=deserializer,
    )
    datatype = JSON()
    encoded = datatype.bind_processor(dialect)({"texto": "acción"})
    decoded = datatype.result_processor(dialect, None)(encoded.encode("utf-8"))

    assert encoded == '{"texto":"acción"}'
    assert decoded == {"texto": "acción"}
    assert calls[0][0] == "serialize"
    assert calls[1] == ("deserialize", encoded)


def test_bson_json_transport_uses_configurable_encoder_decoder():
    encoded_values = []
    decoded_values = []

    def encoder(value):
        encoded_values.append(value)
        return json.dumps(value, default=str, ensure_ascii=False)

    def decoder(value):
        decoded_values.append(value)
        return json.loads(value)

    dialect = IfxDialect_pyodbc(
        bson_encoder=encoder,
        bson_decoder=decoder,
    )
    datatype = BSON()
    bind = datatype.bind_processor(dialect)
    result = datatype.result_processor(dialect, None)

    value = {
        "date": dt.date(2026, 7, 31),
        "number": 123.5,
        "unicode": "Málaga",
    }
    encoded = bind(value)

    assert isinstance(encoded, str)
    assert result(encoded.encode("utf-8")) == {
        "date": "2026-07-31",
        "number": 123.5,
        "unicode": "Málaga",
    }
    assert encoded_values == [value]
    assert decoded_values == [encoded]
    assert result(None) is None
    with pytest.raises(ValueError, match="whole-column document null"):
        bind(BSON.NULL)
    with pytest.raises(ValueError, match="whole-column document null"):
        bind(None)
    assert BSON(none_as_null=True).bind_processor(dialect)(None) is None


def test_bson_binary_transport_requires_explicit_codec_and_preserves_bytes():
    def encoder(value):
        return b"BSON" + json.dumps(value, sort_keys=True).encode("utf-8")

    def decoder(value):
        assert value.startswith(b"BSON")
        return json.loads(value[4:].decode("utf-8"))

    dialect = IfxDialect_pyodbc(
        bson_encoder=encoder,
        bson_decoder=decoder,
    )
    datatype = BSON(transport="binary")
    payload = {"binary": base64.b64encode(b"\x00\xff").decode("ascii")}
    encoded = datatype.bind_processor(dialect)(payload)

    assert isinstance(encoded, bytes)
    assert datatype.result_processor(dialect, None)(memoryview(encoded)) == payload

    no_codec = BSON(transport="binary").bind_processor(IfxDialect_pyodbc())
    assert no_codec(b"raw") == b"raw"
    with pytest.raises(TypeError, match="requires bytes-like values"):
        no_codec({"not": "encoded"})
    with pytest.raises(ValueError, match="whole-column document null"):
        no_codec(BSON.NULL)


def test_document_dbapi_input_types_follow_transport_not_storage_assumption():
    class FakeDBAPI:
        STRING = object()
        BINARY = object()
        SQL_VARCHAR = object()
        SQL_LONGVARCHAR = object()
        SQL_VARBINARY = object()
        SQL_LONGVARBINARY = object()
        paramstyle = "qmark"

    dialect = IfxDialect_pyodbc(dbapi=FakeDBAPI)

    assert JSON().get_dbapi_type(FakeDBAPI) is FakeDBAPI.SQL_VARCHAR
    assert BSON().get_dbapi_type(FakeDBAPI) is FakeDBAPI.SQL_VARCHAR
    assert (
        BSON(transport="binary").get_dbapi_type(FakeDBAPI)
        is FakeDBAPI.SQL_VARBINARY
    )
    assert FakeDBAPI.SQL_VARCHAR in dialect.include_set_input_sizes
    assert FakeDBAPI.SQL_VARBINARY in dialect.include_set_input_sizes
    assert FakeDBAPI.SQL_LONGVARCHAR in dialect.include_set_input_sizes
    assert FakeDBAPI.SQL_LONGVARBINARY in dialect.include_set_input_sizes


class _RecordingCursor:
    def __init__(self):
        self.calls = []

    def setinputsizes(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def test_document_setinputsizes_preserves_non_lob_odbc_descriptors():
    class FakeDBAPI:
        STRING = object()
        BINARY = object()
        NUMBER = object()
        SQL_VARCHAR = 12
        SQL_LONGVARCHAR = -1
        SQL_VARBINARY = -3
        SQL_LONGVARBINARY = -4
        paramstyle = "qmark"

    dialect = IfxDialect_pyodbc(dbapi=FakeDBAPI)
    cursor = _RecordingCursor()

    dialect.do_set_input_sizes(
        cursor,
        [
            ("json_doc", FakeDBAPI.SQL_VARCHAR, JSON()),
            ("bson_doc", FakeDBAPI.SQL_VARCHAR, BSON()),
            (
                "binary_bson_doc",
                FakeDBAPI.SQL_VARBINARY,
                BSON(transport="binary"),
            ),
        ],
        context=SimpleNamespace(execute_style=ExecuteStyle.EXECUTEMANY),
    )

    assert cursor.calls == [
        (
            (
                [
                    (FakeDBAPI.SQL_VARCHAR, 0, 0),
                    (FakeDBAPI.SQL_VARCHAR, 0, 0),
                    (FakeDBAPI.SQL_VARBINARY, 0, 0),
                ],
            ),
            {},
        )
    ]


def test_document_input_size_lookup_uses_varchar_not_text():
    class FakeDBAPI:
        STRING = object()
        BINARY = object()
        NUMBER = object()
        SQL_VARCHAR = 12
        SQL_LONGVARCHAR = -1
        SQL_VARBINARY = -3
        SQL_LONGVARBINARY = -4
        paramstyle = "qmark"

    dialect = IfxDialect_pyodbc(dbapi=FakeDBAPI)
    table = _document_table("ifx_document_input_lookup")
    compiled = insert(table).values(
        id=1,
        json_doc={"large": "x" * 6000},
        bson_doc={"large": "x" * 6000},
    ).compile(dialect=dialect)

    lookup = compiled._get_set_input_sizes_lookup()
    document_dbtypes = [
        dbtype
        for bind_parameter, dbtype in lookup.items()
        if isinstance(bind_parameter.type, (JSON, BSON))
    ]

    assert document_dbtypes == [
        FakeDBAPI.SQL_VARCHAR,
        FakeDBAPI.SQL_VARCHAR,
    ]


def test_text_document_transport_rejects_native_unsupported_shapes_and_size():
    dialect = IfxDialect_pyodbc()
    json_bind = JSON().bind_processor(dialect)
    bson_bind = BSON().bind_processor(dialect)

    nested_array = {"items": [1, 2, None], "nested": {"active": True}}
    assert json.loads(json_bind(nested_array)) == nested_array
    assert json.loads(bson_bind(nested_array)) == nested_array

    for value in ([1, 2], "text", 7, True):
        with pytest.raises(ValueError, match="top-level JSON document object"):
            json_bind(value)
        with pytest.raises(ValueError, match="top-level JSON document object"):
            bson_bind(value)

    oversized = {"large": "x" * (32 * 1024)}
    with pytest.raises(ValueError, match="native maximum is 32768 bytes"):
        json_bind(oversized)
    with pytest.raises(ValueError, match="native maximum is 32768 bytes"):
        bson_bind(oversized)


def test_document_bind_casts_and_literal_rendering_are_native_and_safe():
    dialect = IfxDialect_pyodbc()
    table = _document_table()
    sql = _compact(
        insert(table).values(
            id=1,
            json_doc={"name": "José"},
            bson_doc={"name": "José"},
        ),
        dialect,
        literal_binds=True,
    )

    assert "CAST('{\"name\": \"Jos\\u00e9\"}' AS JSON)" in sql
    assert (
        "CAST(CAST('{\"name\": \"Jos\\u00e9\"}' AS JSON) AS BSON)"
        in sql
    )

    binary = BSON(transport="binary")
    assert binary.literal_processor(dialect) is None

    bad_dialect = IfxDialect_pyodbc(json_serializer=lambda value: b"unsafe")
    processor = JSON().literal_processor(bad_dialect)
    with pytest.raises(CompileError, match="requires.*return str"):
        processor({"x": 1})

    json_literal = JSON().literal_processor(dialect)
    with pytest.raises(CompileError, match="top-level JSON document object"):
        json_literal(JSON.NULL)
    with pytest.raises(CompileError, match="top-level JSON document object"):
        json_literal(None)
    assert JSON(none_as_null=True).literal_processor(dialect)(None) == "NULL"

    bson_literal = BSON().literal_processor(dialect)
    with pytest.raises(CompileError, match="top-level JSON document object"):
        bson_literal(BSON.NULL)


def test_default_bson_projection_is_server_cast_to_json():
    dialect = IfxDialect_pyodbc()
    table = _document_table()

    projected = table.c.bson_doc.type.column_expression(table.c.bson_doc)

    sql = _compact(select(table.c.bson_doc), dialect)

    assert isinstance(projected.type, BSON)
    assert sql == (
        "SELECT CAST(ifx_documents.bson_doc AS JSON) AS bson_doc "
        "FROM ifx_documents"
    )


def test_binary_bson_projection_keeps_raw_driver_value():
    dialect = IfxDialect_pyodbc()
    table = Table(
        "ifx_binary_documents",
        MetaData(),
        Column("payload", BSON(transport="binary")),
    )

    sql = _compact(select(table.c.payload), dialect)

    assert "CAST(" not in sql
    assert sql == (
        "SELECT ifx_binary_documents.payload "
        "FROM ifx_binary_documents"
    )


def test_bson_get_comparison_uses_leaf_field_value_document():
    dialect = IfxDialect_pyodbc()
    table = _document_table()

    sql = _compact(
        select(table.c.id).where(
            table.c.bson_doc.get("customer.name") == {"name": "Ana"}
        ),
        dialect,
        literal_binds=True,
    )

    assert sql == (
        "SELECT ifx_documents.id FROM ifx_documents "
        "WHERE BSON_GET(ifx_documents.bson_doc, 'customer.name') = "
        "CAST(CAST('{\"name\": \"Ana\"}' AS JSON) AS BSON)"
    )


def test_bson_update_bound_document_uses_lvarchar_not_json_cast():
    dialect = IfxDialect_pyodbc(paramstyle="qmark")
    table = _document_table()

    compiled = (
        update(table)
        .where(table.c.id == 1)
        .values(
            bson_doc=table.c.bson_doc.update(
                {"$set": {"customer.name": "Ángela"}}
            )
        )
        .compile(dialect=dialect)
    )
    sql = " ".join(str(compiled).split())

    assert sql == (
        "UPDATE ifx_documents SET "
        "bson_doc=BSON_UPDATE(ifx_documents.bson_doc, ?) "
        "WHERE ifx_documents.id = ?"
    )
    assert "CAST(" not in sql

    update_parameter = compiled.params["param_1"]
    assert update_parameter == {"$set": {"customer.name": "Ángela"}}

    update_bind = compiled.binds["param_1"]
    processor = update_bind.type.bind_processor(dialect)
    assert processor(update_parameter) == (
        r'{"$set": {"customer.name": "\u00c1ngela"}}'
    )


def test_bson_functions_and_casts_compile_only_verified_operations():
    dialect = IfxDialect_pyodbc()
    table = _document_table()

    get_sql = _compact(
        select(bson_get(table.c.bson_doc, "customer.name").as_json()),
        dialect,
        literal_binds=True,
    )
    renamed_sql = _compact(
        select(table.c.bson_doc.get("name", "display_name").as_json()),
        dialect,
        literal_binds=True,
    )
    update_sql = _compact(
        select(
            bson_update(
                table.c.bson_doc,
                {"$set": {"active": True}},
            ).as_json()
        ),
        dialect,
        literal_binds=True,
    )
    total_size_sql = _compact(
        select(bson_size(table.c.bson_doc)),
        dialect,
        literal_binds=True,
    )
    size_sql = _compact(
        select(bson_size(table.c.bson_doc, "items")),
        dialect,
        literal_binds=True,
    )
    gen_sql = _compact(
        select(gen_bson(table.table_valued(), keep_nulls=True, skip_id=True)),
        dialect,
        literal_binds=True,
    )
    casts_sql = _compact(
        select(
            cast(table.c.bson_doc, JSON()),
            cast(table.c.json_doc, BSON()),
        ),
        dialect,
    )

    assert "CAST(BSON_GET(ifx_documents.bson_doc, 'customer.name') AS JSON)" in get_sql
    assert "BSON_GET(ifx_documents.bson_doc, 'name', 'display_name')" in renamed_sql
    assert (
        "BSON_UPDATE(ifx_documents.bson_doc, "
        "'{\"$set\": {\"active\": true}}')"
        in update_sql
    )
    assert "BSON_UPDATE(ifx_documents.bson_doc, CAST(" not in update_sql
    assert "BSON_SIZE(ifx_documents.bson_doc, '')" in total_size_sql
    assert "BSON_SIZE(ifx_documents.bson_doc, 'items')" in size_sql
    assert "genBSON(ifx_documents, 1, 1)" in gen_sql
    assert "CAST(ifx_documents.bson_doc AS JSON)" in casts_sql
    assert "CAST(ifx_documents.json_doc AS BSON)" in casts_sql


def test_bson_field_index_compiles_using_bson_access_method():
    dialect = IfxDialect_pyodbc()
    table = _document_table()
    index = Index(
        "ix_documents_customer_name",
        table.c.bson_doc.get("customer.name"),
        informix_functional=True,
        informix_access_method="BSON",
    )

    sql = _compact(
        CreateIndex(index),
        dialect,
        literal_binds=True,
    )
    assert sql == (
        "CREATE INDEX ix_documents_customer_name ON ifx_documents "
        "(BSON_GET(bson_doc, 'customer.name')) USING BSON"
    )


def test_bson_field_index_rejects_missing_access_method():
    dialect = IfxDialect_pyodbc()
    table = _document_table()
    index = Index(
        "ix_documents_customer_name",
        table.c.bson_doc.get("customer.name"),
        informix_functional=True,
    )

    with pytest.raises(CompileError, match="require.*access_method='BSON'"):
        CreateIndex(index).compile(dialect=dialect)


def test_json_bson_opaque_reflection_without_database():
    dialect = IfxDialect_pyodbc()
    reflector = dialect._reflector

    reflected_json, autoincrement, nullable = reflector._decode_ifx_type(
        41,
        0,
        extended_id=101,
        extended_type_name="JSON",
        extended_maxlen=32768,
    )
    reflected_bson, bson_autoincrement, bson_nullable = reflector._decode_ifx_type(
        41 | 0x0100,
        0,
        extended_id=102,
        extended_type_name="bson",
        extended_maxlen=32768,
    )

    assert type(reflected_json) is JSON
    assert autoincrement is False
    assert nullable is True
    assert type(reflected_bson) is BSON
    assert bson_autoincrement is False
    assert bson_nullable is False


@pytest.mark.requires_informix
def test_json_bson_native_round_trip_query_update_index_and_large_document(
    engine,
    name_factory,
):
    table_name = name_factory("sa_docs_")
    index_name = name_factory("ix_docs_")
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("json_doc", JSON(), nullable=True),
        Column("bson_doc", BSON(), nullable=True),
    )
    index = Index(
        index_name,
        table.c.bson_doc.get("customer.name"),
        informix_functional=True,
        informix_access_method="BSON",
    )

    metadata.create_all(engine)
    try:
        # Greater than 4 KiB so Informix uses the configured sbspace, but
        # below the native 32 KiB document limit.
        large_text = "x" * 6000
        document = {
            "customer": {"name": "Ana"},
            "items": [1, 2, 3, None],
            "unicode": "España 日本語",
            "nullable": None,
            "large": large_text,
        }
        rows = [
            {"id": 1, "json_doc": document, "bson_doc": document},
            {"id": 2, "json_doc": null(), "bson_doc": null()},
        ]

        with engine.begin() as connection:
            connection.execute(insert(table), rows)

            json_value, bson_value = connection.execute(
                select(table.c.json_doc, cast(table.c.bson_doc, JSON())).where(
                    table.c.id == 1
                )
            ).one()
            assert json_value["customer"]["name"] == "Ana"
            assert bson_value["items"] == [1, 2, 3, None]
            assert json_value["nullable"] is None
            assert bson_value["nullable"] is None
            assert json_value["large"] == large_text

            sql_null_row = connection.execute(
                select(table.c.json_doc, table.c.bson_doc).where(table.c.id == 2)
            ).one()
            assert sql_null_row == (None, None)

            # BSON_GET returns a field-value BSON document.  For the
            # multilevel path "customer.name", Informix names the returned
            # field with the leaf component: {"name": "Ana"}.
            matched = connection.execute(
                select(table.c.id).where(
                    table.c.bson_doc.get("customer.name")
                    == {"name": "Ana"}
                )
            ).scalar_one()
            assert matched == 1

            connection.execute(
                update(table)
                .where(table.c.id == 1)
                .values(
                    bson_doc=table.c.bson_doc.update(
                        {"$set": {"customer.name": "Ángela"}}
                    )
                )
            )
            updated = connection.execute(
                select(cast(table.c.bson_doc, JSON())).where(table.c.id == 1)
            ).scalar_one()
            assert updated["customer"]["name"] == "Ángela"
            assert connection.execute(
                select(table.c.bson_doc.size()).where(table.c.id == 1)
            ).scalar_one() > 4096
    finally:
        metadata.drop_all(engine)
