from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    cast,
    inspect,
    insert,
    literal,
    select,
)
from sqlalchemy.engine.interfaces import ExecuteStyle
from sqlalchemy.exc import ArgumentError, UnreflectableTableError
from sqlalchemy.schema import CreateTable

from IfxAlchemy import (
    CreateDistinctType,
    CreateRowType,
    DISTINCT,
    DropDistinctType,
    DropRowType,
    LIST,
    MULTISET,
    ROW,
    SET,
    RowField,
    RowValue,
    parse_complex_value,
)
from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.reflection import IfxReflector


def _compact(statement, dialect, *, literal_binds=False):
    return " ".join(
        str(
            statement.compile(
                dialect=dialect,
                compile_kwargs={"literal_binds": literal_binds},
            )
        ).split()
    )


def test_complex_types_are_public_immutable_hashable_and_cache_safe():
    element = Integer()
    list_type = LIST(element)
    same_list = LIST(Integer())
    row_type = ROW(
        (
            RowField("code", Integer(), False),
            RowField("tags", SET(String(10))),
        ),
        name="record_t",
        owner="informix",
    )
    same_row = ROW(
        (
            ("code", Integer(), False),
            ("tags", SET(String(10))),
        ),
        name="record_t",
        owner="informix",
    )

    assert list_type == same_list
    assert hash(list_type) == hash(same_list)
    assert list_type._static_cache_key == same_list._static_cache_key
    assert row_type == same_row
    assert hash(row_type) == hash(same_row)
    assert row_type._static_cache_key == same_row._static_cache_key
    assert row_type != ROW(row_type.fields)

    with pytest.raises(AttributeError, match="immutable"):
        list_type.element_type = String(20)
    with pytest.raises(AttributeError, match="immutable"):
        row_type.name = "other_t"

    with pytest.raises(ArgumentError, match="NOT NULL"):
        LIST(Integer(), element_nullable=True)



def test_collection_rejects_informix_forbidden_element_types():
    for forbidden in (Text(), LargeBinary()):
        with pytest.raises(ArgumentError, match="collection elements cannot use"):
            LIST(forbidden)


def test_sqlalchemy_copy_and_type_descriptor_preserve_complex_metadata():
    dialect = IfxDialect_pyodbc()
    datatype = LIST(
        ROW(
            (("code", Integer(), False), ("name", String(20))),
            name="item_t",
        )
    )

    copied = datatype.copy()
    adapted = dialect.type_descriptor(datatype)

    assert copied == datatype
    assert copied._static_cache_key == datatype._static_cache_key
    assert adapted == datatype
    assert adapted._static_cache_key == datatype._static_cache_key


def test_named_row_result_accepts_qualified_constructor_name():
    dialect = IfxDialect_pyodbc()
    datatype = ROW(
        (("code", Integer(), False), ("name", String(20))),
        name="item_t",
        owner="informix",
    )

    value = datatype.result_processor(dialect, None)(
        "informix.item_t(7, 'widget')"
    )

    assert value.type_name == "item_t"
    assert value.as_dict() == {"code": 7, "name": "widget"}

def test_complex_ddl_compiler_supports_nesting_and_element_not_null():
    dialect = IfxDialect_pyodbc()
    anonymous_row = ROW(
        (
            ("label", String(30), False),
            ("values", LIST(Integer())),
        )
    )
    table = Table(
        "complex_table",
        MetaData(),
        Column("ordered_values", LIST(Integer())),
        Column("unique_values", SET(String(12))),
        Column("bag_values", MULTISET(anonymous_row)),
        Column("payload", anonymous_row),
    )

    sql = _compact(CreateTable(table), dialect)

    assert "ordered_values LIST(INTEGER NOT NULL)" in sql
    assert "unique_values SET(VARCHAR(12) NOT NULL)" in sql
    assert (
        'bag_values MULTISET(ROW("label" VARCHAR(30) NOT NULL, '
        '"values" LIST(INTEGER NOT NULL)) NOT NULL)'
    ) in sql
    assert (
        'payload ROW("label" VARCHAR(30) NOT NULL, '
        '"values" LIST(INTEGER NOT NULL))'
    ) in sql


def test_named_type_ddl_constructs_compile_to_native_informix_sql():
    dialect = IfxDialect_pyodbc()
    parent = ROW((("id", Integer(), False),), name="entity_t")
    child = ROW((("name", String(40), True),), name="person_t")
    distinct = DISTINCT("code_t", String(12))

    assert _compact(CreateRowType(parent), dialect) == (
        "CREATE ROW TYPE entity_t (id INTEGER NOT NULL)"
    )
    assert _compact(CreateRowType(child, under=parent), dialect) == (
        "CREATE ROW TYPE person_t (name VARCHAR(40)) UNDER entity_t"
    )
    assert _compact(DropRowType(child, if_exists=True), dialect) == (
        "DROP ROW TYPE IF EXISTS person_t RESTRICT"
    )
    assert _compact(CreateDistinctType(distinct), dialect) == (
        "CREATE DISTINCT TYPE code_t AS VARCHAR(12)"
    )
    assert _compact(DropDistinctType(distinct, if_exists=True), dialect) == (
        "DROP TYPE IF EXISTS code_t RESTRICT"
    )


def test_distinct_rejects_native_unsupported_sources():
    with pytest.raises(ArgumentError, match="cannot use LIST"):
        DISTINCT("bad_t", LIST(Integer()))
    with pytest.raises(ArgumentError, match="anonymous ROW"):
        DISTINCT("bad_row_t", ROW((("id", Integer()),)))

    named_row = ROW((("id", Integer()),), name="named_row_t")
    datatype = DISTINCT("named_distinct_t", named_row)
    assert datatype.source_type == named_row


def test_recursive_parser_handles_strings_escapes_nulls_and_nesting():
    datatype = LIST(
        ROW(
            (
                ("text", String(80)),
                ("values", MULTISET(Integer())),
                ("flags", SET(String(10))),
                ("optional", String(20)),
            )
        )
    )
    dialect = IfxDialect_pyodbc()
    raw = (
        "LIST{ROW('a,b', MULTISET{2, 2, 1}, "
        "SET{'it''s', 'x\\'y'}, NULL), "
        "ROW('nested (value)', MULTISET{}, SET{}, 'ok')}"
    )

    value = datatype.result_processor(dialect, None)(raw)

    assert isinstance(value, list)
    assert isinstance(value[0], RowValue)
    assert value[0]["text"] == "a,b"
    assert value[0]["values"] == [2, 2, 1]
    assert value[0]["flags"] == {"it's", "x'y"}
    assert value[0]["optional"] is None
    assert value[1]["values"] == []
    assert value[1]["flags"] == set()

    parsed = parse_complex_value("ROW('one,two', LIST{1, 2}, ROW('x'))")
    assert parsed.name.upper() == "ROW"
    assert len(parsed.values) == 3


def test_collection_bind_result_semantics_empty_order_duplicates_and_set_unique():
    dialect = IfxDialect_pyodbc()
    list_type = LIST(Integer())
    multiset_type = MULTISET(Integer())
    set_type = SET(Integer())

    assert list_type.bind_processor(dialect)([]) == "LIST{}"
    assert list_type.result_processor(dialect, None)("LIST{3, 1, 3}") == [3, 1, 3]
    assert multiset_type.result_processor(dialect, None)(
        "MULTISET{2, 2, 1}"
    ) == [2, 2, 1]
    assert set_type.result_processor(dialect, None)("SET{2, 2, 1}") == {1, 2}
    assert set_type.bind_processor(dialect)([2, 2, 1]) == "SET{2, 1}"

    with pytest.raises(ValueError, match="cannot be NULL"):
        list_type.bind_processor(dialect)([1, None])
    with pytest.raises(ValueError, match="cannot contain NULL"):
        list_type.result_processor(dialect, None)("LIST{1, NULL}")


def test_row_bind_result_and_nested_collection_round_trip():
    dialect = IfxDialect_pyodbc()
    datatype = ROW(
        (
            ("id", Integer(), False),
            ("name", String(40)),
            ("numbers", LIST(Integer())),
            ("child", ROW((("active", String(1)),))),
        )
    )
    value = {
        "id": 7,
        "name": "A, B's",
        "numbers": [3, 1, 3],
        "child": {"active": "Y"},
    }

    encoded = datatype.bind_processor(dialect)(value)
    decoded = datatype.result_processor(dialect, None)(encoded)

    assert encoded == "ROW(7, 'A, B''s', LIST{3, 1, 3}, ROW('Y'))"
    assert decoded.as_dict()["id"] == 7
    assert decoded["name"] == "A, B's"
    assert decoded["numbers"] == [3, 1, 3]
    assert decoded["child"]["active"] == "Y"

    with pytest.raises(ValueError, match="NOT NULL"):
        datatype.bind_processor(dialect)(
            {"id": None, "name": "x", "numbers": [], "child": ("Y",)}
        )


def test_complex_type_adaptation_preserves_named_identity():
    dialect = IfxDialect_pyodbc()
    row_type = ROW(
        (("code", Integer(), False), ("description", String(30))),
        name="address_t",
        owner="informix",
    )
    distinct_type = DISTINCT(
        "code_t",
        String(12),
        owner="informix",
    )

    adapted_row = row_type.dialect_impl(dialect)
    adapted_distinct = distinct_type.dialect_impl(dialect)

    assert adapted_row is not row_type
    assert adapted_row.fields == row_type.fields
    assert adapted_row.name == "address_t"
    assert adapted_row.owner == "informix"
    assert adapted_distinct is not distinct_type
    assert adapted_distinct.name == "code_t"
    assert adapted_distinct.owner == "informix"
    assert adapted_distinct.source_type._static_cache_key == (
        distinct_type.source_type._static_cache_key
    )


def test_named_row_bind_cast_uses_registered_type_name():
    dialect = IfxDialect_pyodbc()
    row_type = ROW(
        (("code", Integer(), False), ("description", String(30))),
        name="address_t",
    )
    table = Table(
        "row_bind_test",
        MetaData(),
        Column("id", Integer()),
        Column("payload", row_type),
    )

    compiled = _compact(
        insert(table).values(
            id=1,
            payload={"code": 7, "description": "native"},
        ),
        dialect,
    )

    assert "CAST(" in compiled
    assert " AS address_t)" in compiled
    assert " AS ROW(code INTEGER" not in compiled


def test_complex_casts_compile_declaratively():
    dialect = IfxDialect_pyodbc()
    list_type = LIST(Integer())
    row_type = ROW((("id", Integer()), ("name", String(20))))
    distinct_type = DISTINCT("code_t", String(8))

    list_sql = _compact(
        select(cast(literal([1, 2], type_=list_type), list_type)),
        dialect,
    )
    row_sql = _compact(
        select(
            cast(
                literal({"id": 1, "name": "a"}, type_=row_type),
                row_type,
            )
        ),
        dialect,
    )
    distinct_sql = _compact(
        select(cast(literal("A1", type_=String()), distinct_type)),
        dialect,
    )

    assert "CAST(" in list_sql
    assert " AS LIST(INTEGER NOT NULL))" in list_sql
    assert "CAST(" in row_sql
    assert " AS ROW(id INTEGER, name VARCHAR(20)))" in row_sql or (
        ' AS ROW("id" INTEGER, name VARCHAR(20)))' in row_sql
    )
    assert "CAST(" in distinct_sql
    assert " AS code_t)" in distinct_sql


class _RecordingCursor:
    def __init__(self):
        self.calls = []

    def setinputsizes(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def test_pyodbc_complex_input_sizes_use_external_varchar_contract():
    class FakeDBAPI:
        STRING = object()
        NUMBER = object()
        SQL_VARCHAR = 12
        SQL_LONGVARCHAR = -1
        paramstyle = "qmark"

    dialect = IfxDialect_pyodbc(dbapi=FakeDBAPI)
    cursor = _RecordingCursor()
    row_type = ROW((("id", Integer()),))
    distinct_type = DISTINCT("code_t", String(12))

    dialect.do_set_input_sizes(
        cursor,
        [
            ("list_value", FakeDBAPI.SQL_VARCHAR, LIST(Integer())),
            ("set_value", FakeDBAPI.SQL_VARCHAR, SET(Integer())),
            ("multiset_value", FakeDBAPI.SQL_VARCHAR, MULTISET(Integer())),
            ("row_value", FakeDBAPI.SQL_VARCHAR, row_type),
            ("distinct_value", FakeDBAPI.SQL_VARCHAR, distinct_type),
        ],
        context=SimpleNamespace(execute_style=ExecuteStyle.EXECUTEMANY),
    )

    assert cursor.calls == [
        (
            (
                [
                    (FakeDBAPI.SQL_VARCHAR, 0, 0),
                    (FakeDBAPI.SQL_VARCHAR, 0, 0),
                    (FakeDBAPI.SQL_VARCHAR, 0, 0),
                    (FakeDBAPI.SQL_VARCHAR, 0, 0),
                    (FakeDBAPI.SQL_VARCHAR, 0, 0),
                ],
            ),
            {},
        )
    ]


class _RowsResult:
    def __init__(self, rows):
        self.rows = list(rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class _CatalogConnection:
    def __init__(self, metadata, attributes, named_rows=()):
        self.metadata = metadata
        self.attributes = attributes
        self.named_rows = list(named_rows)

    def exec_driver_sql(self, statement, parameters=()):
        normalized = " ".join(statement.lower().split())
        if "from sysxtdtypes x" in normalized and "where x.extended_id = ?" in normalized:
            item = self.metadata.get(int(parameters[0]))
            return _RowsResult([item] if item is not None else [])
        if "from sysattrtypes a" in normalized:
            return _RowsResult(self.attributes.get(int(parameters[0]), ()))
        if "x.mode in ('r', 'd')" in normalized:
            return _RowsResult(self.named_rows)
        raise AssertionError(f"Unexpected catalog SQL: {statement}")


def test_structured_reflection_normalizes_native_self_parent_collection_element():
    """Informix 15 can self-reference collection leaves in SYSATTRTYPES."""
    dialect = IfxDialect_pyodbc()
    dialect.default_schema_name = "informix"
    reflector = IfxReflector(dialect)
    connection = _CatalogConnection(
        metadata={
            3001: (3001, "C", "informix", "list_integer", 21, 0, 0, 0, 0),
            3002: (3002, "C", "informix", "row_with_list", 22, 0, 0, 0, 0),
        },
        attributes={
            # Native catalog shape observed on Informix 15.0: the collection
            # element points to its own seqno instead of the collection root.
            3001: (
                (1, 0, 0, None, 0, 21, 0, 0),
                (2, 1, 2, "", 0, 2 | 0x100, 4, 0),
            ),
            # The same convention must work for a collection nested in ROW.
            3002: (
                (1, 0, 0, None, 0, 22, 0, 0),
                (2, 1, 1, "numbers", 1, 21, 0, 0),
                (3, 2, 3, "", 0, 2 | 0x100, 4, 0),
            ),
        },
    )

    reflected_list = reflector._reflect_extended_type(connection, 3001)
    reflected_row = reflector._reflect_extended_type(connection, 3002)

    assert isinstance(reflected_list, LIST)
    assert isinstance(reflected_list.element_type, Integer)
    assert isinstance(reflected_row, ROW)
    assert reflected_row.fields[0].name == "numbers"
    assert isinstance(reflected_row.fields[0].type_, LIST)
    assert isinstance(reflected_row.fields[0].type_.element_type, Integer)


def test_structured_reflection_rejects_unrecoverable_attribute_hierarchy():
    dialect = IfxDialect_pyodbc()
    reflector = IfxReflector(dialect)
    connection = _CatalogConnection(
        metadata={
            3003: (3003, "C", "informix", "broken_list", 21, 0, 0, 0, 0),
        },
        attributes={
            3003: (
                (1, 0, 0, None, 0, 21, 0, 0),
                # There is no level-1 node from which level 2 can recover.
                (2, 2, 2, "", 0, 2 | 0x100, 4, 0),
            ),
        },
    )

    with pytest.raises(
        UnreflectableTableError,
        match="Unable to resolve Informix complex type parent",
    ):
        reflector._reflect_extended_type(connection, 3003)


def test_structured_reflection_reconstructs_named_row_nested_collection_and_distinct():
    dialect = IfxDialect_pyodbc()
    dialect.default_schema_name = "informix"
    reflector = IfxReflector(dialect)
    connection = _CatalogConnection(
        metadata={
            2049: (2049, "R", "informix", "address_t", 4118, 0, 0, 0, 0),
            2050: (2050, "D", "informix", "postal_code_t", 2061, 0, 12, 12, 0),
        },
        attributes={
            2049: (
                (1, 0, 0, None, 0, 4118, 0, 0),
                (2, 1, 1, "street", 1, 13 | 0x100, 40, 0),
                (3, 1, 1, "tags", 2, 21, 0, 0),
                (4, 2, 3, None, 0, 13 | 0x100, 12, 0),
            )
        },
        named_rows=(
            (2049, "R", "informix", "address_t"),
            (2050, "D", "informix", "postal_code_t"),
        ),
    )

    row_type = reflector._reflect_extended_type(connection, 2049)
    distinct_type = reflector._reflect_extended_type(connection, 2050)

    assert isinstance(row_type, ROW)
    assert row_type.name == "address_t"
    assert row_type.owner == "informix"
    assert row_type.fields[0].name == "street"
    assert row_type.fields[0].nullable is False
    assert isinstance(row_type.fields[1].type_, LIST)
    assert isinstance(row_type.fields[1].type_.element_type, String)
    assert row_type.fields[1].type_.element_type.length == 12

    assert isinstance(distinct_type, DISTINCT)
    assert distinct_type.name == "postal_code_t"
    assert isinstance(distinct_type.source_type, String)
    assert distinct_type.source_type.length == 12

    reflected = reflector.get_user_defined_types(connection)
    assert [item["kind"] for item in reflected] == ["row", "distinct"]
    assert isinstance(reflected[0]["type"], ROW)
    assert isinstance(reflected[1]["type"], DISTINCT)


@pytest.mark.requires_informix
def test_complex_types_native_declarative_round_trip_and_reflection(
    engine,
    name_factory,
):
    suffix = name_factory("cx_")[-10:]
    row_name = f"row_{suffix}"
    distinct_name = f"code_{suffix}"
    table_name = f"complex_{suffix}"

    row_type = ROW(
        (
            ("code", Integer(), False),
            ("description", String(30)),
        ),
        name=row_name,
    )
    distinct_type = DISTINCT(distinct_name, String(12))
    anonymous_row = ROW(
        (
            ("label", String(30)),
            ("numbers", LIST(Integer())),
        )
    )
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("ordered_values", LIST(Integer())),
        Column("unique_values", SET(String(12))),
        Column("bag_values", MULTISET(Integer())),
        Column("anonymous_value", anonymous_row),
        Column("named_value", row_type),
        Column("distinct_value", distinct_type),
    )

    with engine.connect() as connection:
        try:
            connection.execute(CreateRowType(row_type))
            connection.execute(CreateDistinctType(distinct_type))
            table.create(connection)
            connection.commit()

            connection.execute(
                insert(table),
                [
                    {
                        "id": 1,
                        "ordered_values": [3, 1, 3],
                        "unique_values": ["b", "a", "b"],
                        "bag_values": [2, 2, 1],
                        "anonymous_value": {
                            "label": "a,b",
                            "numbers": [5, 6],
                        },
                        "named_value": {
                            "code": 7,
                            "description": "native",
                        },
                        "distinct_value": "A1",
                    },
                    {
                        "id": 2,
                        "ordered_values": [],
                        "unique_values": [],
                        "bag_values": [],
                        "anonymous_value": {
                            "label": None,
                            "numbers": [],
                        },
                        "named_value": {
                            "code": 8,
                            "description": None,
                        },
                        "distinct_value": "B2",
                    },
                ],
            )
            connection.commit()

            first = connection.execute(
                select(table).where(table.c.id == 1)
            ).one()
            second = connection.execute(
                select(table).where(table.c.id == 2)
            ).one()

            assert first.ordered_values == [3, 1, 3]
            assert first.unique_values == {"a", "b"}
            assert sorted(first.bag_values) == [1, 2, 2]
            assert first.anonymous_value["label"] == "a,b"
            assert first.anonymous_value["numbers"] == [5, 6]
            assert first.named_value["code"] == 7
            assert first.named_value["description"] == "native"
            assert first.distinct_value == "A1"
            assert second.ordered_values == []
            assert second.unique_values == set()
            assert second.bag_values == []
            assert second.anonymous_value["label"] is None

            reflected_columns = {
                column["name"]: column["type"]
                for column in inspect(connection).get_columns(table_name)
            }
            assert isinstance(reflected_columns["ordered_values"], LIST)
            assert isinstance(reflected_columns["unique_values"], SET)
            assert isinstance(reflected_columns["bag_values"], MULTISET)
            assert isinstance(reflected_columns["anonymous_value"], ROW)
            assert isinstance(reflected_columns["named_value"], ROW)
            assert reflected_columns["named_value"].name == row_name
            assert isinstance(reflected_columns["distinct_value"], DISTINCT)
            assert reflected_columns["distinct_value"].name == distinct_name
        finally:
            try:
                table.drop(connection, checkfirst=True)
                connection.commit()
            except Exception:
                connection.rollback()
            for ddl in (
                DropDistinctType(distinct_type, if_exists=True),
                DropRowType(row_type, if_exists=True),
            ):
                try:
                    connection.execute(ddl)
                    connection.commit()
                except Exception:
                    connection.rollback()
