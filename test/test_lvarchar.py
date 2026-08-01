from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Table,
    cast,
    column,
    inspect,
    insert,
    literal,
    select,
    values,
)
from sqlalchemy.engine.interfaces import ExecuteStyle
from sqlalchemy.exc import CompileError
from sqlalchemy.schema import CreateTable

from IfxAlchemy import LVARCHAR
from IfxAlchemy.base import ischema_names
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _compact_sql(statement, dialect) -> str:
    return " ".join(str(statement.compile(dialect=dialect)).split())


def _lvarchar_table(
    name: str = "ifx_lvarchar_types",
    *,
    default_length=None,
    payload_length=4096,
) -> Table:
    return Table(
        name,
        MetaData(),
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        Column("default_payload", LVARCHAR(default_length), nullable=True),
        Column("payload", LVARCHAR(payload_length), nullable=True),
    )


def test_lvarchar_is_public_and_registered_as_native_type():
    from IfxAlchemy import LVARCHAR as PublicLVARCHAR

    assert PublicLVARCHAR is LVARCHAR
    assert ischema_names["LVARCHAR"] is LVARCHAR
    assert ischema_names["LVARCHAR"] is not ischema_names["VARCHAR"]


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("length", "expected"),
    [
        (None, "payload LVARCHAR"),
        (1, "payload LVARCHAR(1)"),
        (2048, "payload LVARCHAR(2048)"),
        (32739, "payload LVARCHAR(32739)"),
    ],
)
def test_lvarchar_compiles_native_lengths(dialect, length, expected):
    table = Table(
        "ifx_lvarchar_compile",
        MetaData(),
        Column("payload", LVARCHAR(length)),
    )

    compiled = _compact_sql(CreateTable(table), dialect)

    assert expected in compiled
    assert "payload VARCHAR" not in compiled


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("invalid_length", "expected_message"),
    [
        (True, "must be an integer number of bytes"),
        (1.5, "must be an integer number of bytes"),
        ("2048", "must be an integer number of bytes"),
        (0, "must be between 1 and 32739 bytes"),
        (-1, "must be between 1 and 32739 bytes"),
        (32740, "must be between 1 and 32739 bytes"),
    ],
)
def test_lvarchar_rejects_invalid_lengths_before_ddl(
    dialect,
    invalid_length,
    expected_message,
):
    table = Table(
        "ifx_lvarchar_invalid",
        MetaData(),
        Column("payload", LVARCHAR(invalid_length)),
    )

    with pytest.raises(CompileError, match=expected_message):
        _compact_sql(CreateTable(table), dialect)


def test_lvarchar_bind_result_null_and_unicode_contract(dialect):
    datatype = LVARCHAR(2048)
    descriptor = dialect.type_descriptor(datatype)

    assert isinstance(descriptor, LVARCHAR)
    assert descriptor.length == 2048
    assert descriptor.cache_ok is True
    bind_processor = descriptor.bind_processor(dialect)
    result_processor = descriptor.result_processor(dialect, None)

    assert bind_processor is None
    assert result_processor is None

    unicode_value = "España: cañón, pingüino y acción"
    for value in (None, unicode_value):
        bound = bind_processor(value) if bind_processor else value
        received = result_processor(bound) if result_processor else bound
        assert received == value
        assert received is None or isinstance(received, str)


def test_sqlalchemy_input_size_lookup_selects_sql_varchar_for_lvarchar():
    class FakeDBAPI:
        STRING = object()
        SQL_VARCHAR = 12
        SQL_LONGVARCHAR = -1
        paramstyle = "qmark"

    dialect = IfxDialect_pyodbc(dbapi=FakeDBAPI)
    table = Table(
        "ifx_lvarchar_lookup",
        MetaData(),
        Column("payload", LVARCHAR(32739)),
    )
    compiled = insert(table).values(payload="x").compile(dialect=dialect)

    lookup = compiled._get_set_input_sizes_lookup()

    assert list(lookup.values()) == [FakeDBAPI.SQL_VARCHAR]
    bind_parameter = next(iter(lookup))
    assert isinstance(bind_parameter.type.dialect_impl(dialect), LVARCHAR)


def test_lvarchar_pyodbc_descriptor_uses_sql_varchar_not_longvarchar():
    class FakeDBAPI:
        STRING = object()
        SQL_VARCHAR = object()
        SQL_LONGVARCHAR = object()
        paramstyle = "qmark"

    dialect = IfxDialect_pyodbc(dbapi=FakeDBAPI)
    descriptor = dialect.type_descriptor(LVARCHAR(32739))

    assert isinstance(descriptor, LVARCHAR)
    assert descriptor.get_dbapi_type(FakeDBAPI) is FakeDBAPI.SQL_VARCHAR
    assert FakeDBAPI.SQL_VARCHAR in dialect.include_set_input_sizes
    assert FakeDBAPI.SQL_LONGVARCHAR in dialect.include_set_input_sizes
    assert descriptor.get_dbapi_type(FakeDBAPI) is not FakeDBAPI.SQL_LONGVARCHAR


class _RecordingCursor:
    def __init__(self):
        self.calls = []

    def setinputsizes(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def test_lvarchar_setinputsizes_uses_declared_length_and_default():
    class FakeDBAPI:
        STRING = object()
        SQL_VARCHAR = 12
        SQL_LONGVARCHAR = -1
        paramstyle = "qmark"

    dialect = IfxDialect_pyodbc(dbapi=FakeDBAPI)
    cursor = _RecordingCursor()

    dialect.do_set_input_sizes(
        cursor,
        [
            ("default_payload", FakeDBAPI.SQL_VARCHAR, LVARCHAR()),
            ("payload", FakeDBAPI.SQL_VARCHAR, LVARCHAR(32739)),
        ],
        context=SimpleNamespace(execute_style=ExecuteStyle.EXECUTE),
    )

    assert cursor.calls == [
        (
            (
                [
                    (FakeDBAPI.SQL_VARCHAR, 2048, 0),
                    (FakeDBAPI.SQL_VARCHAR, 32739, 0),
                ],
            ),
            {},
        )
    ]


def test_lvarchar_null_parameter_keeps_sql_varchar_input_size():
    class FakeDBAPI:
        STRING = object()
        SQL_VARCHAR = 12
        SQL_LONGVARCHAR = -1
        paramstyle = "qmark"

    dialect = IfxDialect_pyodbc(dbapi=FakeDBAPI)
    cursor = _RecordingCursor()

    dialect.do_set_input_sizes(
        cursor,
        [("payload", FakeDBAPI.SQL_VARCHAR, LVARCHAR(2048))],
        context=SimpleNamespace(execute_style=ExecuteStyle.EXECUTE),
    )

    assert cursor.calls[0][0][0] == [
        (FakeDBAPI.SQL_VARCHAR, 2048, 0)
    ]


@pytest.mark.parametrize(
    ("coltype", "collength", "extended_maxlen", "expected_length"),
    [
        (40, 0, None, None),
        (40, 2048, None, 2048),
        (40, 2048, 32739, 2048),
        (40 | 0x0100, 32739, None, 32739),
        (41, 0, 128, 128),
    ],
)
def test_lvarchar_catalog_and_opaque_reflection_preserve_native_type(
    dialect,
    coltype,
    collength,
    extended_maxlen,
    expected_length,
):
    reflected, autoincrement, nullable = dialect._reflector._decode_ifx_type(
        coltype,
        collength,
        extended_id=4,
        extended_type_name="lvarchar",
        extended_maxlen=extended_maxlen,
    )

    assert type(reflected) is LVARCHAR
    assert reflected.length == expected_length
    assert autoincrement is False
    assert nullable is not bool(coltype & 0x0100)


def test_lvarchar_cast_and_comparison_compile_with_native_type(dialect):
    table = _lvarchar_table(payload_length=2048)

    cast_sql = _compact_sql(
        select(cast(literal("cañón"), LVARCHAR(2048))),
        dialect,
    )
    comparison_sql = _compact_sql(
        select(table.c.id).where(table.c.payload == "cañón"),
        dialect,
    )

    assert "CAST(" in cast_sql
    assert " AS LVARCHAR(2048))" in cast_sql
    assert "param_1" in cast_sql
    assert "ifx_lvarchar_types.payload = :payload_1" in comparison_sql


def test_lvarchar_values_cte_uses_native_cast(dialect):
    rows = values(
        column("payload", LVARCHAR(2048)),
    ).data([("uno",), ("cañón",)])
    cte = rows.cte("lvarchar_values")

    compiled = select(cte.c.payload).compile(dialect=dialect)
    sql = " ".join(str(compiled).split())

    assert sql.startswith("WITH lvarchar_values(payload) AS")
    assert sql.count("CAST(") == 2
    assert "CAST(:param_1 AS LVARCHAR(2048))" in sql
    assert "CAST(:param_2 AS LVARCHAR(2048))" in sql
    assert list(compiled.params.values()) == ["uno", "cañón"]


@pytest.mark.requires_informix
def test_lvarchar_round_trip_reflection_autoload_cast_comparison_and_cte(
    engine,
    name_factory,
):
    table_name = name_factory("sa_lvarchar_")
    table = _lvarchar_table(table_name)
    unicode_value = "España, cañón y pingüino"
    long_value = "áéíóúñ" * 80

    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execute(
                insert(table),
                [
                    {
                        "id": 1,
                        "default_payload": None,
                        "payload": None,
                    },
                    {
                        "id": 2,
                        "default_payload": unicode_value,
                        "payload": unicode_value,
                    },
                    {
                        "id": 3,
                        "default_payload": "más de 255 bytes",
                        "payload": long_value,
                    },
                ],
            )

        with engine.connect() as connection:
            rows = connection.execute(
                select(
                    table.c.id,
                    table.c.default_payload,
                    table.c.payload,
                ).order_by(table.c.id)
            ).all()

            assert rows[0].default_payload is None
            assert rows[0].payload is None
            assert rows[1].default_payload == unicode_value
            assert rows[1].payload == unicode_value
            assert rows[2].payload == long_value
            assert len(rows[2].payload.encode("utf-8")) > 255
            assert all(
                value is None or isinstance(value, str)
                for row in rows
                for value in (row.default_payload, row.payload)
            )

            matching_id = connection.execute(
                select(table.c.id).where(table.c.payload == long_value)
            ).scalar_one()
            assert matching_id == 3

            cast_value = connection.execute(
                select(cast(literal(long_value), LVARCHAR(32739)))
            ).scalar_one()
            assert cast_value == long_value

            cte_rows = values(
                column("payload", LVARCHAR(2048)),
            ).data([(unicode_value,), (long_value,)])
            cte = cte_rows.cte("lvarchar_runtime")
            assert connection.execute(
                select(cte.c.payload)
            ).scalars().all() == [unicode_value, long_value]

            columns = {
                item["name"]: item
                for item in inspect(connection).get_columns(table_name)
            }
            assert type(columns["default_payload"]["type"]) is LVARCHAR
            assert columns["default_payload"]["type"].length == 2048
            assert type(columns["payload"]["type"]) is LVARCHAR
            assert columns["payload"]["type"].length == 4096

            reflected = Table(
                table_name,
                MetaData(),
                autoload_with=connection,
            )
            assert type(reflected.c.default_payload.type) is LVARCHAR
            assert reflected.c.default_payload.type.length == 2048
            assert type(reflected.c.payload.type) is LVARCHAR
            assert reflected.c.payload.type.length == 4096
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)


@pytest.mark.requires_informix
def test_lvarchar_maximum_32739_round_trip_reflection_and_autoload(
    engine,
    name_factory,
):
    """Exercise the native maximum without exceeding Informix row size.

    ``32739`` is the maximum declared size of one LVARCHAR column, not a
    budget that can be combined freely with other large in-row columns.  A
    table containing ``LVARCHAR(32739)`` plus another ``LVARCHAR`` exceeds
    Informix's 32767-byte row-size limit and is rejected with error -499.

    Keep this maximum-boundary test in a dedicated one-column table so it
    verifies the native LVARCHAR capability rather than constructing an
    invalid physical row layout.
    """

    table_name = name_factory("sa_lvarchar_max_")
    table = Table(
        table_name,
        MetaData(),
        Column("payload", LVARCHAR(32739), nullable=True),
    )
    max_value = "x" * 32739

    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execute(insert(table).values(payload=max_value))

        with engine.connect() as connection:
            received = connection.execute(
                select(table.c.payload)
            ).scalar_one()

            assert received == max_value
            assert len(received.encode("utf-8")) == 32739
            assert isinstance(received, str)

            columns = inspect(connection).get_columns(table_name)
            assert len(columns) == 1
            assert columns[0]["name"] == "payload"
            assert type(columns[0]["type"]) is LVARCHAR
            assert columns[0]["type"].length == 32739

            reflected = Table(
                table_name,
                MetaData(),
                autoload_with=connection,
            )
            assert type(reflected.c.payload.type) is LVARCHAR
            assert reflected.c.payload.type.length == 32739
            assert connection.execute(
                select(reflected.c.payload)
            ).scalar_one() == max_value
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)
