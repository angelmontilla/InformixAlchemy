from __future__ import annotations

import os

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import NCHAR
from sqlalchemy import NVARCHAR
from sqlalchemy import Table
from sqlalchemy import inspect
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy.exc import CompileError
from sqlalchemy.schema import CreateTable

from IfxAlchemy.base import ischema_names
from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.reflection import IfxReflector
from IfxAlchemy.requirements import Requirements


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _compact_sql(statement, dialect) -> str:
    return " ".join(
        str(statement.compile(dialect=dialect)).split()
    )


def _national_table(
    name: str = "ifx_national_types",
    *,
    nchar_length=12,
    nvarchar_length=52,
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
        Column("fixed_text", NCHAR(nchar_length)),
        Column("varying_text", NVARCHAR(nvarchar_length)),
    )


def _database_locale(connection) -> str:
    """Return the database COLLATION locale stored by Informix.

    Informix does not define ``DBINFO('db_locale')``.  The database
    catalog stores its condensed GLS locale in ``systables.site``:
    ``tabid = 90`` is the GL_COLLATE row and ``tabid = 91`` is the
    GL_CTYPE row.  NCHAR/NVARCHAR ordering is governed by GL_COLLATE,
    so the integration tests must inspect row 90 directly.
    """
    locale = connection.exec_driver_sql(
        "SELECT site FROM systables WHERE tabid = 90"
    ).scalar_one_or_none()

    if locale is None or not str(locale).strip():
        pytest.fail(
            "Informix did not expose the database GL_COLLATE locale in "
            "systables.site for tabid = 90"
        )

    return str(locale).strip()


def test_ischema_names_keep_national_types_distinct():
    assert ischema_names["NCHAR"] is NCHAR
    assert ischema_names["NVARCHAR"] is NVARCHAR
    assert ischema_names["NCHAR"] is not ischema_names["CHAR"]
    assert ischema_names["NVARCHAR"] is not ischema_names["VARCHAR"]


class _LocaleResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _LocaleConnection:
    def __init__(self, value):
        self.value = value
        self.statements = []

    def exec_driver_sql(self, statement):
        self.statements.append(" ".join(statement.split()))
        return _LocaleResult(self.value)


def test_database_locale_reads_native_gl_collate_catalog_row():
    connection = _LocaleConnection("es_ES.utf8   ")

    assert _database_locale(connection) == "es_ES.utf8"
    assert connection.statements == [
        "SELECT site FROM systables WHERE tabid = 90"
    ]


@pytest.mark.parametrize("value", [None, "", "   " ])
def test_database_locale_rejects_missing_gl_collate_catalog_value(value):
    connection = _LocaleConnection(value)

    with pytest.raises(
        pytest.fail.Exception,
        match="GL_COLLATE locale",
    ):
        _database_locale(connection)


@pytest.mark.ddl_compiler
def test_nchar_and_nvarchar_compile_with_exact_native_names(dialect):
    compiled = _compact_sql(
        CreateTable(_national_table()),
        dialect,
    )

    assert "fixed_text NCHAR(12)" in compiled
    assert "varying_text NVARCHAR(52)" in compiled
    assert "fixed_text CHAR(12)" not in compiled
    assert "varying_text VARCHAR(52)" not in compiled


@pytest.mark.ddl_compiler
@pytest.mark.parametrize("is_ansi_database", [False, True])
def test_national_type_ddl_is_identical_in_ansi_and_non_ansi_modes(
    dialect,
    is_ansi_database,
):
    dialect.is_ansi_database = is_ansi_database

    compiled = _compact_sql(
        CreateTable(_national_table()),
        dialect,
    )

    assert "fixed_text NCHAR(12)" in compiled
    assert "varying_text NVARCHAR(52)" in compiled


@pytest.mark.ddl_compiler
def test_national_types_without_length_keep_native_default_syntax(dialect):
    compiled = _compact_sql(
        CreateTable(
            _national_table(
                nchar_length=None,
                nvarchar_length=None,
            )
        ),
        dialect,
    )

    assert "fixed_text NCHAR" in compiled
    assert "fixed_text NCHAR(" not in compiled
    assert "varying_text NVARCHAR" in compiled
    assert "varying_text NVARCHAR(" not in compiled


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("type_factory", "invalid_length", "expected_message"),
    [
        (NCHAR, True, "must be an integer number of bytes"),
        (NCHAR, 1.5, "must be an integer number of bytes"),
        (NCHAR, "12", "must be an integer number of bytes"),
        (NCHAR, 0, "must be between 1 and 32767 bytes"),
        (NCHAR, -1, "must be between 1 and 32767 bytes"),
        (NCHAR, 32768, "must be between 1 and 32767 bytes"),
        (NVARCHAR, True, "must be an integer number of bytes"),
        (NVARCHAR, 1.5, "must be an integer number of bytes"),
        (NVARCHAR, "52", "must be an integer number of bytes"),
        (NVARCHAR, 0, "must be between 1 and 255 bytes"),
        (NVARCHAR, -1, "must be between 1 and 255 bytes"),
        (NVARCHAR, 256, "must be between 1 and 255 bytes"),
    ],
)
def test_national_type_lengths_are_validated_before_ddl(
    dialect,
    type_factory,
    invalid_length,
    expected_message,
):
    table = Table(
        "ifx_invalid_national_length",
        MetaData(),
        Column("value", type_factory(invalid_length)),
    )

    with pytest.raises(CompileError, match=expected_message):
        _compact_sql(CreateTable(table), dialect)


@pytest.mark.parametrize(
    ("coltype", "collength", "expected_class", "expected_length"),
    [
        (15, 52, NCHAR, 52),
        (16, 52, NVARCHAR, 52),
        (15 | 0x0100, 24, NCHAR, 24),
        (16 | 0x0100, 24, NVARCHAR, 24),
        (16, 255, NVARCHAR, 255),
    ],
)
def test_catalog_codes_reflect_exact_national_types(
    dialect,
    coltype,
    collength,
    expected_class,
    expected_length,
):
    reflected_type, autoincrement, nullable = (
        dialect._reflector._decode_ifx_type(
            coltype,
            collength,
        )
    )

    assert type(reflected_type) is expected_class
    assert reflected_type.length == expected_length
    assert autoincrement is False
    assert nullable is not bool(coltype & 0x0100)


def test_national_type_fallbacks_do_not_degrade_without_ischema_entries(
    dialect,
):
    reflector = IfxReflector(dialect)
    reflector.ischema_names = {}

    reflected_nchar, _, _ = reflector._decode_ifx_type(15, 18)
    reflected_nvarchar, _, _ = reflector._decode_ifx_type(16, 36)

    assert type(reflected_nchar) is NCHAR
    assert reflected_nchar.length == 18
    assert type(reflected_nvarchar) is NVARCHAR
    assert reflected_nvarchar.length == 36


def test_pyodbc_descriptor_preserves_national_types_and_python_str(dialect):
    for original in (NCHAR(12), NVARCHAR(52)):
        descriptor = dialect.type_descriptor(original)

        assert type(descriptor) is type(original)
        assert descriptor.length == original.length
        assert descriptor.bind_processor(dialect) is None
        assert descriptor.result_processor(dialect, None) is None

    value = "cañón ágil"
    assert isinstance(value, str)
    assert not isinstance(value, bytes)


def test_national_types_do_not_force_text_lob_setinputsizes(dialect):
    class FakeDBAPI:
        STRING = object()
        SQL_LONGVARCHAR = object()

    dialect.dbapi = FakeDBAPI
    dialect.include_set_input_sizes = {
        FakeDBAPI.SQL_LONGVARCHAR,
    }

    assert NCHAR(12).get_dbapi_type(FakeDBAPI) is FakeDBAPI.STRING
    assert NVARCHAR(52).get_dbapi_type(FakeDBAPI) is FakeDBAPI.STRING
    assert FakeDBAPI.STRING not in dialect.include_set_input_sizes


class _CursorThatMustNotReceiveInputSizes:
    def setinputsizes(self, *args, **kwargs):
        raise AssertionError(
            "NCHAR/NVARCHAR must use pyodbc str inference, not TEXT sizing"
        )


def test_setinputsizes_is_not_called_for_national_character_parameters(
    dialect,
):
    dialect.do_set_input_sizes(
        _CursorThatMustNotReceiveInputSizes(),
        [
            ("fixed_text", None, NCHAR(12)),
            ("varying_text", None, NVARCHAR(52)),
        ],
        context=None,
    )


def test_requirements_open_only_national_type_reflection_contract():
    requirements = Requirements()

    assert requirements.nvarchar_types.enabled is True
    assert requirements.unicode_data.enabled is False
    assert requirements.unicode_data_no_special_types.enabled is False


@pytest.mark.requires_informix
def test_national_types_create_insert_query_and_reflect(
    engine,
    name_factory,
):
    table_name = name_factory("sa_national_")
    table = _national_table(
        table_name,
        nchar_length=24,
        nvarchar_length=64,
    )
    fixed_value = "España"
    varying_value = "cañón ágil"

    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execute(
                insert(table),
                {
                    "id": 1,
                    "fixed_text": fixed_value,
                    "varying_text": varying_value,
                },
            )

        with engine.connect() as connection:
            row = connection.execute(
                select(
                    table.c.fixed_text,
                    table.c.varying_text,
                )
            ).one()

            assert isinstance(row.fixed_text, str)
            assert isinstance(row.varying_text, str)
            assert row.fixed_text.rstrip() == fixed_value
            assert row.varying_text == varying_value

            columns = {
                column["name"]: column
                for column in inspect(connection).get_columns(table_name)
            }

            assert type(columns["fixed_text"]["type"]) is NCHAR
            assert columns["fixed_text"]["type"].length == 24
            assert type(columns["varying_text"]["type"]) is NVARCHAR
            assert columns["varying_text"]["type"].length == 64

            reflected = Table(
                table_name,
                MetaData(),
                autoload_with=connection,
            )

            assert type(reflected.c.fixed_text.type) is NCHAR
            assert reflected.c.fixed_text.type.length == 24
            assert type(reflected.c.varying_text.type) is NVARCHAR
            assert reflected.c.varying_text.type.length == 64
            assert _database_locale(connection)
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)


@pytest.mark.requires_informix
def test_nchar_fixed_width_and_trailing_spaces_are_observable(
    engine,
    name_factory,
):
    table_name = name_factory("sa_nchar_pad_")
    table = Table(
        table_name,
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("value", NCHAR(12), nullable=False),
    )

    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execute(
                insert(table),
                {"id": 1, "value": "abc   "},
            )

        with engine.connect() as connection:
            value = connection.execute(
                select(table.c.value)
            ).scalar_one()
            octet_length = connection.exec_driver_sql(
                f"SELECT OCTET_LENGTH(value) FROM {table_name} WHERE id = 1"
            ).scalar_one()

            assert isinstance(value, str)
            assert value.rstrip() == "abc"
            assert int(octet_length) == 12
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)


@pytest.mark.requires_informix
def test_multibyte_nvarchar_round_trip_requires_utf8_database_locale(
    engine,
    name_factory,
):
    with engine.connect() as connection:
        database_locale = _database_locale(connection)

    normalized_locale = database_locale.casefold()
    if not (
        "utf8" in normalized_locale
        or normalized_locale.endswith(".57372")
    ):
        pytest.skip(
            "True multibyte verification requires a database created with "
            "an Informix UTF-8 DB_LOCALE; current locale is "
            f"{database_locale!r}"
        )

    configured_db_locale = os.getenv("DB_LOCALE")
    configured_client_locale = os.getenv("CLIENT_LOCALE")
    if not configured_db_locale or not configured_client_locale:
        pytest.fail(
            "UTF-8 national-character tests require CLIENT_LOCALE and "
            "DB_LOCALE to be set explicitly before the pyodbc connection "
            "is opened"
        )

    table_name = name_factory("sa_nvarchar_utf8_")
    table = Table(
        table_name,
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("value", NVARCHAR(96), nullable=False),
    )
    expected = "España – 東京 – Ελληνικά"

    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execute(
                insert(table),
                {"id": 1, "value": expected},
            )

        with engine.connect() as connection:
            received = connection.execute(
                select(table.c.value)
            ).scalar_one()

            assert isinstance(received, str)
            assert not isinstance(received, bytes)
            assert received == expected
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)


@pytest.mark.requires_informix
def test_nvarchar_ordering_uses_configured_database_locale(
    engine,
    name_factory,
):
    with engine.connect() as connection:
        database_locale = _database_locale(connection)

    normalized_locale = database_locale.casefold()
    if not normalized_locale.startswith("es_"):
        pytest.skip(
            "The deterministic localized-collation assertion is defined "
            "for an es_* Informix database locale; current locale is "
            f"{database_locale!r}"
        )

    table_name = name_factory("sa_nvarchar_sort_")
    table = Table(
        table_name,
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("value", NVARCHAR(12), nullable=False),
    )

    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execute(
                insert(table),
                [
                    {"id": 1, "value": "b"},
                    {"id": 2, "value": "á"},
                    {"id": 3, "value": "a"},
                ],
            )

        with engine.connect() as connection:
            ordered = list(
                connection.execute(
                    select(table.c.value).order_by(table.c.value)
                ).scalars()
            )

            assert ordered.index("a") < ordered.index("á")
            assert ordered.index("á") < ordered.index("b")
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)
