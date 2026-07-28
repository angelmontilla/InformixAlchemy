from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import exc

from IfxAlchemy import provision


class _Result:
    def __init__(
        self,
        *,
        rows=(),
        scalar=None,
        first_row=None,
    ):
        self._rows = list(rows)
        self._scalar = scalar
        self._first_row = first_row

    def fetchall(self):
        return list(self._rows)

    def scalar_one(self):
        return self._scalar

    def first(self):
        return self._first_row


class _Preparer:
    @staticmethod
    def quote_identifier(name):
        escaped = str(name).replace('"', '""')
        return f'"{escaped}"'


class _Connection:
    def __init__(
        self,
        *,
        default_schema_name="informix",
        rows_by_owner=None,
        default_owner="informix",
        execute_error=None,
    ):
        self.dialect = SimpleNamespace(
            default_schema_name=default_schema_name,
            identifier_preparer=_Preparer(),
        )
        self.rows_by_owner = rows_by_owner or {}
        self.default_owner = default_owner
        self.execute_error = execute_error
        self.calls = []
        self.statements = []

    def exec_driver_sql(self, statement, parameters=()):
        self.calls.append((statement, parameters))
        normalized = " ".join(statement.split()).upper()

        if normalized.startswith("SELECT USER FROM SYSTABLES"):
            return _Result(scalar=self.default_owner)

        if normalized.startswith("SELECT T.OWNER"):
            owner = parameters[0]
            return _Result(
                rows=self.rows_by_owner.get(
                    str(owner).casefold(),
                    (),
                )
            )

        if normalized.startswith("DROP "):
            if self.execute_error is not None:
                raise self.execute_error
            self.statements.append(statement)
            return _Result()

        raise AssertionError(f"Unexpected SQL in unit test: {statement}")


class _Engine:
    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def begin(self):
        yield self.connection


def _cfg(test_schema="test_schema", test_schema_2="test_schema_2"):
    return SimpleNamespace(
        test_schema=test_schema,
        test_schema_2=test_schema_2,
    )


def test_suite_object_owners_contains_default_and_test_owners_once():
    connection = _Connection(default_schema_name="Informix")

    owners = provision._suite_object_owners(
        _cfg("TEST_SCHEMA", "test_schema_2"),
        connection,
    )

    assert owners == (
        "Informix",
        "TEST_SCHEMA",
        "test_schema_2",
    )


def test_suite_object_owners_queries_user_when_default_schema_is_missing():
    connection = _Connection(
        default_schema_name=None,
        default_owner="informix",
    )

    owners = provision._suite_object_owners(
        _cfg(),
        connection,
    )

    assert owners == (
        "informix",
        "test_schema",
        "test_schema_2",
    )
    assert any(
        "SELECT USER FROM systables" in statement
        for statement, _ in connection.calls
    )


def test_catalog_objects_filters_catalog_rows_and_unrequested_types():
    connection = _Connection(
        rows_by_owner={
            "test_schema": [
                (" test_schema ", " users ", " T ", 101),
                ("test_schema", "vv", "V", 102),
                ("test_schema", "seq", "Q", 103),
                ("test_schema", "systables", "T", 1),
                (None, "invalid", "T", 104),
            ],
        }
    )

    objects = provision._catalog_objects(
        connection,
        ("test_schema",),
        ("V", "T"),
    )

    assert objects == [
        {
            "owner": "test_schema",
            "name": "users",
            "type": "T",
            "tabid": 101,
        },
        {
            "owner": "test_schema",
            "name": "vv",
            "type": "V",
            "tabid": 102,
        },
    ]

    statement, parameters = connection.calls[0]
    assert "t.tabid >= 100" in statement
    assert "t.tabtype IN (?, ?)" in statement
    assert parameters == (
        "test_schema",
        "V",
        "T",
    )


def test_pre_tables_hook_drops_all_views_before_tables(monkeypatch):
    connection = _Connection()
    engine = _Engine(connection)

    monkeypatch.setattr(
        provision,
        "_suite_object_owners",
        lambda cfg, conn: (
            "informix",
            "test_schema",
            "test_schema_2",
        ),
    )
    monkeypatch.setattr(
        provision,
        "_catalog_objects",
        lambda conn, owners, tabtypes: [
            {
                "owner": "test_schema",
                "name": "users",
                "type": "T",
                "tabid": 110,
            },
            {
                "owner": "test_schema",
                "name": "vv",
                "type": "V",
                "tabid": 111,
            },
            {
                "owner": "informix",
                "name": "some_table",
                "type": "T",
                "tabid": 112,
            },
        ],
    )

    hook = (
        provision
        ._informix_drop_all_schema_objects_pre_tables
        .fns["informix"]
    )
    hook(_cfg(), engine)

    assert connection.statements == [
        'DROP VIEW "test_schema"."vv"',
        'DROP TABLE "test_schema"."users" CASCADE',
        'DROP TABLE "informix"."some_table" CASCADE',
    ]


def test_post_tables_hook_drops_qualified_sequences(monkeypatch):
    connection = _Connection()
    engine = _Engine(connection)

    monkeypatch.setattr(
        provision,
        "_suite_object_owners",
        lambda cfg, conn: (
            "informix",
            "test_schema",
            "test_schema_2",
        ),
    )
    monkeypatch.setattr(
        provision,
        "_catalog_objects",
        lambda conn, owners, tabtypes: [
            {
                "owner": "test_schema",
                "name": "user_id_seq",
                "type": "Q",
                "tabid": 120,
            },
            {
                "owner": "test_schema_2",
                "name": "other_seq",
                "type": "Q",
                "tabid": 121,
            },
        ],
    )

    hook = (
        provision
        ._informix_drop_all_schema_objects_post_tables
        .fns["informix"]
    )
    hook(_cfg(), engine)

    assert connection.statements == [
        'DROP SEQUENCE "test_schema"."user_id_seq"',
        'DROP SEQUENCE "test_schema_2"."other_seq"',
    ]


def test_drop_catalog_object_wraps_dbapi_error_with_object_identity():
    original = exc.ProgrammingError(
        "DROP TABLE",
        (),
        RuntimeError("driver failure"),
    )
    connection = _Connection(execute_error=original)

    with pytest.raises(
        RuntimeError,
        match=r'table "test_schema"\."users"',
    ) as captured:
        provision._drop_catalog_object(
            connection,
            {
                "owner": "test_schema",
                "name": "users",
                "type": "T",
                "tabid": 101,
            },
        )

    assert captured.value.__cause__ is original


def test_drop_catalog_object_rejects_unknown_catalog_type():
    connection = _Connection()

    with pytest.raises(
        RuntimeError,
        match="Unsupported Informix catalog object type",
    ):
        provision._drop_catalog_object(
            connection,
            {
                "owner": "informix",
                "name": "unknown_object",
                "type": "X",
                "tabid": 999,
            },
        )
