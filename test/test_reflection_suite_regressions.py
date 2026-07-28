from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import exc

from IfxAlchemy.reflection import IfxReflector

from sqlalchemy.sql import quoted_name


class _Dialect:
    ischema_names = {}
    default_schema_name = "informix"
    identifier_preparer = SimpleNamespace(
        _requires_quotes=lambda value: False,
    )


def test_missing_view_definition_raises_no_such_table(monkeypatch):
    reflector = IfxReflector(_Dialect())
    monkeypatch.setattr(
        reflector,
        "_get_table_row",
        lambda *args, **kw: None,
    )

    with pytest.raises(exc.NoSuchTableError):
        reflector.get_view_definition(object(), "missing_view")


class _Result:
    def __init__(self, first_row=None):
        self.first_row = first_row

    def first(self):
        return self.first_row


class _Connection:
    def __init__(self, first_row=None):
        self.first_row = first_row
        self.calls = 0
        self.statement = None
        self.params = None

    def exec_driver_sql(self, statement, params=()):
        self.calls += 1
        self.statement = statement
        self.params = params
        return _Result(self.first_row)


def test_table_options_reflect_native_systables_values(monkeypatch):
    reflector = IfxReflector(_Dialect())
    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda *args, **kw: (42, "users", "informix", "T"),
    )
    connection = _Connection(("T", "P", 16, 32, 4096))

    reflected = reflector.get_table_options(connection, "users")

    assert reflected == {
        "informix_lock_level": "PAGE",
        "informix_first_extent": 16,
        "informix_next_extent": 32,
        "informix_page_size": 4096,
    }
    assert connection.params == (42,)
    assert "t.locklevel" in connection.statement
    assert "t.fextsize" in connection.statement
    assert "t.nextsize" in connection.statement
    assert "t.pagesize" in connection.statement


def test_view_table_options_are_empty(monkeypatch):
    reflector = IfxReflector(_Dialect())
    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda *args, **kw: (84, "users_v", "informix", "V"),
    )
    connection = _Connection(("V", "B", 0, 0, 0))

    assert reflector.get_table_options(connection, "users_v") == {}


def test_table_options_omit_nonpositive_catalog_values(monkeypatch):
    reflector = IfxReflector(_Dialect())
    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda *args, **kw: (7, "small_table", "informix", "T"),
    )
    connection = _Connection(("T", "R", 0, None, -1))

    assert reflector.get_table_options(connection, "small_table") == {
        "informix_lock_level": "ROW",
    }


def test_has_sequence_uses_info_cache():
    reflector = IfxReflector(_Dialect())
    connection = _Connection(first_row=(1,))
    info_cache = {}

    assert reflector.has_sequence(
        connection,
        "seq_one",
        info_cache=info_cache,
    ) is True

    connection.first_row = None

    assert reflector.has_sequence(
        connection,
        "seq_one",
        info_cache=info_cache,
    ) is True
    assert connection.calls == 1

    info_cache.clear()

    assert reflector.has_sequence(
        connection,
        "seq_one",
        info_cache=info_cache,
    ) is False
    assert connection.calls == 2


def test_lowercase_catalog_name_is_forced_quoted_name():
    reflector = IfxReflector(_Dialect())

    name = reflector.normalize_name("t1")

    assert isinstance(name, quoted_name)
    assert name.quote is True
    assert name.upper() == name.lower() == "t1"


class _RowsResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class _RowsConnection:
    def __init__(self, rows):
        self.rows = rows
        self.statement = None
        self.params = None

    def exec_driver_sql(self, statement, params=()):
        self.statement = statement
        self.params = params
        return _RowsResult(self.rows)


def test_check_constraint_reflection_reassembles_catalog_chunks(
    monkeypatch,
):
    reflector = IfxReflector(_Dialect())

    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda *args, **kw: (
            42,
            "users",
            "test_schema",
            "T",
        ),
    )

    connection = _RowsConnection(
        [
            (
                10,
                "test_schema__ck_positive",
                0,
                "amount > 0 AND amount < 1000 ",
            ),
            (
                10,
                "test_schema__ck_positive",
                1,
                "OR amount = 2000                ",
            ),
            (
                11,
                "test_schema__ck_other",
                0,
                "other <> amount                  ",
            ),
        ]
    )

    reflected = reflector.get_check_constraints(
        connection,
        "users",
        schema="test_schema",
    )

    assert reflected == [
        {
            "name": "ck_positive",
            "sqltext": (
                "amount > 0 AND amount < 1000 "
                "OR amount = 2000"
            ),
        },
        {
            "name": "ck_other",
            "sqltext": "other <> amount",
        },
    ]

    assert connection.params == (42,)

    assert (
        "UPPER(TRIM(ch.type)) = 'T'"
        in connection.statement
    )


def test_foreign_key_reflection_omits_default_restrict_rule(monkeypatch):
    reflector = IfxReflector(_Dialect())

    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda *args, **kwargs: (
            7,
            "ifx_default_child",
            "informix",
            "T",
        ),
    )

    def get_index_columns(
        connection,
        tabid,
        idxname,
        owner=None,
    ):
        _ = (connection, idxname, owner)
        if tabid == 7:
            return ["parent_id"], {}
        if tabid == 42:
            return ["id"], {}
        raise AssertionError(f"unexpected tabid: {tabid}")

    monkeypatch.setattr(
        reflector,
        "_get_index_columns",
        get_index_columns,
    )

    connection = _RowsConnection(
        [
            (
                11,
                "fk_ifx_default_parent",
                "informix",
                "fk_default_idx",
                21,
                42,
                "R",
                "pk_ifx_default_parent",
                "informix",
                "pk_default_idx",
                "ifx_default_parent",
                "informix",
            )
        ]
    )

    reflected = reflector.get_foreign_keys(
        connection,
        "ifx_default_child",
    )

    assert reflected == [
        {
            "name": "fk_ifx_default_parent",
            "constrained_columns": ["parent_id"],
            "referred_schema": None,
            "referred_table": "ifx_default_parent",
            "referred_columns": ["id"],
            "options": {},
        }
    ]


def test_check_catalog_text_removes_space_before_parentheses():
    reflector = IfxReflector(_Dialect())

    reflected = reflector._normalize_check_catalog_text(
        "((address_id > 0 ) AND "
        "(address_id < 1000 ) )"
    )

    assert reflected == (
        "((address_id > 0) AND "
        "(address_id < 1000))"
    )


def test_check_catalog_text_preserves_quoted_content():
    reflector = IfxReflector(_Dialect())

    reflected = reflector._normalize_check_catalog_text(
        "((note = 'text ) and it''s valid' ) AND "
        '("odd ) name" > 0 ) )'
    )

    assert reflected == (
        "((note = 'text ) and it''s valid') AND "
        '("odd ) name" > 0))'
    )


def test_check_constraints_are_sorted_by_expression():
    reflector = IfxReflector(_Dialect())

    constraints = [
        {
            "name": "zz_test2_gt_zero",
            "sqltext": "(test2 > 0)",
        },
        {
            "name": "c1234_5678",
            "sqltext": "(test2 <= 1000)",
        },
    ]

    constraints.sort(
        key=reflector._check_constraint_sort_key
    )

    assert constraints == [
        {
            "name": "c1234_5678",
            "sqltext": "(test2 <= 1000)",
        },
        {
            "name": "zz_test2_gt_zero",
            "sqltext": "(test2 > 0)",
        },
    ]


def test_index_catalog_parts_preserve_declared_column_order():
    reflector = IfxReflector(_Dialect())

    colnos, descending = reflector._extract_index_colnos(
        (3, 1, 2, 0, None, -4)
    )

    assert colnos == [3, 1, 2, 4]
    assert descending == {4: True}


def test_get_index_columns_returns_catalog_key_order(monkeypatch):
    reflector = IfxReflector(_Dialect())

    monkeypatch.setattr(
        reflector,
        "_get_index_parts_row",
        lambda *args, **kwargs: (
            "Index_Example",
            "informix",
            "D",
            3,
            1,
            2,
            0,
        ),
    )
    monkeypatch.setattr(
        reflector,
        "_get_column_name_map",
        lambda *args, **kwargs: {
            1: "Column1",
            2: "Column2",
            3: "Column3",
        },
    )

    column_names, column_sorting = reflector._get_index_columns(
        object(),
        42,
        "Index_Example",
        owner="informix",
    )

    assert column_names == ["Column3", "Column1", "Column2"]
    assert column_sorting == {}


def test_foreign_key_reflection_reports_ondelete_cascade(monkeypatch):
    reflector = IfxReflector(_Dialect())

    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda *args, **kwargs: (
            7,
            "ifx_cascade_child",
            "informix",
            "T",
        ),
    )

    def get_index_columns(
        connection,
        tabid,
        idxname,
        owner=None,
    ):
        _ = (connection, idxname, owner)
        if tabid == 7:
            return ["parent_id"], {}
        if tabid == 42:
            return ["id"], {}
        raise AssertionError(f"unexpected tabid: {tabid}")

    monkeypatch.setattr(
        reflector,
        "_get_index_columns",
        get_index_columns,
    )

    connection = _RowsConnection(
        [
            (
                10,
                "fk_ifx_cascade_parent",
                "informix",
                "fk_cascade_idx",
                20,
                42,
                "C",
                "pk_ifx_cascade_parent",
                "informix",
                "pk_cascade_idx",
                "ifx_cascade_parent",
                "informix",
            )
        ]
    )

    reflected = reflector.get_foreign_keys(
        connection,
        "ifx_cascade_child",
    )

    assert reflected == [
        {
            "name": "fk_ifx_cascade_parent",
            "constrained_columns": ["parent_id"],
            "referred_schema": None,
            "referred_table": "ifx_cascade_parent",
            "referred_columns": ["id"],
            "options": {"ondelete": "CASCADE"},
        }
    ]
    assert connection.params == (7,)
    assert "r.delrule" in connection.statement
