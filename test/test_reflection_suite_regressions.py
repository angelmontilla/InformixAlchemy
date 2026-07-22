from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import exc

from IfxAlchemy.reflection import IfxReflector


class _Dialect:
    ischema_names = {}
    default_schema_name = "informix"
    identifier_preparer = SimpleNamespace(
        _requires_quotes=lambda value: False,
    )


def test_table_options_contract_is_not_implemented():
    reflector = IfxReflector(_Dialect())

    with pytest.raises(NotImplementedError, match="table-option reflection"):
        reflector.get_table_options(object(), "users")


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

    def exec_driver_sql(self, statement, params=()):
        self.calls += 1
        return _Result(self.first_row)


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
