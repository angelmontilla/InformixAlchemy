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
