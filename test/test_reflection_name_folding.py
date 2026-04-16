from __future__ import annotations

from sqlalchemy.sql import quoted_name

from IfxAlchemy.reflection import IfxReflector


class _FakeIdentifierPreparer:
    def _requires_quotes(self, value):
        return False

    def quote(self, value):
        return f'"{value}"'

    def quote_schema(self, value):
        return f'"{value}"'


class _FakeDialect:
    ischema_names = {}
    identifier_preparer = _FakeIdentifierPreparer()
    supports_unicode_binds = True
    default_schema_name = "ctl"


class _FakeResult:
    def __init__(self, first_row=None, scalar_value=None):
        self._first_row = first_row
        self._scalar_value = scalar_value

    def first(self):
        return self._first_row

    def scalar(self):
        return self._scalar_value


class _RecordingConnection:
    def __init__(self, *, first_row=None, scalar_value=1):
        self.first_row = first_row
        self.scalar_value = scalar_value
        self.calls = []

    def exec_driver_sql(self, statement, params=()):
        self.calls.append((statement, params))
        return _FakeResult(first_row=self.first_row, scalar_value=self.scalar_value)


def test_get_table_row_folds_unquoted_names_to_lowercase_catalog_form():
    reflector = IfxReflector(_FakeDialect())
    connection = _RecordingConnection(first_row=(1, "sa_norm_demo", "ctl", "T"))

    row = reflector._get_table_row(connection, "SA_NORM_DEMO")

    assert row == (1, "sa_norm_demo", "ctl", "T")
    assert connection.calls[0][1][0] == "sa_norm_demo"


def test_get_table_row_keeps_quoted_name_exact():
    reflector = IfxReflector(_FakeDialect())
    connection = _RecordingConnection(first_row=(1, "MixCaseDemo", "ctl", "T"))

    row = reflector._get_table_row(connection, quoted_name("MixCaseDemo", True))

    assert row == (1, "MixCaseDemo", "ctl", "T")
    assert connection.calls[0][1][0] == "MixCaseDemo"


def test_has_table_sql_probe_uses_lowercase_unquoted_token():
    reflector = IfxReflector(_FakeDialect())
    connection = _RecordingConnection(scalar_value=1)

    assert reflector._has_table_via_sql_probe(connection, "SA_NORM_DEMO") is True
    assert connection.calls[0][0] == "SELECT COUNT(*) FROM sa_norm_demo"


def test_has_table_sql_probe_quotes_explicitly_quoted_names():
    reflector = IfxReflector(_FakeDialect())
    connection = _RecordingConnection(scalar_value=1)

    assert reflector._has_table_via_sql_probe(
        connection,
        quoted_name("MixCaseDemo", True),
    ) is True
    assert connection.calls[0][0] == 'SELECT COUNT(*) FROM "MixCaseDemo"'
