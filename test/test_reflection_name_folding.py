from __future__ import annotations

from sqlalchemy.sql import quoted_name

from IfxAlchemy.reflection import IfxReflector


class _FakeIdentifierPreparer:
    reserved_words = {"select", "from", "table"}

    def _requires_quotes(self, value):
        return (
            value.lower() in self.reserved_words
            or value != value.lower()
            or " " in value
        )

    def quote(self, value):
        if getattr(value, "quote", None) is True or self._requires_quotes(str(value)):
            return f'"{value}"'
        return str(value)

    def quote_schema(self, value):
        return self.quote(value)


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


class _RecordingCursor:
    def __init__(self):
        self.calls = []
        self.closed = False

    def execute(self, statement):
        self.calls.append(statement)

    def fetchone(self):
        return (1,)

    def close(self):
        self.closed = True


class _RecordingDbapiConnection:
    def __init__(self):
        self.cursor_obj = _RecordingCursor()

    def cursor(self):
        return self.cursor_obj


class _DbapiProbeConnection:
    def __init__(self):
        self.dbapi_connection = _RecordingDbapiConnection()
        self.connection = self


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


def test_has_table_sql_probe_quotes_reserved_words():
    reflector = IfxReflector(_FakeDialect())
    connection = _RecordingConnection(scalar_value=1)

    assert reflector._has_table_via_sql_probe(connection, "SELECT") is True
    assert connection.calls[0][0] == 'SELECT COUNT(*) FROM "select"'


def test_has_table_sql_probe_quotes_names_with_spaces():
    reflector = IfxReflector(_FakeDialect())
    connection = _RecordingConnection(scalar_value=1)

    assert reflector._has_table_via_sql_probe(
        connection,
        quoted_name("Table With Spaces", True),
    ) is True
    assert connection.calls[0][0] == 'SELECT COUNT(*) FROM "Table With Spaces"'


def test_has_table_sql_probe_quotes_schema_and_table_with_preparer():
    reflector = IfxReflector(_FakeDialect())
    connection = _RecordingConnection(scalar_value=1)

    assert reflector._has_table_via_sql_probe(
        connection,
        quoted_name("Order Detail", True),
        schema=quoted_name("Reporting Owner", True),
    ) is True
    assert connection.calls[0][0] == (
        'SELECT COUNT(*) FROM "Reporting Owner"."Order Detail"'
    )


def test_has_table_dbapi_probe_uses_rendered_identifier():
    reflector = IfxReflector(_FakeDialect())
    connection = _DbapiProbeConnection()

    assert reflector._has_table_via_dbapi_probe(
        connection,
        quoted_name("Order Detail", True),
    ) is True

    cursor = connection.dbapi_connection.cursor_obj
    assert cursor.calls == ['SELECT COUNT(*) FROM "Order Detail"']
    assert cursor.closed is True
