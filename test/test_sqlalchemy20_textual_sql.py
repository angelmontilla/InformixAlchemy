import unittest

from IfxAlchemy.reflection import BaseReflector


class _FakeIdentifierPreparer:
    def _requires_quotes(self, value):
        return False


class _FakeDialect:
    ischema_names = {}
    identifier_preparer = _FakeIdentifierPreparer()
    supports_unicode_binds = True
    default_schema_name = "informix"


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _ConnectionWithExecDriverSql:
    def __init__(self, value):
        self.value = value
        self.called_with = None

    def exec_driver_sql(self, statement):
        self.called_with = statement
        return _ScalarResult(self.value)


class BaseReflectorTextualSqlTests(unittest.TestCase):
    def test_get_default_schema_name_uses_exec_driver_sql(self):
        reflector = BaseReflector(_FakeDialect())
        connection = _ConnectionWithExecDriverSql(" INFORMIX ")

        schema_name = reflector._get_default_schema_name(connection)

        self.assertEqual(
            connection.called_with,
            "SELECT USER FROM systables WHERE tabid = 1",
        )
        self.assertEqual(schema_name, "informix")


if __name__ == "__main__":
    unittest.main()
