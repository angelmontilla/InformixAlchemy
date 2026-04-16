import unittest
from unittest import mock

from IfxAlchemy.reflection import IfxReflector


class _FakeIdentifierPreparer:
    def _requires_quotes(self, value):
        return False


class _FakeDialect:
    ischema_names = {}
    identifier_preparer = _FakeIdentifierPreparer()
    supports_unicode_binds = True
    default_schema_name = "informix"


class _FakeResult:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        row = self.first()
        return None if row is None else row[0]

    def fetchall(self):
        return list(self._rows)


class _RecordingConnection:
    def __init__(self, results=()):
        self._results = [_FakeResult(rows) for rows in results]
        self.calls = []

    def exec_driver_sql(self, statement, params=()):
        self.calls.append((statement, params))
        if self._results:
            return self._results.pop(0)
        return _FakeResult()


class ReflectionSelectStyleTests(unittest.TestCase):
    def setUp(self):
        self.reflector = IfxReflector(_FakeDialect())

    def test_has_table_uses_exec_driver_sql_catalog_lookup(self):
        connection = _RecordingConnection(
            results=[[(42, "employee", "informix", "T")]],
        )

        self.assertTrue(self.reflector.has_table(connection, "EMPLOYEE"))
        self.assertIn("FROM systables t", connection.calls[0][0])
        self.assertEqual(connection.calls[0][1][0], "employee")

    def test_get_schema_names_uses_exec_driver_sql(self):
        connection = _RecordingConnection(results=[[("informix",), ("reporting",)]])

        schema_names = self.reflector.get_schema_names(connection)

        self.assertEqual(schema_names, ["informix", "reporting"])
        self.assertIn("SELECT DISTINCT t.owner", connection.calls[0][0])

    def test_get_incoming_foreign_keys_groups_without_has_key(self):
        connection = _RecordingConnection(
            results=[
                [(100, "parent", "informix", "T")],
                [
                    (
                        1,
                        "FK_ORDER",
                        "INFORMIX",
                        200,
                        "FK_CHILD_PARENT",
                        "CHILD",
                        "INFORMIX",
                        "PK_PARENT",
                        "INFORMIX",
                        "PARENT",
                        "INFORMIX",
                    )
                ],
            ]
        )

        def _fake_get_index_columns(_connection, tabid, idxname, owner=None):
            if tabid == 200:
                return ["parent_id", "parent_id_2"], {}
            if tabid == 100:
                return ["id", "id_2"], {}
            raise AssertionError(f"unexpected tabid={tabid}, idxname={idxname}, owner={owner}")

        with mock.patch.object(
            self.reflector,
            "_get_index_columns",
            side_effect=_fake_get_index_columns,
        ):
            foreign_keys = self.reflector.get_incoming_foreign_keys(connection, "parent")

        self.assertEqual(len(foreign_keys), 1)
        self.assertEqual(foreign_keys[0]["name"], "fk_order")
        self.assertEqual(foreign_keys[0]["constrained_table"], "child")
        self.assertEqual(
            foreign_keys[0]["constrained_columns"],
            ["parent_id", "parent_id_2"],
        )
        self.assertEqual(foreign_keys[0]["referred_table"], "parent")
        self.assertEqual(
            foreign_keys[0]["referred_columns"],
            ["id", "id_2"],
        )
        self.assertIn("FROM sysreferences r", connection.calls[1][0])


if __name__ == "__main__":
    unittest.main()
