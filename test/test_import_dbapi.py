import types
import unittest
from unittest import mock

from IfxAlchemy.IfxPy import IfxDialect_IfxPy
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


class ImportDbapiCompatibilityTests(unittest.TestCase):
    def _patch_import(self, module_name):
        fake_module = types.ModuleType(module_name)
        real_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == module_name:
                return fake_module
            return real_import(name, globals, locals, fromlist, level)

        return fake_module, mock.patch("builtins.__import__", side_effect=fake_import)

    def test_ifxpy_exposes_import_dbapi_and_dbapi_alias(self):
        fake_module, patched_import = self._patch_import("IfxPyDbi")

        with patched_import:
            self.assertIs(IfxDialect_IfxPy.import_dbapi(), fake_module)
            self.assertIs(IfxDialect_IfxPy.dbapi(), fake_module)

    def test_pyodbc_exposes_import_dbapi_and_dbapi_alias(self):
        fake_module, patched_import = self._patch_import("pyodbc")

        with patched_import:
            self.assertIs(IfxDialect_pyodbc.import_dbapi(), fake_module)
            self.assertIs(IfxDialect_pyodbc.dbapi(), fake_module)


if __name__ == "__main__":
    unittest.main()
