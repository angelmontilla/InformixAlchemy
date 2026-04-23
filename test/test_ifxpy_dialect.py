from types import SimpleNamespace
from unittest import mock

from IfxAlchemy.IfxPy import IfxDialect_IfxPy


def test_do_execute_omits_parameters_when_none():
    dialect = IfxDialect_IfxPy()
    cursor = mock.Mock()

    dialect.do_execute(cursor, "SELECT 1", None)

    cursor.execute.assert_called_once_with("SELECT 1")


def test_do_execute_passes_parameters_when_present():
    dialect = IfxDialect_IfxPy()
    cursor = mock.Mock()

    dialect.do_execute(cursor, "SELECT ?", (1,))

    cursor.execute.assert_called_once_with("SELECT ?", (1,))


def test_do_execute_callproc_uses_empty_list_for_none_parameters():
    dialect = IfxDialect_IfxPy()
    cursor = mock.Mock()
    context = SimpleNamespace(_out_parameters=["x_out"], _callproc_result=None)

    dialect.do_execute(cursor, "CALL foo()", None, context=context)

    cursor.callproc.assert_called_once_with("foo", [])
