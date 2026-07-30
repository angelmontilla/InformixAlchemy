from __future__ import annotations

import inspect

import pytest
from sqlalchemy import Integer, column, select

from IfxAlchemy.base import IfxCompiler
from IfxAlchemy.pyodbc import IfxDialect_pyodbc
from IfxAlchemy.requirements import Requirements


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


def _literal_sql(expression, dialect):
    return " ".join(
        str(
            select(expression).compile(
                dialect=dialect,
                compile_kwargs={"literal_binds": True},
            )
        ).split()
    )


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (
            lambda value: value.bitwise_and(5),
            "SELECT BITAND(bits, 5) AS anon_1 FROM "
            "sysmaster:informix.sysdual",
        ),
        (
            lambda value: value.bitwise_or(5),
            "SELECT BITOR(bits, 5) AS anon_1 FROM "
            "sysmaster:informix.sysdual",
        ),
        (
            lambda value: value.bitwise_xor(5),
            "SELECT BITXOR(bits, 5) AS anon_1 FROM "
            "sysmaster:informix.sysdual",
        ),
        (
            lambda value: value.bitwise_not(),
            "SELECT BITNOT(bits) FROM "
            "sysmaster:informix.sysdual",
        ),
        (
            lambda value: value.bitwise_lshift(5),
            "SELECT IFX_BIT_LEFTSHIFT(bits, 5) AS anon_1 FROM "
            "sysmaster:informix.sysdual",
        ),
        (
            lambda value: value.bitwise_rshift(5),
            "SELECT IFX_BIT_RIGHTSHIFT(bits, 5) AS anon_1 FROM "
            "sysmaster:informix.sysdual",
        ),
    ],
)
def test_bitwise_operators_compile_to_informix_functions(
    dialect,
    expression,
    expected,
):
    value = column("bits", Integer)

    assert _literal_sql(expression(value), dialect) == expected


def test_bitwise_compilation_preserves_nested_expression_context(dialect):
    left = column("left_value", Integer)
    right = column("right_value", Integer)
    expression = (left + 1).bitwise_xor(right - 1)

    assert _literal_sql(expression, dialect) == (
        "SELECT BITXOR((left_value + 1), (right_value - 1)) AS anon_1 "
        "FROM sysmaster:informix.sysdual"
    )


@pytest.mark.parametrize(
    "requirement_name",
    [
        "supports_bitwise_and",
        "supports_bitwise_or",
        "supports_bitwise_xor",
        "supports_bitwise_not",
        "supports_bitwise_shift",
    ],
)
def test_bitwise_suite_requirements_are_enabled(requirement_name):
    assert getattr(Requirements(), requirement_name).enabled is True


@pytest.mark.parametrize(
    "method_name",
    [
        "visit_bitwise_and_op_binary",
        "visit_bitwise_or_op_binary",
        "visit_bitwise_xor_op_binary",
        "visit_bitwise_not_op_unary_operator",
        "visit_bitwise_lshift_op_binary",
        "visit_bitwise_rshift_op_binary",
    ],
)
def test_bitwise_compiler_visitors_accept_compiler_kwargs(method_name):
    signature = inspect.signature(getattr(IfxCompiler, method_name))

    assert any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
