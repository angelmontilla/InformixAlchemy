from __future__ import annotations

import pytest

from IfxAlchemy._compiler_helpers import (
    contains_arithmetic_default,
    normalize_boolean_default,
    strip_outer_parentheses,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("(((42)))", "42"),
        ("('a''b')", "'a''b'"),
        ("(1) + 2", "(1) + 2"),
        ("('unterminated)", "('unterminated)"),
    ],
)
def test_strip_outer_parentheses(value, expected):
    assert strip_outer_parentheses(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1 + 2", True),
        ("(-1)", False),
        ("1e-5", False),
        ("'a+b'", False),
        ("CURRENT YEAR TO SECOND", False),
    ],
)
def test_contains_arithmetic_default(value, expected):
    assert contains_arithmetic_default(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("t", "true"),
        ("'FALSE'", "false"),
        ("0", "false"),
        ("CURRENT", None),
    ],
)
def test_normalize_boolean_default(value, expected):
    assert normalize_boolean_default(value) == expected
