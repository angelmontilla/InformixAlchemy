from __future__ import annotations

import pytest

from IfxAlchemy._reflection_helpers import int_or_default, row_value, single_or_tuple


class AttributeRow:
    column_name = "from-attribute"

    def __getitem__(self, index):
        return "from-index"


def test_row_value_prefers_named_odbc_attribute():
    missing = object()

    assert row_value(
        AttributeRow(),
        ("column_name", "COLUMN_NAME"),
        3,
        default=missing,
        missing=missing,
    ) == "from-attribute"


def test_row_value_uses_default_only_for_missing_row_data():
    missing = object()

    assert row_value(
        (),
        ("column_name",),
        3,
        default=None,
        missing=missing,
    ) is None

    with pytest.raises(IndexError):
        row_value(
            (),
            ("column_name",),
            3,
            default=missing,
            missing=missing,
        )


def test_int_or_default_handles_only_conversion_errors():
    assert int_or_default("12", None) == 12
    assert int_or_default("invalid", 7) == 7


def test_single_or_tuple_removes_nulls_and_duplicates():
    assert single_or_tuple([None, "a", "a", "b"]) == ("a", "b")
    assert single_or_tuple([None, "a"]) == "a"
    assert single_or_tuple([None]) is None
