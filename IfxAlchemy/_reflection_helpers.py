"""Side-effect-free helpers shared by Informix reflection code."""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any


def row_value(
    row: Any,
    attribute_names: Sequence[str],
    index: int,
    *,
    default: Any,
    missing: Any,
) -> Any:
    """Read an ODBC row by attribute first and positional index second."""
    for attribute_name in attribute_names:
        value = getattr(row, attribute_name, None)
        if value is not None:
            return value

    try:
        return row[index]
    except (IndexError, KeyError, TypeError):
        if default is missing:
            raise
        return default


def int_or_default(value: Any, default: Any) -> Any:
    """Convert *value* to ``int`` without hiding unrelated exceptions."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def single_or_tuple(values: Iterable[Any]) -> Any:
    """Return unique non-null values as ``None``, one value, or a tuple."""
    ordered: list[Any] = []
    for value in values:
        if value is not None and value not in ordered:
            ordered.append(value)

    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    return tuple(ordered)
