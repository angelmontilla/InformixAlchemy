from __future__ import annotations

import datetime

import pytest

from IfxAlchemy.temporal import format_ifx_datetime
from IfxAlchemy.temporal import format_ifx_time
from IfxAlchemy.temporal import quantize_temporal_value


@pytest.mark.parametrize(
    ("microsecond", "expected"),
    [
        (0, 0),
        (1, 0),
        (9, 0),
        (10, 10),
        (396, 390),
        (123456, 123450),
        (999999, 999990),
    ],
)
def test_fraction5_truncation_is_deterministic(
    microsecond,
    expected,
):
    value = datetime.time(
        12,
        57,
        18,
        microsecond,
    )

    result = quantize_temporal_value(
        value,
        fraction_digits=5,
    )

    assert result.microsecond == expected


def test_fraction5_quantization_is_idempotent():
    original = datetime.time(
        12,
        57,
        18,
        123456,
    )

    first = quantize_temporal_value(
        original,
        fraction_digits=5,
    )

    second = quantize_temporal_value(
        first,
        fraction_digits=5,
    )

    third = quantize_temporal_value(
        second,
        fraction_digits=5,
    )

    assert first == datetime.time(
        12,
        57,
        18,
        123450,
    )

    assert second == first
    assert third == first


def test_fraction5_does_not_roll_into_next_second():
    original = datetime.time(
        12,
        59,
        59,
        999999,
    )

    result = quantize_temporal_value(
        original,
        fraction_digits=5,
    )

    assert result == datetime.time(
        12,
        59,
        59,
        999990,
    )


def test_time_canonical_format():
    value = datetime.time(
        12,
        57,
        18,
        123456,
    )

    assert format_ifx_time(
        value,
        fraction_digits=5,
    ) == "12:57:18.12345"


def test_datetime_canonical_format():
    value = datetime.datetime(
        2026,
        7,
        20,
        12,
        57,
        18,
        123456,
    )

    assert format_ifx_datetime(
        value,
        fraction_digits=5,
    ) == "2026-07-20 12:57:18.12345"
