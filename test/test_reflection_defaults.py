import pytest

from IfxAlchemy.reflection import IfxReflector


@pytest.fixture
def reflector():
    return object.__new__(IfxReflector)


@pytest.mark.parametrize(
    ("raw_value", "base_code", "expected"),
    [
        ("gAAAAAAAAAAA 0.000", 5, "0.000"),
        ("encoded 123", 2, "123"),
        ("encoded -123.45", 5, "-123.45"),
        ("encoded 2026-07-16", 7, "2026-07-16"),
        ("encoded 2026-07-16 12:34:56", 10, "2026-07-16 12:34:56"),
        ("0.000", 5, "0.000"),
    ],
)
def test_decode_encoded_literal_default(
    reflector,
    raw_value,
    base_code,
    expected,
):
    result = reflector._decode_literal_default(raw_value, base_code)

    assert result == expected


@pytest.mark.parametrize(
    ("raw_value", "base_code"),
    [
        ("HELLO WORLD", 0),
        ("HELLO WORLD", 13),
        ("HELLO WORLD", 15),
        ("HELLO WORLD", 16),
        ("HELLO WORLD", 40),
        ("t", 45),
    ],
)
def test_decode_plain_literal_default_preserves_value(
    reflector,
    raw_value,
    base_code,
):
    result = reflector._decode_literal_default(raw_value, base_code)

    assert result == raw_value


@pytest.mark.parametrize(
    ("default_type", "default_value", "base_code", "expected"),
    [
        ("L", "gAAAAAAAAAAA 0.000", 5, "0.000"),
        ("T", None, 7, "TODAY"),
        ("U", None, 13, "USER"),
        ("C", None, 10, "CURRENT"),
        ("S", None, 13, "DBSERVERNAME"),
        ("N", None, 13, None),
        (None, None, 13, None),
    ],
)
def test_decode_default(
    reflector,
    default_type,
    default_value,
    base_code,
    expected,
):
    result = reflector._decode_default(
        default_type,
        default_value,
        base_code,
    )

    assert result == expected
