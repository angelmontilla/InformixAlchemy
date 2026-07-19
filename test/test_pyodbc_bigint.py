import pytest

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def get_converter():
    dialect = IfxDialect_pyodbc()

    converter_holder = {}

    class FakeConnection:
        def add_output_converter(self, code, fn):
            converter_holder[code] = fn

    on_connect = dialect.on_connect()
    on_connect(FakeConnection())

    return converter_holder[-114]


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, None),
        (1234567890123, 1234567890123),
        ("1234567890123", 1234567890123),
        ("-1234567890123", -1234567890123),
        (b"1234567890123", 1234567890123),
        (b"-1234567890123", -1234567890123),
        (bytearray(b"1234567890123"), 1234567890123),
        (memoryview(b"1234567890123"), 1234567890123),
    ],
)
def test_bigint_converter_accepts_verified_text_formats(
    input_value,
    expected,
):
    converter = get_converter()

    assert converter(input_value) == expected


@pytest.mark.parametrize(
    ("expected", "binary_value"),
    [
        (
            0,
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
        ),
        (
            1,
            b"\x01\x00\x00\x00\x00\x00\x00\x00",
        ),
        (
            256,
            b"\x00\x01\x00\x00\x00\x00\x00\x00",
        ),
        (
            1234567890123,
            int(1234567890123).to_bytes(
                8,
                byteorder="little",
                signed=True,
            ),
        ),
        (
            -1,
            b"\xff\xff\xff\xff\xff\xff\xff\xff",
        ),
        (
            -1234567890123,
            int(-1234567890123).to_bytes(
                8,
                byteorder="little",
                signed=True,
            ),
        ),
    ],
)
def test_bigint_converter_accepts_signed_little_endian_binary(
    expected,
    binary_value,
):
    converter = get_converter()

    assert converter(binary_value) == expected


def test_bigint_converter_prefers_decimal_ascii_for_eight_byte_value():
    converter = get_converter()

    # Aunque tiene exactamente ocho bytes, representa el texto decimal
    # "12345678" y no debe interpretarse como un entero binario.
    assert converter(b"12345678") == 12345678


def test_bigint_converter_accepts_null_padded_decimal_ascii():
    converter = get_converter()

    assert converter(b"123\x00\x00\x00\x00\x00") == 123


@pytest.mark.parametrize(
    "input_value",
    [
        b"\xea\x16\xb0L\x02",
        b"\x01\x02",
        b"\x01\x02\x03\x04\x05\x06\x07",
        b"\x01\x02\x03\x04\x05\x06\x07\x08\x09",
    ],
)
def test_bigint_converter_rejects_unknown_binary_format(
    input_value,
):
    converter = get_converter()

    with pytest.raises(
        ValueError,
        match="unknown binary format",
    ):
        converter(input_value)


@pytest.mark.parametrize(
    "input_value",
    [
        "",
        "   ",
        b"",
        bytearray(),
        memoryview(b""),
    ],
)
def test_bigint_converter_returns_none_for_empty_values(
    input_value,
):
    converter = get_converter()

    assert converter(input_value) is None


@pytest.mark.parametrize(
    "input_value",
    [
        "12.5",
        "abc",
        "+",
        "-",
        b"12.5",
        b"abc",
        b"+",
        b"-",
    ],
)
def test_bigint_converter_rejects_non_decimal_text(
    input_value,
):
    converter = get_converter()

    with pytest.raises(ValueError):
        converter(input_value)
