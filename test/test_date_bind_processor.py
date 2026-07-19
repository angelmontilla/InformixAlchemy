from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import Column, Date, MetaData, Table

from IfxAlchemy.base import IfxDialect, _IFXDate
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


TEST_DATE = date(2026, 7, 16)
TEST_DATETIME = datetime(2026, 7, 16, 14, 35, 27)


def _get_bind_processor(dialect=None):
    """
    Return the bind processor used by the Informix DATE implementation.

    The helper keeps each test focused on the value being processed rather
    than on the mechanics required to construct the processor.
    """
    if dialect is None:
        dialect = IfxDialect()

    processor = _IFXDate().bind_processor(dialect)

    assert processor is not None

    return processor


def _get_result_processor(dialect=None):
    """
    Return the result processor used by the Informix DATE implementation.
    """
    if dialect is None:
        dialect = IfxDialect()

    processor = _IFXDate().result_processor(
        dialect,
        coltype=None,
    )

    assert processor is not None

    return processor


def test_date_bind_processor_preserves_none():
    processor = _get_bind_processor()

    assert processor(None) is None


def test_date_bind_processor_preserves_date_object():
    """
    A DATE bind processor must preserve datetime.date values.

    Converting the value to str would bypass the DBAPI's native date binding
    and make the result dependent on locale and textual date formats.
    """
    processor = _get_bind_processor()

    processed = processor(TEST_DATE)

    assert processed == TEST_DATE
    assert type(processed) is date
    assert not isinstance(processed, str)


def test_date_bind_processor_converts_datetime_to_date():
    """
    Informix DATE columns do not store a time component.

    A datetime value is accepted for convenience, but its time component must
    be discarded before it reaches the DBAPI.
    """
    processor = _get_bind_processor()

    processed = processor(TEST_DATETIME)

    assert processed == TEST_DATE
    assert type(processed) is date
    assert not isinstance(processed, datetime)
    assert not isinstance(processed, str)


@pytest.mark.parametrize(
    "invalid_value",
    [
        "2026-07-16",
        b"2026-07-16",
        20260716,
        0.0,
        object(),
    ],
    ids=[
        "string",
        "bytes",
        "integer",
        "float",
        "object",
    ],
)
def test_date_bind_processor_rejects_incompatible_types(
    invalid_value,
):
    processor = _get_bind_processor()

    with pytest.raises(
        TypeError,
        match=(
            r"Informix DATE columns require "
            r"datetime\.date or datetime\.datetime"
        ),
    ):
        processor(invalid_value)


def test_date_bind_processor_reports_received_type():
    processor = _get_bind_processor()

    with pytest.raises(
        TypeError,
        match=r"received str",
    ):
        processor("2026-07-16")


def test_date_result_processor_preserves_none():
    processor = _get_result_processor()

    assert processor(None) is None


def test_date_result_processor_preserves_date_object():
    """
    pyodbc normally returns datetime.date for an Informix DATE column.
    """
    processor = _get_result_processor()

    processed = processor(TEST_DATE)

    assert processed == TEST_DATE
    assert type(processed) is date
    assert not isinstance(processed, str)


def test_date_result_processor_converts_datetime_to_date():
    """
    Some DBAPI implementations may return datetime for a DATE column.

    The dialect must normalize that value to datetime.date so callers always
    receive the semantic Python type corresponding to SQL DATE.
    """
    processor = _get_result_processor()

    processed = processor(TEST_DATETIME)

    assert processed == TEST_DATE
    assert type(processed) is date
    assert not isinstance(processed, datetime)
    assert not isinstance(processed, str)


@pytest.mark.parametrize(
    "invalid_value",
    [
        "2026-07-16",
        b"2026-07-16",
        20260716,
        0.0,
        object(),
    ],
    ids=[
        "string",
        "bytes",
        "integer",
        "float",
        "object",
    ],
)
def test_date_result_processor_rejects_incompatible_types(
    invalid_value,
):
    processor = _get_result_processor()

    with pytest.raises(
        TypeError,
        match=r"Informix DATE returned an incompatible value",
    ):
        processor(invalid_value)


def test_date_result_processor_reports_received_type():
    processor = _get_result_processor()

    with pytest.raises(
        TypeError,
        match=r"\(str\)",
    ):
        processor("2026-07-16")


def test_ifx_date_declares_cache_safety():
    """
    SQLAlchemy may cache statements containing this type because its behavior
    does not depend on mutable instance state.
    """
    assert _IFXDate.cache_ok is True


@pytest.mark.parametrize(
    "dialect",
    [
        pytest.param(
            IfxDialect(),
            id="base-dialect",
        ),
        pytest.param(
            IfxDialect_pyodbc(),
            id="pyodbc-dialect",
        ),
    ],
)
def test_public_date_type_uses_ifx_date_implementation(
    dialect,
):
    """
    It is not enough to test _IFXDate directly.

    This test verifies that a public sqlalchemy.Date column is adapted to the
    Informix-specific implementation used during real statement execution.
    """
    metadata = MetaData()

    table = Table(
        "date_type_contract",
        metadata,
        Column(
            "fecha",
            Date,
            nullable=False,
        ),
    )

    effective_type = table.c.fecha.type.dialect_impl(
        dialect
    )

    assert isinstance(effective_type, _IFXDate)


@pytest.mark.parametrize(
    "dialect",
    [
        pytest.param(
            IfxDialect(),
            id="base-dialect",
        ),
        pytest.param(
            IfxDialect_pyodbc(),
            id="pyodbc-dialect",
        ),
    ],
)
def test_effective_date_type_preserves_native_date_binding(
    dialect,
):
    """
    Validate the complete type-adaptation path used by SQLAlchemy.

    The effective Informix implementation must pass a native date object to
    the DBAPI instead of producing a textual representation.
    """
    metadata = MetaData()

    table = Table(
        "date_binding_contract",
        metadata,
        Column(
            "fecha",
            Date,
            nullable=False,
        ),
    )

    effective_type = table.c.fecha.type.dialect_impl(
        dialect
    )
    processor = effective_type.bind_processor(dialect)

    assert processor is not None

    processed = processor(TEST_DATE)

    assert processed == TEST_DATE
    assert type(processed) is date
    assert not isinstance(processed, str)
