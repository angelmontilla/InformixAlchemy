import pytest
from sqlalchemy import Boolean, Date, DateTime, Integer, Numeric, String

from IfxAlchemy.base import _normalize_ifx_reflected_default


@pytest.mark.parametrize(
    ("raw_default", "column_type", "expected"),
    [
        ("gAAAAAAAAAAA 0.000", Numeric(15, 3), "0.000"),
        ("AAAAAA 0", Integer(), "0"),
        ("AAAAAAAAAAE 1", Integer(), "1"),
        ("AACOrQ 2000-01-01", Date(), "2000-01-01"),
        ("ACTIVO", String(20), "ACTIVO"),
        ("NO INICIADO", String(30), "NO INICIADO"),
        ("t", Boolean(), "true"),
        ("f", Boolean(), "false"),
        ("'t'", Boolean(), "true"),
        ("'f'", Boolean(), "false"),
        ("TRUE", Boolean(), "true"),
        ("FALSE", Boolean(), "false"),
        ("1", Boolean(), "true"),
        ("0", Boolean(), "false"), 
        ("CURRENT YEAR TO SECOND", DateTime(), "CURRENT YEAR TO SECOND"),
        ("TODAY", Date(), "TODAY"),
        (None, Integer(), None),
    ],
)
def test_normalize_ifx_reflected_default(
    raw_default,
    column_type,
    expected,
):
    assert (
        _normalize_ifx_reflected_default(
            raw_default,
            column_type,
        )
        == expected
    )
