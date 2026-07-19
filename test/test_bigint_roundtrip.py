import pytest
from sqlalchemy import BigInteger, Column, Integer, MetaData, Table, insert, select


pytestmark = pytest.mark.requires_informix


@pytest.mark.parametrize(
    "expected",
    [
        0,
        1,
        -1,
        2**31,
        2**40,
        2**63 - 1,
        -(2**63) + 1,
    ],
)
def test_bigint_roundtrip(engine, expected):
    metadata = MetaData()
    table = Table(
        "ifx_test_bigint",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("value", BigInteger, nullable=False),
    )

    metadata.drop_all(engine, checkfirst=True)
    metadata.create_all(engine)

    try:
        with engine.begin() as connection:
            connection.execute(
                insert(table).values(value=expected)
            )

        with engine.connect() as connection:
            received = connection.execute(
                select(table.c.value)
            ).scalar_one()

        assert received == expected
        assert isinstance(received, int)
    finally:
        metadata.drop_all(engine, checkfirst=True)
