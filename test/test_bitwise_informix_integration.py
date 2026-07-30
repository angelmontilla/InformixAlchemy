from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, select


pytestmark = pytest.mark.requires_informix


@pytest.fixture
def bitwise_table(conn, name_factory):
    metadata = MetaData()
    table = Table(
        name_factory("sa_bitwise_"),
        metadata,
        Column("a", Integer, nullable=False),
        Column("b", Integer, nullable=False),
    )

    metadata.create_all(conn)
    conn.execute(
        table.insert(),
        [{"a": value, "b": value + 1} for value in range(10)],
    )
    conn.commit()

    try:
        yield table
    finally:
        conn.rollback()
        metadata.drop_all(conn, checkfirst=True)
        conn.commit()


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (lambda value: value.bitwise_xor(5), [0, 1, 2, 3, 4, 6, 7, 8, 9]),
        (lambda value: value.bitwise_or(1), list(range(10))),
        (lambda value: value.bitwise_and(4), [4, 5, 6, 7]),
        (lambda value: (value - 2).bitwise_not(), [0]),
        (lambda value: value.bitwise_lshift(1), list(range(1, 10))),
        (lambda value: value.bitwise_rshift(2), list(range(4, 10))),
    ],
)
def test_bitwise_operations_round_trip(
    conn,
    bitwise_table,
    operation,
    expected,
):
    value = bitwise_table.c.a
    rows = conn.execute(
        select(value)
        .where(operation(value) > 0)
        .order_by(value)
    ).scalars().all()

    assert rows == expected
