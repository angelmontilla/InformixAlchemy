from __future__ import annotations

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    cast,
    select,
    type_coerce,
)
from sqlalchemy.types import TypeDecorator


class StringAsInt(TypeDecorator):
    impl = String(50)
    cache_ok = True

    def column_expression(self, col):
        return cast(col, Integer)

    def bind_expression(self, col):
        return cast(type_coerce(col, Integer), String(50))


def test_string_type_decorator_with_cast_round_trips_as_integer(engine, name_factory):
    metadata = MetaData()
    table = Table(
        name_factory("sa_cast_"),
        metadata,
        Column("x", StringAsInt()),
    )

    with engine.begin() as conn:
        metadata.create_all(conn)
        conn.execute(table.insert(), [{"x": value} for value in [1, 2, 3]])

        result = {row[0] for row in conn.execute(select(table.c.x))}
        filtered = {row[0] for row in conn.execute(select(table.c.x).where(table.c.x == 2))}

        assert result == {1, 2, 3}
        assert filtered == {2}

        metadata.drop_all(conn)
