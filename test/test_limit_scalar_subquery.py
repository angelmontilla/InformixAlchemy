from __future__ import annotations

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy import union


def test_limit_scalar_subquery_rendered_twice(engine, name_factory):
    table_name = name_factory("sa_limit_scalar_")
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
    )

    with engine.begin() as connection:
        metadata.create_all(connection)
        connection.execute(
            table.insert(),
            [{"id": 1}, {"id": 2}],
        )

        limited_scalar = (
            select(table.c.id)
            .order_by(table.c.id)
            .limit(1)
            .scalar_subquery()
        )
        statement = union(
            select(limited_scalar),
            select(limited_scalar),
        ).subquery().select()

        assert connection.execute(statement).all() == [(1,)]
