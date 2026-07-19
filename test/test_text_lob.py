from __future__ import annotations

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Table,
    Text,
    insert,
    select,
)


pytestmark = pytest.mark.requires_informix


def test_text_roundtrip(engine, name_factory):
    metadata = MetaData()

    table = Table(
        name_factory("ifx_txt_"),
        metadata,
        Column("id", Integer, primary_key=True),
        Column("content", Text, nullable=False),
    )

    expected = "CAMIÓN PEÑA MUÑOZ " * 1000
    table_created = False

    try:
        metadata.create_all(engine)
        table_created = True

        with engine.begin() as connection:
            connection.execute(
                insert(table).values(content=expected)
            )

        with engine.connect() as connection:
            received = connection.execute(
                select(table.c.content)
            ).scalar_one()

        assert received == expected
        assert isinstance(received, str)
    finally:
        if table_created:
            metadata.drop_all(engine, checkfirst=True)
