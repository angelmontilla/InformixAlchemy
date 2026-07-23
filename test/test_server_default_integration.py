from decimal import Decimal

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    insert,
    inspect,
    select,
    text,
)


TABLE_NAME = "ifx_test_server_defaults"


@pytest.fixture
def server_default_table(engine):
    metadata = MetaData()

    table = Table(
        TABLE_NAME,
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        Column(
            "importe",
            Numeric(15, 3),
            nullable=False,
            server_default=text("0.000"),
        ),
        Column(
            "estado",
            String(10),
            nullable=False,
            server_default=text("'ACTIVO'"),
        ),
    )

    metadata.drop_all(engine, checkfirst=True)
    metadata.create_all(engine)

    try:
        yield table
    finally:
        metadata.drop_all(engine, checkfirst=True)


def test_create_table_with_server_defaults(
    engine,
    server_default_table,
):
    inspector = inspect(engine)

    assert inspector.has_table(server_default_table.name)


def test_server_defaults_are_applied(
    engine,
    server_default_table,
):
    with engine.begin() as connection:
        connection.execute(
            insert(server_default_table).values(id=1)
        )

    with engine.connect() as connection:
        row = connection.execute(
            select(
                server_default_table.c.importe,
                server_default_table.c.estado,
            ).where(server_default_table.c.id == 1)
        ).one()

    assert row.importe == Decimal("0.000")
    assert row.estado == "ACTIVO"


def test_explicit_values_override_server_defaults(
    engine,
    server_default_table,
):
    with engine.begin() as connection:
        connection.execute(
            insert(server_default_table).values(
                id=2,
                importe=Decimal("125.750"),
                estado="MANUAL",
            )
        )

    with engine.connect() as connection:
        row = connection.execute(
            select(
                server_default_table.c.importe,
                server_default_table.c.estado,
            ).where(server_default_table.c.id == 2)
        ).one()

    assert row.importe == Decimal("125.750")
    assert row.estado == "MANUAL"


def test_server_defaults_are_reflected_on_the_correct_columns(
    engine,
    server_default_table,
):
    inspector = inspect(engine)

    columns = {
        str(column["name"]): column
        for column in inspector.get_columns(server_default_table.name)
    }

    assert set(columns) == {"id", "importe", "estado"}
    assert columns["id"]["default"] is None
    assert columns["importe"]["default"] == "0.000"
    assert columns["estado"]["default"] == "ACTIVO"


@pytest.fixture
def literal_default_table(engine):
    metadata = MetaData()
    table = Table(
        "ifx_test_literal_defaults",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("literal_value", Integer, server_default=text("10")),
        Column("literal_text", String(20), server_default=text("'ACTIVO'")),
    )

    metadata.drop_all(engine, checkfirst=True)
    metadata.create_all(engine)
    try:
        yield table
    finally:
        metadata.drop_all(engine, checkfirst=True)


def test_literal_defaults_are_reflected(engine, expression_default_table):
    columns = {
        str(column["name"]): column
        for column in inspect(engine).get_columns(expression_default_table.name)
    }

    assert columns["literal_value"]["default"] == "10"
    assert columns["literal_text"]["default"].replace(" ", "") == "ACTIVO"
