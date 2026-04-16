from __future__ import annotations

import pytest
from sqlalchemy import Column, Identity, Integer, MetaData, String, Table, inspect
from sqlalchemy.schema import CreateTable

from IfxAlchemy.pyodbc import IfxDialect_pyodbc

@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


@pytest.mark.serial_identity
def test_reflect_serial_marks_autoincrement_true(db_builder, engine, name_factory):
    table_name = name_factory("sa_ser_")

    db_builder(
        f"""
        CREATE TABLE {table_name} (
            id SERIAL NOT NULL PRIMARY KEY,
            name VARCHAR(30)
        )
        """,
        f"DROP TABLE {table_name}",
    )

    with engine.connect() as connection:
        insp = inspect(connection)
        cols = {col["name"]: col for col in insp.get_columns(table_name)}

    assert cols["id"]["autoincrement"] is True


@pytest.mark.serial_identity
def test_reflect_serial8_marks_autoincrement_true(db_builder, engine, name_factory):
    table_name = name_factory("sa_ser8_")

    db_builder(
        f"""
        CREATE TABLE {table_name} (
            id SERIAL8 NOT NULL PRIMARY KEY,
            name VARCHAR(30)
        )
        """,
        f"DROP TABLE {table_name}",
    )

    with engine.connect() as connection:
        insp = inspect(connection)
        cols = {col["name"]: col for col in insp.get_columns(table_name)}

    assert cols["id"]["autoincrement"] is True


@pytest.mark.serial_identity
def test_integer_primary_key_contract_uses_serial(dialect):
    metadata = MetaData()
    table = Table(
        "sa_auto_compile",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(30)),
    )

    compiled = str(CreateTable(table).compile(dialect=dialect)).upper()
    assert "ID SERIAL NOT NULL" in compiled
    assert "IDENTITY" not in compiled


@pytest.mark.serial_identity
def test_identity_contract_is_normalized_to_serial(dialect):
    metadata = MetaData()
    table = Table(
        "sa_auto_compile2",
        metadata,
        Column("id", Integer, Identity(), primary_key=True),
        Column("name", String(30)),
    )

    compiled = str(CreateTable(table).compile(dialect=dialect)).upper()
    assert "ID SERIAL NOT NULL" in compiled
    assert "IDENTITY" not in compiled
