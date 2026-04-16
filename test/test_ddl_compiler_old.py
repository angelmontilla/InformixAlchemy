from __future__ import annotations

import pytest
from sqlalchemy import Column, Date, DateTime, Index, Integer, MetaData, Numeric, String, Table
from sqlalchemy.schema import CreateIndex, CreateTable, DropIndex, DropTable

from IfxAlchemy.pyodbc import IfxDialect_pyodbc

@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


@pytest.mark.ddl_compiler
def test_create_table_compiles_basic_types(dialect):
    metadata = MetaData()
    table = Table(
        "sa_compile_basic",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50), nullable=False),
        Column("amount", Numeric(10, 2)),
        Column("created_on", Date),
        Column("updated_at", DateTime),
    )

    compiled = str(CreateTable(table).compile(dialect=dialect))
    upper = compiled.upper()

    assert "CREATE TABLE" in upper
    assert "SYSCAT" not in upper
    assert "SYSIBM" not in upper
    assert "ID" in upper
    assert "NAME" in upper
    assert "AMOUNT" in upper
    assert "CREATED_ON" in upper
    assert "UPDATED_AT" in upper


@pytest.mark.ddl_compiler
def test_drop_table_compiles(dialect):
    metadata = MetaData()
    table = Table("sa_compile_drop", metadata, Column("id", Integer))

    compiled = str(DropTable(table).compile(dialect=dialect)).upper()

    assert "DROP TABLE" in compiled
    assert "SA_COMPILE_DROP" in compiled


@pytest.mark.ddl_compiler
def test_create_and_drop_index_compile(dialect):
    metadata = MetaData()
    table = Table(
        "sa_compile_idx",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
    )
    index = Index("ix_sa_compile_idx_name", table.c.name)

    create_compiled = str(CreateIndex(index).compile(dialect=dialect)).upper()
    drop_compiled = str(DropIndex(index).compile(dialect=dialect)).upper()

    assert "CREATE INDEX" in create_compiled
    assert "IX_SA_COMPILE_IDX_NAME" in create_compiled
    assert "DROP INDEX" in drop_compiled


@pytest.mark.ddl_compiler
def test_type_compiler_smoke(dialect):
    type_compiler = dialect.type_compiler

    assert "INTEGER" in type_compiler.process(Integer()).upper()
    assert "VARCHAR" in type_compiler.process(String(50)).upper()
    assert "DECIMAL" in type_compiler.process(Numeric(10, 2)).upper()
    assert "DATE" in type_compiler.process(Date()).upper()
    assert "DATETIME" in type_compiler.process(DateTime()).upper()
