from __future__ import annotations

import pytest
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    LargeBinary,
    Column,
    Date,
    DateTime,
    func,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    select,
)
from sqlalchemy.sql.elements import (
    ReleaseSavepointClause,
    RollbackToSavepointClause,
    SavepointClause,
)
from sqlalchemy.schema import CreateIndex, CreateTable, DropIndex, DropTable

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc()


@pytest.fixture
def sample_table():
    metadata = MetaData()

    table = Table(
        "sa_compile_basic",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("code", String(20), nullable=False),
        Column("name", String(50)),
        Column("created_on", Date),
        Column("updated_at", DateTime),
        Column("amount", Numeric(10, 2)),
        Column("flag", Boolean),
    )

    Index("ix_sa_compile_basic_name", table.c.name)

    return table


def _upper_sql(sql_text: str) -> str:
    return " ".join(sql_text.upper().split())


@pytest.mark.ddl_compiler
def test_create_table_compiles_basic_types(dialect, sample_table):
    compiled = str(CreateTable(sample_table).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "CREATE TABLE" in upper
    assert "SYSCAT" not in upper
    assert "SYSIBM" not in upper

    assert "ID INTEGER NOT NULL" in upper
    assert "CODE VARCHAR(20) NOT NULL" in upper
    assert "NAME VARCHAR(50)" in upper
    assert "CREATED_ON DATE" in upper
    assert "UPDATED_AT DATETIME" in upper
    assert "AMOUNT DECIMAL(10, 2)" in upper
    assert "FLAG SMALLINT" in upper


@pytest.mark.ddl_compiler
def test_drop_table_compiles(dialect, sample_table):
    compiled = str(DropTable(sample_table).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "DROP TABLE" in upper
    assert "SA_COMPILE_BASIC" in upper


@pytest.mark.ddl_compiler
def test_create_index_compiles(dialect, sample_table):
    index = next(iter(sample_table.indexes))

    compiled = str(CreateIndex(index).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "CREATE INDEX" in upper
    assert "IX_SA_COMPILE_BASIC_NAME" in upper
    assert "ON SA_COMPILE_BASIC" in upper
    assert "(NAME)" in upper


@pytest.mark.ddl_compiler
def test_drop_index_compiles(dialect, sample_table):
    index = next(iter(sample_table.indexes))

    compiled = str(DropIndex(index).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "DROP INDEX" in upper
    assert "IX_SA_COMPILE_BASIC_NAME" in upper


@pytest.mark.ddl_compiler
def test_type_compiler_smoke(dialect):
    type_compiler = dialect.type_compiler

    assert type_compiler.process(Integer()).upper() == "INTEGER"
    assert type_compiler.process(String(50)).upper() == "VARCHAR(50)"
    assert type_compiler.process(Numeric(10, 2)).upper() == "DECIMAL(10, 2)"
    assert type_compiler.process(Date()).upper() == "DATE"
    assert type_compiler.process(DateTime()).upper() == "DATETIME YEAR TO SECOND"
    assert type_compiler.process(Boolean()).upper() == "SMALLINT"


@pytest.mark.ddl_compiler
def test_limit_compiles_as_first(dialect, sample_table):
    stmt = select(sample_table.c.id, sample_table.c.name).limit(5)

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "SELECT FIRST 5" in upper
    assert "FROM SA_COMPILE_BASIC" in upper


@pytest.mark.ddl_compiler
def test_limit_offset_compiles_with_row_number_wrapper(dialect, sample_table):
    stmt = (
        select(sample_table.c.id, sample_table.c.name)
        .order_by(sample_table.c.id)
        .limit(5)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "ROW_NUMBER() OVER (ORDER BY SA_COMPILE_BASIC.ID)" in upper
    assert "IFX_RN" in upper
    assert "> 10" in upper
    assert "<= 15" in upper


@pytest.mark.ddl_compiler
def test_limit_offset_keeps_scalar_subquery_projection_intact(
    dialect, sample_table
):
    scalar_table = sample_table.alias("sq")
    scalar_subquery = (
        select(scalar_table.c.code)
        .where(scalar_table.c.id == sample_table.c.id)
        .scalar_subquery()
    )
    stmt = (
        select(sample_table.c.id, scalar_subquery.label("code_copy"))
        .order_by(sample_table.c.id)
        .limit(5)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert (
        "(SELECT SQ.CODE FROM SA_COMPILE_BASIC AS SQ "
        "WHERE SQ.ID = SA_COMPILE_BASIC.ID) AS CODE_COPY"
    ) in upper
    assert "ROW_NUMBER() OVER (ORDER BY SA_COMPILE_BASIC.ID)" in upper


@pytest.mark.ddl_compiler
def test_limit_offset_keeps_function_arguments_with_commas_intact(
    dialect, sample_table
):
    stmt = (
        select(
            sample_table.c.id,
            func.replace(sample_table.c.name, "FROM", "X").label("name2"),
        )
        .order_by(sample_table.c.id)
        .limit(5)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert (
        "REPLACE(SA_COMPILE_BASIC.NAME, :REPLACE_1, :REPLACE_2) AS NAME2"
    ) in upper
    assert "ROW_NUMBER() OVER (ORDER BY SA_COMPILE_BASIC.ID)" in upper


@pytest.mark.ddl_compiler
def test_limit_offset_keeps_unlabeled_function_projection_intact(
    dialect, sample_table
):
    stmt = (
        select(func.replace(sample_table.c.name, "FROM", "X"))
        .limit(5)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "REPLACE(SA_COMPILE_BASIC.NAME," in upper
    assert "AS REPLACE_1" in upper
    assert "__IFX_" not in upper
    assert "ROW_NUMBER() OVER () AS IFX_RN" in upper


@pytest.mark.ddl_compiler
def test_limit_offset_keeps_cte_projection_intact(dialect, sample_table):
    cte = (
        select(
            sample_table.c.id.label("id"),
            sample_table.c.name.label("name"),
        )
        .cte("cte1")
    )
    stmt = select(cte.c.id, cte.c.name).select_from(cte).limit(5).offset(10)

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert (
        "WITH CTE1 AS (SELECT SA_COMPILE_BASIC.ID AS ID, "
        "SA_COMPILE_BASIC.NAME AS NAME FROM SA_COMPILE_BASIC)"
    ) in upper
    assert (
        "FROM (SELECT CTE1.ID AS ID, CTE1.NAME AS NAME, "
        "ROW_NUMBER() OVER () AS IFX_RN FROM CTE1) AS ANON_1"
    ) in upper
    assert " AS ID AS " not in upper
    assert "__IFX_" not in upper


@pytest.mark.ddl_compiler
def test_offset_with_order_by_compiles_with_row_number_wrapper(
    dialect, sample_table
):
    stmt = (
        select(sample_table.c.id, sample_table.c.name)
        .order_by(sample_table.c.name, sample_table.c.id)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert (
        "ROW_NUMBER() OVER (ORDER BY SA_COMPILE_BASIC.NAME, "
        "SA_COMPILE_BASIC.ID)"
    ) in upper
    assert "ORDER BY ANON_1.IFX_RN" in upper
    assert "> 10" in upper
    assert "<=" not in upper


@pytest.mark.ddl_compiler
def test_limit_offset_preserves_distinct_before_row_number(
    dialect, sample_table
):
    stmt = (
        select(sample_table.c.name)
        .distinct()
        .order_by(sample_table.c.name)
        .limit(5)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert (
        "FROM (SELECT DISTINCT SA_COMPILE_BASIC.NAME AS NAME "
        "FROM SA_COMPILE_BASIC) AS ANON_2"
    ) in upper
    assert "ROW_NUMBER() OVER (ORDER BY ANON_2.NAME)" in upper
    assert "> 10" in upper
    assert "<= 15" in upper


@pytest.mark.ddl_compiler
def test_reserved_words_are_quoted_in_compiled_ddl(dialect):
    metadata = MetaData()
    table = Table(
        "order",
        metadata,
        Column("select", Integer, primary_key=True),
        Column("from", String(20)),
    )

    compiled = str(CreateTable(table).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert 'CREATE TABLE "ORDER"' in upper
    assert '"SELECT" SERIAL NOT NULL' in upper
    assert '"FROM" VARCHAR(20)' in upper


@pytest.mark.ddl_compiler
def test_large_binary_compiles_without_blob_size_suffix(dialect):
    compiled = dialect.type_compiler.process(LargeBinary())

    assert compiled.upper() == "BYTE"


@pytest.mark.ddl_compiler
def test_named_check_constraint_compiles_with_postfixed_name(dialect):
    metadata = MetaData()
    table = Table(
        "sa_check_compile",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("qty", Integer, nullable=False),
        CheckConstraint("qty > 0", name="ck_qty_pos"),
    )

    compiled = str(CreateTable(table).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "CHECK (QTY > 0) CONSTRAINT CK_QTY_POS" in upper
    assert "CONSTRAINT CK_QTY_POS CHECK" not in upper


@pytest.mark.ddl_compiler
def test_savepoint_clauses_compile_with_informix_syntax(dialect):
    savepoint = str(SavepointClause("sa_savepoint_1").compile(dialect=dialect)).upper()
    rollback = str(
        RollbackToSavepointClause("sa_savepoint_1").compile(dialect=dialect)
    ).upper()
    release = str(
        ReleaseSavepointClause("sa_savepoint_1").compile(dialect=dialect)
    ).upper()

    assert savepoint == "SAVEPOINT SA_SAVEPOINT_1"
    assert rollback == "ROLLBACK TO SAVEPOINT SA_SAVEPOINT_1"
    assert release == "RELEASE SAVEPOINT SA_SAVEPOINT_1"
