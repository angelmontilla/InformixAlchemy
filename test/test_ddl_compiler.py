from __future__ import annotations

import pytest
from sqlalchemy import (
    bindparam,
    Boolean,
    CheckConstraint,
    CLOB,
    LargeBinary,
    Column,
    Date,
    DateTime,
    ForeignKey,
    func,
    ForeignKeyConstraint,
    Index,
    Integer,
    literal_column,
    MetaData,    
    Numeric,
    String,
    Table,
    Text,
    text,
    Time,
    Unicode,
    Sequence,
    select,
)
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.elements import (
    ReleaseSavepointClause,
    RollbackToSavepointClause,
    SavepointClause,
)
from sqlalchemy.schema import (
    AddConstraint,
    CreateIndex,
    CreateSequence,
    CreateTable,
    DropConstraint,
    DropIndex,
    DropSequence,
    DropTable,
)

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


def _assert_row_number_lower_bound(sql_text: str) -> None:
    assert "IFX_RN > __[POSTCOMPILE_" in sql_text


def _assert_row_number_upper_bound(sql_text: str) -> None:
    assert "IFX_RN <= __[POSTCOMPILE_" in sql_text
    assert " + __[POSTCOMPILE_" in sql_text


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
    assert "FLAG BOOLEAN" in upper


@pytest.mark.ddl_compiler
def test_self_referential_fk_is_deferred_until_after_create(
    dialect,
):
    table = Table(
        "ifx_self_ref",
        MetaData(),
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        Column(
            "parent_id",
            Integer,
            ForeignKey(
                "ifx_self_ref.id",
                name="fk_ifx_self_ref_parent",
            ),
        ),
    )

    create_sql = _upper_sql(
        str(CreateTable(table).compile(dialect=dialect))
    )
    constraint = next(iter(table.foreign_key_constraints))
    alter_sql = _upper_sql(
        str(AddConstraint(constraint).compile(dialect=dialect))
    )

    assert "PARENT_ID INTEGER" in create_sql
    assert "REFERENCES" not in create_sql
    assert "FOREIGN KEY(PARENT_ID)" not in create_sql
    assert (
        "ALTER TABLE IFX_SELF_REF ADD CONSTRAINT "
        "FOREIGN KEY(PARENT_ID) REFERENCES IFX_SELF_REF (ID) "
        "CONSTRAINT FK_IFX_SELF_REF_PARENT"
        in alter_sql
    )


@pytest.mark.ddl_compiler
def test_schema_constraint_names_use_physical_namespace(dialect):
    table = Table(
        "ifx_ns_users",
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("value", Integer),
        CheckConstraint("value > 0", name="ifx_same_check"),
        schema="test_schema",
    )

    compiled = _upper_sql(
        str(CreateTable(table).compile(dialect=dialect))
    )

    assert (
        "CHECK (VALUE > 0) CONSTRAINT "
        "TEST_SCHEMA__IFX_SAME_CHECK"
        in compiled
    )
    constraint = next(
        item
        for item in table.constraints
        if isinstance(item, CheckConstraint)
    )
    assert constraint.name == "ifx_same_check"


@pytest.mark.ddl_compiler
def test_schema_foreign_key_name_uses_physical_namespace(dialect):
    metadata = MetaData()
    parent = Table(
        "ifx_ns_parent",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        schema="test_schema",
    )
    child = Table(
        "ifx_ns_child",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column(
            "parent_id",
            Integer,
            ForeignKey(
                "test_schema.ifx_ns_parent.id",
                name="ifx_same_fk",
                use_alter=True,
            ),
        ),
        schema="test_schema",
    )
    _ = parent
    constraint = next(iter(child.foreign_key_constraints))

    compiled = _upper_sql(
        str(AddConstraint(constraint).compile(dialect=dialect))
    )

    assert (
        "FOREIGN KEY(PARENT_ID) REFERENCES "
        "TEST_SCHEMA.IFX_NS_PARENT (ID) "
        "CONSTRAINT TEST_SCHEMA__IFX_SAME_FK"
        in compiled
    )
    assert constraint.name == "ifx_same_fk"


@pytest.mark.ddl_compiler
def test_foreign_key_ondelete_cascade_compiles(dialect):
    metadata = MetaData()
    parent = Table(
        "ifx_cascade_parent",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
    )
    child = Table(
        "ifx_cascade_child",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column(
            "parent_id",
            Integer,
            ForeignKey(
                "ifx_cascade_parent.id",
                name="fk_ifx_cascade_parent",
                ondelete="CASCADE",
                use_alter=True,
            ),
        ),
    )
    _ = parent
    constraint = next(iter(child.foreign_key_constraints))

    compiled = _upper_sql(
        str(AddConstraint(constraint).compile(dialect=dialect))
    )

    assert (
        "FOREIGN KEY(PARENT_ID) REFERENCES "
        "IFX_CASCADE_PARENT (ID) "
        "CONSTRAINT FK_IFX_CASCADE_PARENT "
        "ON DELETE CASCADE"
        in compiled
    )


@pytest.mark.ddl_compiler
def test_schema_index_names_are_physically_namespaced(dialect):
    table = Table(
        "ifx_ns_users",
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("value", Integer),
        schema="test_schema",
    )
    index = Index("ifx_same_index", table.c.value)

    create_sql = _upper_sql(
        str(CreateIndex(index).compile(dialect=dialect))
    )
    drop_sql = _upper_sql(
        str(DropIndex(index).compile(dialect=dialect))
    )

    assert (
        "CREATE INDEX TEST_SCHEMA__IFX_SAME_INDEX "
        'ON TEST_SCHEMA.IFX_NS_USERS ("VALUE")'
        in create_sql
    )
    assert "DROP INDEX TEST_SCHEMA__IFX_SAME_INDEX" in drop_sql
    assert index.name == "ifx_same_index"


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
def test_constraint_backing_index_appends_exclude_null_keys(dialect):
    metadata = MetaData()
    table = Table(
        "sa_compile_unique",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(50)),
    )
    index = Index("ix_sa_compile_unique_name", table.c.name, unique=True)
    index.info["informix_unique_constraint_as_index"] = True

    compiled = str(CreateIndex(index).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "CREATE UNIQUE INDEX" in upper
    assert "IX_SA_COMPILE_UNIQUE_NAME" in upper
    assert "EXCLUDE NULL KEYS" in upper


@pytest.mark.ddl_compiler
def test_drop_index_compiles(dialect, sample_table):
    index = next(iter(sample_table.indexes))

    compiled = str(DropIndex(index).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "DROP INDEX" in upper
    assert "IX_SA_COMPILE_BASIC_NAME" in upper


@pytest.mark.ddl_compiler
def test_create_table_if_not_exists_compiles(dialect, sample_table):
    compiled = str(
        CreateTable(sample_table, if_not_exists=True).compile(
            dialect=dialect
        )
    )

    assert _upper_sql(compiled).startswith(
        "CREATE TABLE IF NOT EXISTS SA_COMPILE_BASIC ("
    )


@pytest.mark.ddl_compiler
def test_drop_table_if_exists_compiles(dialect, sample_table):
    compiled = str(
        DropTable(sample_table, if_exists=True).compile(dialect=dialect)
    )

    assert _upper_sql(compiled) == (
        "DROP TABLE IF EXISTS SA_COMPILE_BASIC"
    )


@pytest.mark.ddl_compiler
def test_create_index_if_not_exists_compiles(dialect, sample_table):
    index = next(iter(sample_table.indexes))

    compiled = str(
        CreateIndex(index, if_not_exists=True).compile(dialect=dialect)
    )

    assert _upper_sql(compiled) == (
        "CREATE INDEX IF NOT EXISTS IX_SA_COMPILE_BASIC_NAME "
        "ON SA_COMPILE_BASIC (NAME)"
    )


@pytest.mark.ddl_compiler
def test_drop_index_if_exists_compiles(dialect, sample_table):
    index = next(iter(sample_table.indexes))

    compiled = str(
        DropIndex(index, if_exists=True).compile(dialect=dialect)
    )

    assert _upper_sql(compiled) == (
        "DROP INDEX IF EXISTS IX_SA_COMPILE_BASIC_NAME"
    )


@pytest.mark.ddl_compiler
def test_create_constraint_backing_index_if_not_exists_preserves_suffix(
    dialect,
):
    metadata = MetaData()
    table = Table(
        "sa_compile_unique_ifne",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(50)),
    )
    index = Index(
        "ix_sa_compile_unique_ifne_name",
        table.c.name,
        unique=True,
    )
    index.info["informix_unique_constraint_as_index"] = True

    compiled = str(
        CreateIndex(index, if_not_exists=True).compile(dialect=dialect)
    )

    assert _upper_sql(compiled) == (
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "IX_SA_COMPILE_UNIQUE_IFNE_NAME "
        "ON SA_COMPILE_UNIQUE_IFNE (NAME) EXCLUDE NULL KEYS"
    )


@pytest.mark.ddl_compiler
def test_type_compiler_smoke(dialect):
    type_compiler = dialect.type_compiler

    assert type_compiler.process(Integer()).upper() == "INTEGER"
    assert type_compiler.process(String(50)).upper() == "VARCHAR(50)"
    assert type_compiler.process(Numeric(10, 2)).upper() == "DECIMAL(10, 2)"
    assert type_compiler.process(Date()).upper() == "DATE"

    assert (
        type_compiler.process(Time()).upper()
        == "DATETIME HOUR TO FRACTION(5)"
    )

    assert (
        type_compiler.process(DateTime()).upper()
        == "DATETIME YEAR TO FRACTION(5)"
    )

    assert type_compiler.process(Boolean()).upper() == "BOOLEAN"


@pytest.mark.ddl_compiler
def test_time_uses_fraction5_in_create_table(dialect):
    metadata = MetaData()

    table = Table(
        "ifx_time_fraction",
        metadata,
        Column("time_value", Time()),
    )

    compiled = str(
        CreateTable(table).compile(
            dialect=dialect,
        )
    )

    upper = _upper_sql(compiled)

    assert (
        "TIME_VALUE DATETIME HOUR TO FRACTION(5)"
        in upper
    )


@pytest.mark.ddl_compiler
def test_datetime_uses_fraction5_in_create_table(dialect):
    metadata = MetaData()

    table = Table(
        "ifx_datetime_fraction",
        metadata,
        Column("datetime_value", DateTime()),
    )

    compiled = str(
        CreateTable(table).compile(
            dialect=dialect,
        )
    )

    upper = _upper_sql(compiled)

    assert (
        "DATETIME_VALUE DATETIME YEAR TO FRACTION(5)"
        in upper
    )


@pytest.mark.ddl_compiler
def test_limit_compiles_as_first(dialect, sample_table):
    stmt = select(sample_table.c.id, sample_table.c.name).limit(5)

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "SELECT FIRST __[POSTCOMPILE_" in upper
    assert "FROM SA_COMPILE_BASIC" in upper


@pytest.mark.ddl_compiler
def test_fetch_compiles_as_first(dialect, sample_table):
    stmt = select(sample_table.c.id, sample_table.c.name).fetch(5)

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "SELECT FIRST __[POSTCOMPILE_" in upper
    assert "FETCH FIRST" not in upper
    assert "FROM SA_COMPILE_BASIC" in upper


@pytest.mark.ddl_compiler
def test_limit_offset_compiles_with_native_skip_first(dialect, sample_table):
    stmt = (
        select(sample_table.c.id, sample_table.c.name)
        .order_by(sample_table.c.id)
        .limit(5)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert upper.startswith(
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2]"
    )
    assert "ROW_NUMBER" not in upper
    assert upper.endswith("ORDER BY SA_COMPILE_BASIC.ID")


@pytest.mark.ddl_compiler
def test_fetch_offset_compiles_with_native_skip_first(dialect, sample_table):
    stmt = (
        select(sample_table.c.id, sample_table.c.name)
        .order_by(sample_table.c.id)
        .fetch(5)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert upper.startswith(
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2]"
    )
    assert "ROW_NUMBER" not in upper
    assert "FETCH FIRST" not in upper


@pytest.mark.ddl_compiler
def test_limit_offset_integer_values_compile_as_postcompile_skip_first(
    dialect,
    sample_table,
):
    stmt = (
        select(sample_table.c.id)
        .order_by(sample_table.c.id)
        .limit(5)
        .offset(2)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert upper.startswith(
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2]"
    )
    assert "ROW_NUMBER" not in upper


@pytest.mark.ddl_compiler
def test_offset_zero_keeps_cache_safe_native_skip(dialect, sample_table):
    stmt = (
        select(sample_table.c.id)
        .order_by(sample_table.c.id)
        .limit(5)
        .offset(0)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert upper.startswith(
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2]"
    )
    assert "ROW_NUMBER" not in upper


@pytest.mark.ddl_compiler
def test_fetch_percent_rejected(dialect, sample_table):
    stmt = select(sample_table.c.id).fetch(50, percent=True)

    with pytest.raises(CompileError):
        stmt.compile(dialect=dialect)


@pytest.mark.ddl_compiler
def test_fetch_with_ties_rejected(dialect, sample_table):
    stmt = (
        select(sample_table.c.id)
        .order_by(sample_table.c.id)
        .fetch(5, with_ties=True)
    )

    with pytest.raises(CompileError):
        stmt.compile(dialect=dialect)


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
    assert upper.startswith(
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2]"
    )
    assert "ROW_NUMBER" not in upper


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

    compiled = stmt.compile(dialect=dialect)
    upper = _upper_sql(str(compiled))

    assert (
        "REPLACE(SA_COMPILE_BASIC.NAME, __[POSTCOMPILE_REPLACE_1], "
        "__[POSTCOMPILE_REPLACE_2]) AS NAME2"
    ) in upper
    assert compiled.literal_execute_params
    assert upper.startswith(
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2]"
    )
    assert "ROW_NUMBER" not in upper


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
    assert upper.startswith(
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2]"
    )
    assert "ROW_NUMBER" not in upper


@pytest.mark.ddl_compiler
def test_offset_keeps_unlabeled_replace_projection_intact_without_limit(
    dialect, sample_table
):
    stmt = select(func.replace(sample_table.c.name, "a", "b")).offset(2)

    compiled = stmt.compile(dialect=dialect)
    upper = _upper_sql(str(compiled))

    assert upper.startswith("SELECT SKIP __[POSTCOMPILE_PARAM_1]")
    assert "REPLACE(SA_COMPILE_BASIC.NAME, __[POSTCOMPILE_REPLACE_" in upper
    assert compiled.literal_execute_params
    assert "ROW_NUMBER" not in upper
    assert "__IFX_" not in upper


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
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2] CTE1.ID, CTE1.NAME FROM CTE1"
    ) in upper
    assert " AS ID AS " not in upper
    assert "__IFX_" not in upper
    assert "ROW_NUMBER" not in upper


@pytest.mark.ddl_compiler
def test_limit_offset_keeps_direct_cte_projection_intact(
    dialect, sample_table
):
    cte = select(sample_table.c.id, sample_table.c.name).cte("cte1")
    stmt = select(cte.c.id, cte.c.name).limit(5).offset(2)

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert (
        "WITH CTE1 AS (SELECT SA_COMPILE_BASIC.ID AS ID, "
        "SA_COMPILE_BASIC.NAME AS NAME FROM SA_COMPILE_BASIC)"
    ) in upper
    assert (
        "SELECT SKIP __[POSTCOMPILE_PARAM_1] "
        "FIRST __[POSTCOMPILE_PARAM_2] CTE1.ID, CTE1.NAME FROM CTE1"
    ) in upper
    assert "__IFX_" not in upper
    assert "ROW_NUMBER" not in upper


@pytest.mark.ddl_compiler
def test_offset_with_order_by_compiles_with_native_skip(
    dialect, sample_table
):
    stmt = (
        select(sample_table.c.id, sample_table.c.name)
        .order_by(sample_table.c.name, sample_table.c.id)
        .offset(10)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert upper.startswith("SELECT SKIP __[POSTCOMPILE_PARAM_1]")
    assert upper.endswith(
        "ORDER BY SA_COMPILE_BASIC.NAME, SA_COMPILE_BASIC.ID"
    )
    assert "ROW_NUMBER" not in upper


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
    _assert_row_number_lower_bound(upper)
    _assert_row_number_upper_bound(upper)


@pytest.mark.ddl_compiler
def test_fetch_offset_preserves_distinct_before_row_number(
    dialect, sample_table
):
    stmt = (
        select(sample_table.c.name)
        .distinct()
        .order_by(sample_table.c.name)
        .fetch(10)
        .offset(3)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "SELECT DISTINCT" in upper
    assert "ROW_NUMBER() OVER" in upper
    _assert_row_number_lower_bound(upper)
    _assert_row_number_upper_bound(upper)
    assert "FETCH FIRST" not in upper
    assert "__IFX_" not in upper


@pytest.mark.ddl_compiler
def test_for_update_compiles_to_update_lock_clause(dialect, sample_table):
    stmt = select(sample_table).with_for_update()

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert upper.endswith("WITH RS USE AND KEEP UPDATE LOCKS")


@pytest.mark.ddl_compiler
def test_for_update_read_compiles_to_share_lock_clause(dialect, sample_table):
    stmt = select(sample_table).with_for_update(read=True)

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert upper.endswith("WITH RS USE AND KEEP SHARE LOCKS")


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("builder", "message"),
    [
        (lambda table: {"nowait": True}, "NOWAIT"),
        (lambda table: {"skip_locked": True}, "SKIP LOCKED"),
        (lambda table: {"of": [table.c.id]}, "FOR UPDATE OF"),
        (lambda table: {"key_share": True}, "KEY SHARE"),
    ],
)
def test_for_update_rejects_unsupported_variants(
    dialect, sample_table, builder, message
):
    stmt = select(sample_table).with_for_update(**builder(sample_table))

    with pytest.raises(CompileError, match=message):
        stmt.compile(dialect=dialect)


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"with_ties": True}, "WITH TIES"),
        ({"percent": True}, "PERCENT"),
    ],
)
def test_fetch_rejects_unsupported_variants(
    dialect, sample_table, kwargs, message
):
    stmt = select(sample_table).fetch(5, **kwargs)

    with pytest.raises(CompileError, match=message):
        stmt.compile(dialect=dialect)


@pytest.mark.ddl_compiler
def test_exists_in_columns_clause_wraps_as_case_expression(
    dialect, sample_table
):
    stmt = select(
        select(sample_table.c.id)
        .where(sample_table.c.id == 1)
        .exists()
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "CASE WHEN EXISTS (" in upper
    assert "THEN 'T' ELSE 'F' END AS ANON_1" in upper


@pytest.mark.ddl_compiler
def test_exists_in_where_clause_remains_plain_exists(dialect, sample_table):
    stmt = select(sample_table.c.id).where(
        select(sample_table.c.id)
        .where(sample_table.c.id == 1)
        .exists()
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert "WHERE EXISTS (" in upper
    assert "CASE WHEN EXISTS" not in upper


@pytest.mark.ddl_compiler
def test_inner_join_compiles_with_base_join_syntax(dialect):
    metadata = MetaData()
    left = Table(
        "sa_join_left",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(20)),
    )
    right = Table(
        "sa_join_right",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("left_id", Integer),
    )

    stmt = select(left.c.id, right.c.id).select_from(
        left.join(right, left.c.id == right.c.left_id)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert (
        "FROM SA_JOIN_LEFT JOIN SA_JOIN_RIGHT "
        "ON SA_JOIN_LEFT.ID = SA_JOIN_RIGHT.LEFT_ID"
    ) in upper


@pytest.mark.ddl_compiler
def test_left_outer_join_compiles_with_base_join_syntax(dialect):
    metadata = MetaData()
    left = Table(
        "sa_join_left",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(20)),
    )
    right = Table(
        "sa_join_right",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("left_id", Integer),
    )

    stmt = select(left.c.id, right.c.id).select_from(
        left.outerjoin(right, left.c.id == right.c.left_id)
    )

    compiled = str(stmt.compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert (
        "FROM SA_JOIN_LEFT LEFT OUTER JOIN SA_JOIN_RIGHT "
        "ON SA_JOIN_LEFT.ID = SA_JOIN_RIGHT.LEFT_ID"
    ) in upper


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
def test_identifiers_with_illegal_initial_characters_are_quoted(dialect):
    metadata = MetaData()
    table = Table(
        "1bad_table",
        metadata,
        Column("2bad_column", Integer, primary_key=True, autoincrement=False),
        Column("_private_name", String(20)),
        Column("$amount", String(20)),
    )

    compiled = str(CreateTable(table).compile(dialect=dialect))
    upper = _upper_sql(compiled)

    assert 'CREATE TABLE "1BAD_TABLE"' in upper
    assert '"2BAD_COLUMN" INTEGER NOT NULL' in upper
    assert '"_PRIVATE_NAME" VARCHAR(20)' in upper
    assert '"$AMOUNT" VARCHAR(20)' in upper


@pytest.mark.ddl_compiler
def test_unbounded_string_raises_compile_error(dialect):
    string_type = String()

    with pytest.raises(
        CompileError,
        match="Informix VARCHAR requires an explicit length",
    ):
        dialect.type_compiler.process(string_type)


@pytest.mark.ddl_compiler
def test_zero_length_string_raises_compile_error(dialect):
    string_type = String(0)

    with pytest.raises(
        CompileError,
        match="Informix VARCHAR requires an explicit length",
    ):
        dialect.type_compiler.process(string_type)


@pytest.mark.ddl_compiler
def test_unbounded_unicode_raises_compile_error(dialect):
    unicode_type = Unicode()

    with pytest.raises(
        CompileError,
        match="Informix VARGRAPHIC requires an explicit length",
    ):
        dialect.type_compiler.process(unicode_type)


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


@pytest.mark.ddl_compiler
def test_text_and_clob_compile_as_distinct_informix_types(dialect):
    type_compiler = dialect.type_compiler

    assert type_compiler.process(Text()).upper() == "TEXT"
    assert type_compiler.process(CLOB()).upper() == "CLOB"


@pytest.mark.ddl_compiler
def test_create_sequence_omits_generic_no_minmax(dialect):
    sql = str(
        CreateSequence(Sequence("other_seq")).compile(
            dialect=dialect,
        )
    ).upper()

    assert sql == "CREATE SEQUENCE OTHER_SEQ"
    assert "NO MINVALUE" not in sql
    assert "NO MAXVALUE" not in sql


@pytest.mark.ddl_compiler
def test_drop_sequence_compiles(dialect):
    sql = str(
        DropSequence(Sequence("other_seq")).compile(
            dialect=dialect,
        )
    ).upper()

    assert sql == "DROP SEQUENCE OTHER_SEQ"


@pytest.mark.ddl_compiler
def test_create_sequence_if_not_exists_compiles_natively(dialect):
    sql = str(
        CreateSequence(
            Sequence("other_seq"),
            if_not_exists=True,
        ).compile(dialect=dialect)
    ).upper()

    assert sql == "CREATE SEQUENCE IF NOT EXISTS OTHER_SEQ"


@pytest.mark.ddl_compiler
def test_create_sequence_if_not_exists_preserves_schema_and_options(dialect):
    sequence = Sequence(
        "orders_id_seq",
        schema="reporting",
        start=10,
        increment=5,
        minvalue=1,
        maxvalue=1000,
        cache=20,
        cycle=True,
    )

    sql = _upper_sql(
        str(
            CreateSequence(
                sequence,
                if_not_exists=True,
            ).compile(dialect=dialect)
        )
    )

    assert sql == (
        "CREATE SEQUENCE IF NOT EXISTS REPORTING.ORDERS_ID_SEQ "
        "START WITH 10 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 "
        "CACHE 20 CYCLE"
    )


@pytest.mark.ddl_compiler
def test_drop_sequence_if_exists_compiles_natively(dialect):
    sql = str(
        DropSequence(
            Sequence("other_seq"),
            if_exists=True,
        ).compile(dialect=dialect)
    ).upper()

    assert sql == "DROP SEQUENCE IF EXISTS OTHER_SEQ"


@pytest.mark.ddl_compiler
def test_drop_sequence_if_exists_preserves_schema(dialect):
    sql = str(
        DropSequence(
            Sequence("orders_id_seq", schema="reporting"),
            if_exists=True,
        ).compile(dialect=dialect)
    ).upper()

    assert sql == "DROP SEQUENCE IF EXISTS REPORTING.ORDERS_ID_SEQ"


@pytest.mark.ddl_compiler
def test_limit_offset_untyped_bindparams_are_integer_typed(sample_table):
    dialect = IfxDialect_pyodbc(paramstyle="qmark")

    statement = (
        select(sample_table.c.id)
        .order_by(sample_table.c.id)
        .limit(bindparam("l"))
        .offset(bindparam("o"))
    )

    compiled = statement.compile(dialect=dialect)
    sql_text = _upper_sql(str(compiled))

    assert sql_text.startswith("SELECT SKIP ? FIRST ?")
    assert "ROW_NUMBER" not in sql_text

    assert "__[POSTCOMPILE_L]" not in sql_text
    assert "__[POSTCOMPILE_O]" not in sql_text

    assert compiled.positiontup == ["o", "l"]

    assert isinstance(
        compiled.binds["l"].type,
        Integer,
    )
    assert isinstance(
        compiled.binds["o"].type,
        Integer,
    )

    literal_execute_keys = {
        bind.key
        for bind in compiled.literal_execute_params
    }

    assert "l" not in literal_execute_keys
    assert "o" not in literal_execute_keys

@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    "server_default",
    [
        text("3 + 5"),
        text("(3 * 5)"),
        text("10 / 2"),
        text("10 % 3"),
        literal_column("3") + literal_column("5"),
    ],
)
def test_arithmetic_server_defaults_are_rejected(
    dialect,
    server_default,
):
    metadata = MetaData()
    table = Table(
        "sa_arithmetic_default",
        metadata,
        Column(
            "value",
            Integer,
            server_default=server_default,
        ),
    )

    with pytest.raises(
        CompileError,
        match="does not support arithmetic server-default expressions",
    ):
        str(CreateTable(table).compile(dialect=dialect))


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    ("column_type", "server_default", "expected_sql"),
    [
        (Integer, text("10"), "DEFAULT 10"),
        (Integer, text("-10"), "DEFAULT -10"),
        (String(20), text("'A+B'"), "DEFAULT 'A+B'"),
        (Date, text("TODAY"), "DEFAULT TODAY"),
        (
            DateTime,
            text("CURRENT YEAR TO FRACTION(5)"),
            "DEFAULT CURRENT YEAR TO FRACTION(5)",
        ),
    ],
)
def test_supported_non_arithmetic_server_defaults_still_compile(
    dialect,
    column_type,
    server_default,
    expected_sql,
):
    metadata = MetaData()
    table = Table(
        "sa_supported_default",
        metadata,
        Column(
            "value",
            column_type,
            server_default=server_default,
        ),
    )

    compiled = _upper_sql(
        str(CreateTable(table).compile(dialect=dialect))
    )

    assert expected_sql in compiled

def _table_with_foreign_key_ondelete(ondelete):
    metadata = MetaData()

    parent = Table(
        "sa_fk_parent",
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
    )

    child = Table(
        "sa_fk_child",
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        Column(
            "parent_id",
            Integer,
        ),
        ForeignKeyConstraint(
            ["parent_id"],
            [parent.c.id],
            name="fk_sa_child_parent",
            ondelete=ondelete,
        ),
    )

    return child


@pytest.mark.ddl_compiler
def test_foreign_key_without_ondelete_compiles_without_action(
    dialect,
):
    child = _table_with_foreign_key_ondelete(None)

    compiled = _upper_sql(
        str(
            CreateTable(child).compile(
                dialect=dialect
            )
        )
    )

    assert "ON DELETE" not in compiled


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    "ondelete",
    [
        "CASCADE",
        "cascade",
        "  cascade  ",
    ],
)
def test_foreign_key_ondelete_cascade_compiles_canonically(
    dialect,
    ondelete,
):
    child = _table_with_foreign_key_ondelete(
        ondelete
    )

    compiled = _upper_sql(
        str(
            CreateTable(child).compile(
                dialect=dialect
            )
        )
    )

    assert "ON DELETE CASCADE" in compiled


@pytest.mark.ddl_compiler
@pytest.mark.parametrize(
    "ondelete",
    [
        "RESTRICT",
        "SET NULL",
        "SET DEFAULT",
        "NO ACTION",
        "CUALQUIER COSA",
        "CASCADE; DROP TABLE sa_fk_parent",
    ],
)
def test_unsupported_foreign_key_ondelete_actions_are_rejected(
    dialect,
    ondelete,
):
    child = _table_with_foreign_key_ondelete(
        ondelete
    )

    with pytest.raises(
        CompileError,
        match="supports only ON DELETE CASCADE",
    ):
        CreateTable(child).compile(
            dialect=dialect
        )


@pytest.mark.ddl_compiler
def test_drop_named_foreign_key_uses_drop_constraint(dialect):
    metadata = MetaData()
    parent = Table(
        "ifx_drop_parent",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
    )
    child = Table(
        "ifx_drop_child",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("parent_id", Integer),
        ForeignKeyConstraint(
            ["parent_id"],
            [parent.c.id],
            name="fk_ifx_drop_parent",
            use_alter=True,
        ),
    )
    constraint = next(iter(child.foreign_key_constraints))

    compiled = _upper_sql(
        str(DropConstraint(constraint).compile(dialect=dialect))
    )

    assert compiled == (
        "ALTER TABLE IFX_DROP_CHILD "
        "DROP CONSTRAINT FK_IFX_DROP_PARENT"
    )
    assert "DROP FOREIGN KEY" not in compiled


@pytest.mark.ddl_compiler
def test_ansi_owner_constraints_avoid_cross_owner_catalog_collisions(dialect):
    """Equal logical names remain safe when one table has an ANSI owner.

    The official SQLAlchemy ComponentReflectionTest creates the same named
    CHECK constraints for the default owner and ``test_schema``.  Informix can
    record both constraints under the executing authorization identifier, so
    their physical catalog names must differ even though SQLAlchemy must keep
    the logical name unchanged.
    """
    logical_name = "zz_test2_gt_zero"

    default_table = Table(
        "users",
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("test2", Integer),
        CheckConstraint("test2 > 0", name=logical_name),
    )
    schema_table = Table(
        "users",
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("test2", Integer),
        CheckConstraint("test2 > 0", name=logical_name),
        schema="test_schema",
    )

    default_sql = _upper_sql(
        str(CreateTable(default_table).compile(dialect=dialect))
    )
    schema_sql = _upper_sql(
        str(CreateTable(schema_table).compile(dialect=dialect))
    )

    assert "CONSTRAINT ZZ_TEST2_GT_ZERO" in default_sql
    assert "CONSTRAINT TEST_SCHEMA__ZZ_TEST2_GT_ZERO" in schema_sql

    default_check = next(
        c for c in default_table.constraints if isinstance(c, CheckConstraint)
    )
    schema_check = next(
        c for c in schema_table.constraints if isinstance(c, CheckConstraint)
    )
    assert default_check.name == logical_name
    assert schema_check.name == logical_name
