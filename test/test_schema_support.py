from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Sequence
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy.schema import CreateSequence
from sqlalchemy.schema import CreateTable
from sqlalchemy.schema import DropSequence

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def test_schema_qualified_table_compilation():
    dialect = IfxDialect_pyodbc()
    metadata = MetaData()

    table = Table(
        "orders",
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
        ),
        schema="reporting",
    )

    create_sql = str(
        CreateTable(table).compile(
            dialect=dialect,
        )
    )

    select_sql = str(
        select(table.c.id).compile(
            dialect=dialect,
        )
    )

    assert "CREATE TABLE reporting.orders" in create_sql
    assert "FROM reporting.orders AS orders" in select_sql


def test_cross_schema_foreign_key_compilation():
    dialect = IfxDialect_pyodbc()
    metadata = MetaData()

    parent = Table(
        "parent",
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
        ),
        schema="owner_a",
    )

    child = Table(
        "child",
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
        ),
        Column(
            "parent_id",
            Integer,
            ForeignKey(parent.c.id),
        ),
        schema="owner_b",
    )

    sql = str(
        CreateTable(child).compile(
            dialect=dialect,
        )
    )

    assert "CREATE TABLE owner_b.child" in sql
    assert "REFERENCES owner_a.parent" in sql


def test_schema_qualified_sequence_compilation():
    dialect = IfxDialect_pyodbc()

    sequence = Sequence(
        "orders_id_seq",
        schema="reporting",
    )

    create_sql = str(
        CreateSequence(sequence).compile(
            dialect=dialect,
        )
    )

    drop_sql = str(
        DropSequence(sequence).compile(
            dialect=dialect,
        )
    )

    nextval_sql = str(
        sequence.next_value().compile(
            dialect=dialect,
        )
    )

    assert create_sql.startswith(
        "CREATE SEQUENCE reporting.orders_id_seq"
    )
    assert drop_sql == (
        "DROP SEQUENCE reporting.orders_id_seq"
    )
    assert nextval_sql == "'REPORTING'.orders_id_seq.NEXTVAL"


def test_schema_translate_map_compilation():
    dialect = IfxDialect_pyodbc()
    metadata = MetaData()

    table = Table(
        "orders",
        metadata,
        Column(
            "id",
            Integer,
        ),
        schema="logical_schema",
    )

    sequence = Sequence(
        "orders_id_seq",
        schema="logical_schema",
    )

    translate_map = {
        "logical_schema": "physical_owner",
    }

    table_sql = str(
        CreateTable(table).compile(
            dialect=dialect,
            schema_translate_map=translate_map,
            render_schema_translate=True,
        )
    )

    sequence_sql = str(
        sequence.next_value().compile(
            dialect=dialect,
            schema_translate_map=translate_map,
            render_schema_translate=True,
        )
    )

    assert "CREATE TABLE physical_owner.orders" in table_sql
    assert sequence_sql == "'PHYSICAL_OWNER'.orders_id_seq.NEXTVAL"


def test_same_named_default_and_schema_tables_receive_distinct_aliases():
    dialect = IfxDialect_pyodbc()
    metadata = MetaData()
    local_table = Table(
        "some_table", metadata, Column("id", Integer), Column("some_table_id", Integer)
    )
    owned_table = Table(
        "some_table", metadata, Column("id", Integer), schema="test_schema"
    )
    statement = select(local_table, owned_table.c.id).select_from(
        local_table.join(
            owned_table, local_table.c.some_table_id == owned_table.c.id
        )
    )
    compiled = str(statement.compile(dialect=dialect))
    assert "FROM some_table AS some_table_1" in compiled
    assert "JOIN test_schema.some_table AS some_table" in compiled


def test_sequence_runtime_schema_translate_uses_authorization_owner():
    from types import SimpleNamespace
    from IfxAlchemy.base import IfxExecutionContext

    dialect = IfxDialect_pyodbc()
    captured = {}
    context = SimpleNamespace(
        identifier_preparer=dialect.identifier_preparer,
        execution_options={
            "schema_translate_map": {"alt_schema": "test_schema"}
        },
    )

    def execute_scalar(statement, type_):
        captured["statement"] = statement
        captured["type"] = type_
        return 1

    context._execute_scalar = execute_scalar
    sequence = Sequence("noret_sch_id_seq", schema="alt_schema")
    result = IfxExecutionContext.fire_sequence(context, sequence, Integer())
    assert result == 1
    assert captured["statement"] == (
        "SELECT FIRST 1 'TEST_SCHEMA'.noret_sch_id_seq.NEXTVAL FROM systables"
    )


def test_same_named_schema_table_inside_subquery_keeps_local_alias():
    """Owner-qualified homonyms remain addressable inside a subquery."""
    dialect = IfxDialect_pyodbc()
    metadata = MetaData()

    local_table = Table(
        "some_table",
        metadata,
        Column("id", Integer),
        Column("some_table_id", Integer),
    )
    owned_table = Table(
        "some_table",
        metadata,
        Column("id", Integer),
        schema="test_schema",
    )

    subquery = (
        select(local_table)
        .join_from(
            local_table,
            owned_table,
            local_table.c.some_table_id == owned_table.c.id,
        )
        .where(local_table.c.id == 1)
        .subquery()
    )
    statement = (
        select(local_table, subquery.c.id)
        .join_from(
            local_table,
            subquery,
            local_table.c.some_table_id == subquery.c.id,
        )
        .where(local_table.c.id == 1)
    )

    compiled = str(statement.compile(dialect=dialect))

    assert "FROM some_table AS some_table_1" in compiled
    assert "JOIN test_schema.some_table AS some_table" in compiled
    assert "some_table_1.some_table_id = some_table.id" in compiled
