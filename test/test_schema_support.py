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
    assert "FROM reporting.orders" in select_sql


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
    assert nextval_sql == (
        "reporting.orders_id_seq.NEXTVAL"
    )


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
    assert sequence_sql == (
        "physical_owner.orders_id_seq.NEXTVAL"
    )
