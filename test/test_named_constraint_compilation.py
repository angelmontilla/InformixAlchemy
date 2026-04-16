from __future__ import annotations

from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.schema import AddConstraint, CreateTable, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


def test_create_table_compiles_named_constraints_in_informix_order():
    metadata = MetaData()
    Table("sa_parent", metadata, Column("id", Integer, primary_key=True))
    table = Table(
        "sa_named_constraints",
        metadata,
        Column("id", Integer, nullable=False),
        Column("code", String(20), nullable=False),
        Column("parent_id", Integer),
        PrimaryKeyConstraint("id", name="pk_named"),
        UniqueConstraint("code", name="uq_named"),
        ForeignKeyConstraint(["parent_id"], ["sa_parent.id"], name="fk_named"),
    )

    compiled = str(CreateTable(table).compile(dialect=IfxDialect_pyodbc()))

    assert "PRIMARY KEY (id) CONSTRAINT pk_named" in compiled
    assert "UNIQUE (code) CONSTRAINT uq_named" in compiled
    assert "FOREIGN KEY(parent_id) REFERENCES sa_parent (id) CONSTRAINT fk_named" in compiled


def test_add_constraint_compiles_named_constraints_in_informix_order():
    metadata = MetaData()
    parent = Table("sa_parent", metadata, Column("id", Integer, primary_key=True))
    table = Table(
        "sa_named_constraints",
        metadata,
        Column("id", Integer, nullable=False),
        Column("code", String(20), nullable=False),
        Column("parent_id", Integer),
    )

    pk = PrimaryKeyConstraint(table.c.id, name="pk_named")
    uq = UniqueConstraint(table.c.code, name="uq_named")
    fk = ForeignKeyConstraint([table.c.parent_id], [parent.c.id], name="fk_named")

    assert str(AddConstraint(pk).compile(dialect=IfxDialect_pyodbc())) == (
        "ALTER TABLE sa_named_constraints ADD CONSTRAINT PRIMARY KEY (id) CONSTRAINT pk_named"
    )
    assert str(AddConstraint(uq).compile(dialect=IfxDialect_pyodbc())) == (
        "ALTER TABLE sa_named_constraints ADD CONSTRAINT UNIQUE (code) CONSTRAINT uq_named"
    )
    assert str(AddConstraint(fk).compile(dialect=IfxDialect_pyodbc())) == (
        "ALTER TABLE sa_named_constraints ADD CONSTRAINT "
        "FOREIGN KEY(parent_id) REFERENCES sa_parent (id) CONSTRAINT fk_named"
    )
