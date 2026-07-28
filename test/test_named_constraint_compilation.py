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


def test_long_convention_pk_name_is_truncated_by_identifier_preparer():
    long_suffix = "_".join(["abcdef" * 5] * 10)

    metadata = MetaData(
        naming_convention={
            "pk": (
                "primary_key_%(table_name)s_%(column_0_N_name)s_"
                + long_suffix
            )
        }
    )

    table = Table(
        "a_things_with_stuff",
        metadata,
        Column(
            "id_long_column_name",
            Integer,
            primary_key=True,
        ),
        Column(
            "id_another_long_name",
            Integer,
            primary_key=True,
        ),
    )

    dialect = IfxDialect_pyodbc()

    assert table.primary_key.name is not None
    assert (
        len(table.primary_key.name)
        > dialect.max_identifier_length
    )

    compiled = str(
        CreateTable(table).compile(dialect=dialect)
    )

    rendered_name = compiled.split(
        " CONSTRAINT ",
        1,
    )[1].split()[0]

    assert (
        len(rendered_name)
        <= dialect.max_identifier_length
    )

    assert rendered_name[:-5] == str(
        table.primary_key.name
    )[: len(rendered_name) - 5]