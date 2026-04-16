from __future__ import annotations

import uuid

import pytest
from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    inspect,
)


@pytest.fixture
def name_factory():
    def _make(prefix: str = "sa_") -> str:
        return f"{prefix}{uuid.uuid4().hex[:12]}"
    return _make


@pytest.fixture
def ddl_roundtrip_objects(name_factory):
    suffix = name_factory("ddl_")[-8:]

    parent_name = f"sa_ddl_parent_{suffix}"
    child_name = f"sa_ddl_child_{suffix}"

    pk_parent = f"pk_p_{suffix}"
    pk_child = f"pk_c_{suffix}"
    uq_parent = f"uq_p_{suffix}"
    fk_child_parent = f"fk_c_p_{suffix}"
    ix_child_note = f"ix_c_note_{suffix}"

    metadata = MetaData()

    parent = Table(
        parent_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False, nullable=False),
        Column("code", String(20), nullable=False),
        Column("name", String(50)),
        Column("created_on", Date),
        Column("amount", Numeric(10, 2)),
    )
    parent.primary_key.name = pk_parent
    parent.append_constraint(UniqueConstraint("code", name=uq_parent))

    child = Table(
        child_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False, nullable=False),
        Column("parent_id", Integer, nullable=False),
        Column("note", String(30)),
    )
    child.primary_key.name = pk_child
    child.append_constraint(
        ForeignKeyConstraint(
            ["parent_id"],
            [f"{parent_name}.id"],
            name=fk_child_parent,
        )
    )
    Index(ix_child_note, child.c.note)

    return {
        "metadata": metadata,
        "names": {
            "parent": parent_name,
            "child": child_name,
            "pk_parent": pk_parent,
            "pk_child": pk_child,
            "uq_parent": uq_parent,
            "fk_child_parent": fk_child_parent,
            "ix_child_note": ix_child_note,
        },
    }


@pytest.mark.ddl_execute
def test_create_table_inspect_drop_table(engine, ddl_roundtrip_objects):
    metadata = ddl_roundtrip_objects["metadata"]
    names = ddl_roundtrip_objects["names"]

    with engine.begin() as connection:
        metadata.create_all(connection)

        insp = inspect(connection)

        assert insp.has_table(names["parent"]) is True
        assert insp.has_table(names["child"]) is True

        table_names = insp.get_table_names()
        assert names["parent"] in table_names, table_names
        assert names["child"] in table_names, table_names

        parent_columns = {col["name"]: col for col in insp.get_columns(names["parent"])}
        assert set(parent_columns.keys()) == {
            "id",
            "code",
            "name",
            "created_on",
            "amount",
        }

        assert parent_columns["id"]["type"]._type_affinity is Integer
        assert parent_columns["code"]["type"]._type_affinity is String
        assert parent_columns["created_on"]["type"]._type_affinity is Date
        assert parent_columns["amount"]["type"]._type_affinity is Numeric

        parent_pk = insp.get_pk_constraint(names["parent"])
        assert parent_pk["name"] == names["pk_parent"].lower(), parent_pk
        assert parent_pk["constrained_columns"] == ["id"], parent_pk

        parent_uqs = insp.get_unique_constraints(names["parent"])
        uq_by_name = {uq["name"]: uq for uq in parent_uqs}
        assert names["uq_parent"].lower() in uq_by_name, parent_uqs
        assert uq_by_name[names["uq_parent"].lower()]["column_names"] == ["code"]

        child_fks = insp.get_foreign_keys(names["child"])
        assert len(child_fks) == 1, child_fks
        fk = child_fks[0]
        assert fk["name"] == names["fk_child_parent"].lower(), fk
        assert fk["constrained_columns"] == ["parent_id"], fk
        assert fk["referred_table"] == names["parent"], fk
        assert fk["referred_columns"] == ["id"], fk

        child_indexes = insp.get_indexes(names["child"])
        ix_by_name = {ix["name"]: ix for ix in child_indexes}
        assert names["ix_child_note"].lower() in ix_by_name, child_indexes
        assert ix_by_name[names["ix_child_note"].lower()]["column_names"] == ["note"]

        metadata.drop_all(connection)

        insp_after_drop = inspect(connection)
        assert insp_after_drop.has_table(names["child"]) is False
        assert insp_after_drop.has_table(names["parent"]) is False


@pytest.mark.ddl_execute
def test_named_check_constraint_ddl_executes(engine, name_factory):
    table_name = name_factory("sa_ck_")
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("qty", Integer, nullable=False),
        CheckConstraint("qty > 0", name=f"ck_{table_name}_qty"),
    )

    with engine.begin() as connection:
        metadata.create_all(connection)

        insp = inspect(connection)
        assert insp.has_table(table_name) is True

        metadata.drop_all(connection)
