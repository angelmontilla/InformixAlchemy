from __future__ import annotations

import pytest
from sqlalchemy import inspect


@pytest.fixture
def composite_objects(db_builder, name_factory):
    suffix = name_factory("c_")[-8:]

    parent_name = f"sa_cp_{suffix}"
    child_name = f"sa_cc_{suffix}"
    ix_parent = f"ix_cp_payload_code_{suffix}"
    ix_child = f"ix_cc_note_parent_{suffix}"

    create_sqls = [
        f"""
        CREATE TABLE {parent_name} (
            a INTEGER NOT NULL,
            b INTEGER NOT NULL,
            code1 VARCHAR(20) NOT NULL,
            code2 VARCHAR(20) NOT NULL,
            payload VARCHAR(30),
            PRIMARY KEY (a, b),
            UNIQUE (code1, code2)
        )
        """,
        f"""
        CREATE TABLE {child_name} (
            id INTEGER NOT NULL PRIMARY KEY,
            parent_a INTEGER NOT NULL,
            parent_b INTEGER NOT NULL,
            note VARCHAR(30),
            FOREIGN KEY (parent_a, parent_b)
                REFERENCES {parent_name} (a, b)
        )
        """,
        f"CREATE INDEX {ix_parent} ON {parent_name} (payload, code1)",
        f"CREATE INDEX {ix_child} ON {child_name} (note, parent_a)",
    ]

    drop_sqls = [
        f"DROP TABLE {child_name}",
        f"DROP TABLE {parent_name}",
    ]

    db_builder(create_sqls, drop_sqls)

    return {
        "parent": parent_name,
        "child": child_name,
        "ix_parent": ix_parent,
        "ix_child": ix_child,
    }


@pytest.fixture
def descending_index_objects(db_builder, name_factory):
    suffix = name_factory("d_")[-8:]

    table_name = f"sa_desc_{suffix}"
    ix_name = f"ix_desc_payload_{suffix}"

    create_sqls = [
        f"""
        CREATE TABLE {table_name} (
            id INTEGER NOT NULL PRIMARY KEY,
            payload VARCHAR(30),
            code VARCHAR(20)
        )
        """,
        f"CREATE INDEX {ix_name} ON {table_name} (payload DESC, code)",
    ]

    drop_sqls = [
        f"DROP TABLE {table_name}",
    ]

    db_builder(create_sqls, drop_sqls)

    return {
        "table": table_name,
        "index": ix_name,
    }


@pytest.mark.reflection_composite
def test_get_pk_constraint_composite(engine, composite_objects):
    with engine.connect() as connection:
        insp = inspect(connection)
        pk = insp.get_pk_constraint(composite_objects["parent"])

    assert pk["name"]
    assert pk["constrained_columns"] == ["a", "b"]


@pytest.mark.reflection_composite
def test_get_unique_constraints_composite(engine, composite_objects):
    with engine.connect() as connection:
        insp = inspect(connection)
        unique_constraints = insp.get_unique_constraints(composite_objects["parent"])

    assert any(
        uq["column_names"] == ["code1", "code2"] for uq in unique_constraints
    ), unique_constraints


@pytest.mark.reflection_composite
def test_get_indexes_composite(engine, composite_objects):
    with engine.connect() as connection:
        insp = inspect(connection)
        parent_indexes = insp.get_indexes(composite_objects["parent"])
        child_indexes = insp.get_indexes(composite_objects["child"])

    p_by_name = {ix["name"]: ix for ix in parent_indexes}
    c_by_name = {ix["name"]: ix for ix in child_indexes}

    assert p_by_name[composite_objects["ix_parent"].lower()]["column_names"] == ["payload", "code1"]
    assert c_by_name[composite_objects["ix_child"].lower()]["column_names"] == ["note", "parent_a"]


@pytest.mark.reflection_composite
def test_get_foreign_keys_composite(engine, composite_objects):
    with engine.connect() as connection:
        insp = inspect(connection)
        fks = insp.get_foreign_keys(composite_objects["child"])

    assert len(fks) == 1, fks
    fk = fks[0]
    assert fk["constrained_columns"] == ["parent_a", "parent_b"]
    assert fk["referred_table"] == composite_objects["parent"]
    assert fk["referred_columns"] == ["a", "b"]


@pytest.mark.reflection_composite
def test_get_indexes_reports_desc_sorting(engine, descending_index_objects):
    with engine.connect() as connection:
        insp = inspect(connection)
        indexes = insp.get_indexes(descending_index_objects["table"])

    ix_by_name = {ix["name"]: ix for ix in indexes}
    idx = ix_by_name[descending_index_objects["index"].lower()]

    assert idx["column_names"] == ["payload", "code"], idx
    assert idx["unique"] is False, idx
    assert idx.get("column_sorting") == {"payload": ("desc",)}, idx
