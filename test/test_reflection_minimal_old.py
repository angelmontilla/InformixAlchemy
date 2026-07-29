from __future__ import annotations

import uuid

import pytest
from sqlalchemy import inspect
from sqlalchemy.sql import sqltypes


def _name(prefix: str = "sa_reflect_") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _norm(value: str | None) -> str | None:
    return value.lower() if isinstance(value, str) else value


@pytest.fixture
def reflected_table(engine):
    """
    Create a real minimal table and clean it up afterward.

    Note:
    - Native SQL is used intentionally to avoid mixing this test with dialect DDL.
    - Commit explicitly after CREATE/DROP.
    """
    table_name = _name()

    create_sql = f"""
    CREATE TABLE {table_name} (
        id INTEGER NOT NULL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        qty INTEGER
    )
    """

    drop_sql = f"DROP TABLE {table_name}"

    with engine.connect() as conn:
        conn.exec_driver_sql(create_sql)
        conn.commit()

    try:
        yield table_name
    finally:
        with engine.connect() as conn:
            try:
                conn.exec_driver_sql(drop_sql)
                conn.commit()
            except Exception:
                conn.rollback()
                raise


def test_has_table_and_get_table_names(engine, reflected_table):
    table_name = reflected_table

    with engine.connect() as conn:
        insp = inspect(conn)

        assert insp.has_table(table_name) is True

        table_names = insp.get_table_names()
        assert any(_norm(t) == _norm(table_name) for t in table_names), table_names


def test_get_columns(engine, reflected_table):
    table_name = reflected_table

    with engine.connect() as conn:
        insp = inspect(conn)
        columns = insp.get_columns(table_name)

    assert len(columns) == 3, columns

    by_name = {_norm(col["name"]): col for col in columns}

    assert set(by_name.keys()) == {"id", "name", "qty"}

    # id
    assert by_name["id"]["nullable"] is False
    assert by_name["id"]["type"]._type_affinity is sqltypes.Integer

    # name
    assert by_name["name"]["nullable"] is False
    assert by_name["name"]["type"]._type_affinity is sqltypes.String

    # qty
    # nullable may be True or None depending on the reflection result at this
    # early stage; the important properties here are the name and type affinity.
    assert by_name["qty"]["type"]._type_affinity is sqltypes.Integer


def test_get_pk_constraint(engine, reflected_table):
    table_name = reflected_table

    with engine.connect() as conn:
        insp = inspect(conn)
        pk = insp.get_pk_constraint(table_name)

    assert isinstance(pk, dict), pk
    assert "constrained_columns" in pk, pk

    constrained = [_norm(col) for col in (pk.get("constrained_columns") or [])]
    assert constrained == ["id"], pk
