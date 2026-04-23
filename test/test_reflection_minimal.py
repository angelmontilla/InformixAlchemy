from __future__ import annotations

import uuid

import pytest
from sqlalchemy import inspect
from sqlalchemy.sql import sqltypes


def _norm(value: str | None) -> str | None:
    return value.lower() if isinstance(value, str) else value


@pytest.fixture
def name_factory():
    def _make(prefix: str = "sa_") -> str:
        return f"{prefix}{uuid.uuid4().hex[:12]}"
    return _make


@pytest.fixture
def db_builder(engine):
    """
    build(create_sqls, drop_sqls)

    - create_sqls: str | list[str]
    - drop_sqls: str | list[str]

    Ejecuta CREATE con commit explícito y limpia al final en orden inverso.
    """
    created_groups: list[list[str]] = []

    def _build(create_sqls, drop_sqls):
        create_list = [create_sqls] if isinstance(create_sqls, str) else list(create_sqls)
        drop_list = [drop_sqls] if isinstance(drop_sqls, str) else list(drop_sqls)

        with engine.connect() as connection:
            for stmt in create_list:
                connection.exec_driver_sql(stmt)
            connection.commit()

        created_groups.append(drop_list)

    yield _build

    with engine.connect() as connection:
        for drop_list in reversed(created_groups):
            for stmt in drop_list:
                try:
                    connection.exec_driver_sql(stmt)
                    connection.commit()
                except Exception:
                    connection.rollback()


@pytest.fixture
def default_schema_name(engine) -> str | None:
    return _norm(engine.dialect.default_schema_name)


@pytest.fixture
def basic_reflection_objects(db_builder, name_factory, default_schema_name):
    suffix = name_factory("x_")[-8:]

    table_name = f"sa_min_{suffix}"
    view_name = f"sa_min_v_{suffix}"
    pk_name = f"pk_{suffix}"
    uq_name = f"uq_{suffix}"
    ix_name = f"ix_{suffix}"

    create_sqls = [
        f"""
        CREATE TABLE {table_name} (
            id INTEGER NOT NULL,
            code VARCHAR(20) NOT NULL,
            name VARCHAR(50),
            created_on DATE,
            amount DECIMAL(10,2),
            PRIMARY KEY (id) CONSTRAINT {pk_name},
            UNIQUE (code) CONSTRAINT {uq_name}
        )
        """,
        f"CREATE INDEX {ix_name} ON {table_name} (name)",
        f"CREATE VIEW {view_name} AS SELECT id, code, name FROM {table_name}",
    ]

    drop_sqls = [
        f"DROP VIEW {view_name}",
        f"DROP TABLE {table_name}",
    ]

    db_builder(create_sqls, drop_sqls)

    return {
        "table": table_name,
        "view": view_name,
        "pk": pk_name,
        "uq": uq_name,
        "ix": ix_name,
        "schema": default_schema_name,
    }


@pytest.fixture
def single_fk_objects(db_builder, name_factory, default_schema_name):
    suffix = name_factory("f_")[-8:]

    parent_name = f"sa_parent_{suffix}"
    child_name = f"sa_child_{suffix}"
    parent_pk_name = f"pk_p_{suffix}"
    child_pk_name = f"pk_c_{suffix}"
    fk_name = f"fk_c_p_{suffix}"

    create_sqls = [
        f"""
        CREATE TABLE {parent_name} (
            id INTEGER NOT NULL,
            code VARCHAR(20),
            PRIMARY KEY (id) CONSTRAINT {parent_pk_name}
        )
        """,
        f"""
        CREATE TABLE {child_name} (
            id INTEGER NOT NULL,
            parent_id INTEGER NOT NULL,
            note VARCHAR(30),
            PRIMARY KEY (id) CONSTRAINT {child_pk_name},
            FOREIGN KEY (parent_id)
                REFERENCES {parent_name} (id)
                CONSTRAINT {fk_name}
        )
        """,
    ]

    drop_sqls = [
        f"DROP TABLE {child_name}",
        f"DROP TABLE {parent_name}",
    ]

    db_builder(create_sqls, drop_sqls)

    return {
        "parent": parent_name,
        "child": child_name,
        "parent_pk": parent_pk_name,
        "child_pk": child_pk_name,
        "fk": fk_name,
        "schema": default_schema_name,
    }


@pytest.mark.reflection_minimal
def test_has_table_and_get_table_names(engine, basic_reflection_objects):
    table_name = basic_reflection_objects["table"]
    view_name = basic_reflection_objects["view"]

    with engine.connect() as connection:
        insp = inspect(connection)

        assert insp.has_table(table_name) is True

        table_names = insp.get_table_names()
        assert table_name in table_names, table_names

        # get_table_names() debe listar tablas, no vistas
        assert view_name not in table_names, table_names


@pytest.mark.reflection_minimal
def test_has_table_returns_true_for_view(engine, basic_reflection_objects):
    view_name = basic_reflection_objects["view"]

    with engine.connect() as connection:
        insp = inspect(connection)
        assert insp.has_table(view_name) is True


@pytest.mark.reflection_minimal
def test_get_view_names_and_definition(engine, basic_reflection_objects):
    table_name = basic_reflection_objects["table"]
    view_name = basic_reflection_objects["view"]

    with engine.connect() as connection:
        insp = inspect(connection)

        view_names = insp.get_view_names()
        assert view_name in view_names, view_names

        view_def = insp.get_view_definition(view_name)
        assert view_def is not None
        assert "select" in view_def.lower()
        assert "id" in view_def.lower()
        assert "code" in view_def.lower()
        assert "name" in view_def.lower()
        assert table_name.lower() in view_def.lower()


@pytest.mark.reflection_minimal
def test_get_materialized_view_names_returns_empty_list(engine):
    with engine.connect() as connection:
        insp = inspect(connection)
        mat_views = insp.get_materialized_view_names()

    assert mat_views == []


@pytest.mark.reflection_minimal
def test_get_check_constraints_returns_empty_list(
    engine, basic_reflection_objects
):
    table_name = basic_reflection_objects["table"]

    with engine.connect() as connection:
        insp = inspect(connection)
        checks = insp.get_check_constraints(table_name)

    assert checks == []


@pytest.mark.reflection_minimal
def test_get_table_comment_returns_none_text(engine, basic_reflection_objects):
    table_name = basic_reflection_objects["table"]

    with engine.connect() as connection:
        insp = inspect(connection)
        comment = insp.get_table_comment(table_name)

    assert comment == {"text": None}


@pytest.mark.reflection_minimal
def test_get_table_options_returns_empty_dict(engine, basic_reflection_objects):
    table_name = basic_reflection_objects["table"]

    with engine.connect() as connection:
        insp = inspect(connection)
        options = insp.get_table_options(table_name)

    assert options == {}


@pytest.mark.reflection_minimal
def test_get_columns(engine, basic_reflection_objects):
    table_name = basic_reflection_objects["table"]

    with engine.connect() as connection:
        insp = inspect(connection)
        columns = insp.get_columns(table_name)

    assert len(columns) == 5, columns

    by_name = {col["name"]: col for col in columns}
    assert set(by_name.keys()) == {"id", "code", "name", "created_on", "amount"}

    assert by_name["id"]["nullable"] is False
    assert by_name["id"]["type"]._type_affinity is sqltypes.Integer

    assert by_name["code"]["nullable"] is False
    assert by_name["code"]["type"]._type_affinity is sqltypes.String

    assert by_name["name"]["nullable"] is True
    assert by_name["name"]["type"]._type_affinity is sqltypes.String

    assert by_name["created_on"]["type"]._type_affinity is sqltypes.Date
    assert by_name["amount"]["type"]._type_affinity is sqltypes.Numeric


@pytest.mark.reflection_minimal
def test_get_pk_constraint(engine, basic_reflection_objects):
    table_name = basic_reflection_objects["table"]
    expected_pk_name = basic_reflection_objects["pk"]

    with engine.connect() as connection:
        insp = inspect(connection)
        pk = insp.get_pk_constraint(table_name)

    assert isinstance(pk, dict), pk
    assert pk["name"] == expected_pk_name.lower(), pk
    assert pk["constrained_columns"] == ["id"], pk


@pytest.mark.reflection_minimal
def test_get_unique_constraints_and_indexes(engine, basic_reflection_objects):
    table_name = basic_reflection_objects["table"]
    expected_uq_name = basic_reflection_objects["uq"]
    expected_ix_name = basic_reflection_objects["ix"]

    with engine.connect() as connection:
        insp = inspect(connection)

        unique_constraints = insp.get_unique_constraints(table_name)
        indexes = insp.get_indexes(table_name)

    uq_by_name = {uq["name"]: uq for uq in unique_constraints}

    assert expected_uq_name.lower() in uq_by_name, unique_constraints
    assert uq_by_name[expected_uq_name.lower()]["column_names"] == ["code"], unique_constraints

    ix_by_name = {ix["name"]: ix for ix in indexes}
    assert expected_ix_name.lower() in ix_by_name, indexes
    assert ix_by_name[expected_ix_name.lower()]["column_names"] == ["name"], indexes
    assert ix_by_name[expected_ix_name.lower()]["unique"] is False, indexes


@pytest.mark.reflection_minimal
def test_get_foreign_keys_single_column(engine, single_fk_objects):
    child_name = single_fk_objects["child"]
    parent_name = single_fk_objects["parent"]
    expected_fk_name = single_fk_objects["fk"]
    expected_schema = single_fk_objects["schema"]

    with engine.connect() as connection:
        insp = inspect(connection)

        # Pasamos schema explícito para congelar también referred_schema
        fks = insp.get_foreign_keys(child_name, schema=expected_schema)

    assert len(fks) == 1, fks

    fk = fks[0]
    assert fk["name"] == expected_fk_name.lower(), fk
    assert fk["constrained_columns"] == ["parent_id"], fk
    assert fk["referred_table"] == parent_name, fk
    assert fk["referred_columns"] == ["id"], fk
    assert fk["referred_schema"] == expected_schema, fk
