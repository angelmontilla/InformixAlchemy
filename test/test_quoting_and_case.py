from __future__ import annotations

import pytest
from sqlalchemy import inspect, exc
from sqlalchemy.sql import quoted_name

@pytest.fixture
def quoting_objects(engine, name_factory):
    suffix = name_factory("q_")[-8:]

    unquoted_table = f"sa_norm_{suffix}"
    unquoted_view = f"sa_norm_v_{suffix}"

    quoted_table = f"MixCaseT_{suffix}"
    quoted_view = f"MixCaseV_{suffix}"

    create_sqls = [
        f"""
        CREATE TABLE {unquoted_table} (
            id INTEGER NOT NULL PRIMARY KEY,
            plain_name VARCHAR(20),
            created_on DATE
        )
        """,
        f"""
        CREATE VIEW {unquoted_view}
        AS SELECT id, plain_name FROM {unquoted_table}
        """,
        f'''
        CREATE TABLE "{quoted_table}" (
            "Id" INTEGER NOT NULL PRIMARY KEY,
            "CamelName" VARCHAR(20),
            "CreatedOn" DATE
        )
        ''',
        f'''
        CREATE VIEW "{quoted_view}"
        AS SELECT "Id", "CamelName" FROM "{quoted_table}"
        ''',
    ]

    drop_sqls = [
        f'DROP VIEW "{quoted_view}"',
        f'DROP TABLE "{quoted_table}"',
        f"DROP VIEW {unquoted_view}",
        f"DROP TABLE {unquoted_table}",
    ]

    with engine.connect() as conn:
        for stmt in create_sqls:
            conn.exec_driver_sql(stmt)
        conn.commit()

    try:
        yield {
            "unquoted_table": unquoted_table,
            "unquoted_view": unquoted_view,
            "quoted_table": quoted_table,
            "quoted_view": quoted_view,
        }
    finally:
        with engine.connect() as conn:
            for stmt in drop_sqls:
                try:
                    conn.exec_driver_sql(stmt)
                    conn.commit()
                except Exception:
                    conn.rollback()


@pytest.mark.quoting
def test_unquoted_table_round_trip_normalizes(engine, quoting_objects):
    table_name = quoting_objects["unquoted_table"]

    with engine.connect() as conn:
        insp = inspect(conn)

        assert insp.has_table(table_name) is True

        table_names = insp.get_table_names()
        assert table_name in table_names, table_names

        cols = {col["name"] for col in insp.get_columns(table_name)}
        assert cols == {"id", "plain_name", "created_on"}


@pytest.mark.quoting
def test_unquoted_view_round_trip_normalizes(engine, quoting_objects):
    view_name = quoting_objects["unquoted_view"]
    table_name = quoting_objects["unquoted_table"]

    with engine.connect() as conn:
        insp = inspect(conn)

        assert insp.has_table(view_name) is True

        view_names = insp.get_view_names()
        assert view_name in view_names, view_names

        ddl = insp.get_view_definition(view_name)
        assert ddl is not None
        assert table_name.lower() in ddl.lower()


@pytest.mark.quoting
def test_quoted_table_round_trip_preserves_case(engine, quoting_objects):
    table_name = quoting_objects["quoted_table"]

    with engine.connect() as conn:
        insp = inspect(conn)

        assert insp.has_table(quoted_name(table_name, True)) is True

        cols = {col["name"] for col in insp.get_columns(quoted_name(table_name, True))}
        assert "Id" in cols
        assert "CamelName" in cols
        assert "CreatedOn" in cols


@pytest.mark.quoting
def test_quoted_view_round_trip_preserves_case(engine, quoting_objects):
    view_name = quoting_objects["quoted_view"]
    table_name = quoting_objects["quoted_table"]

    with engine.connect() as conn:
        insp = inspect(conn)

        assert insp.has_table(quoted_name(view_name, True)) is True

        view_names = insp.get_view_names()
        assert view_name in view_names, view_names

        ddl = insp.get_view_definition(quoted_name(view_name, True))
        assert ddl is not None
        assert table_name in ddl


@pytest.mark.quoting
def test_unquoted_name_does_not_find_quoted_mixed_case_object(engine, quoting_objects):
    quoted_table = quoting_objects["quoted_table"]

    with engine.connect() as conn:
        insp = inspect(conn)
        assert insp.has_table(quoted_table) is False


@pytest.mark.quoting
def test_get_columns_with_explicit_quoted_name(engine, quoting_objects):
    table_name = quoting_objects["quoted_table"]

    with engine.connect() as conn:
        insp = inspect(conn)
        cols = insp.get_columns(quoted_name(table_name, True))

    by_name = {col["name"]: col for col in cols}
    assert set(by_name.keys()) == {"Id", "CamelName", "CreatedOn"}


@pytest.mark.quoting
def test_unquoted_identifiers_normalize_to_lowercase(db_builder, engine, name_factory):
    table_name = name_factory("sa_norm_")

    db_builder(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER NOT NULL PRIMARY KEY,
            plain_name VARCHAR(20)
        )
        """,
        f"DROP TABLE {table_name}",
    )

    with engine.connect() as connection:
        insp = inspect(connection)
        assert insp.has_table(table_name) is True

        table_names = insp.get_table_names()
        assert table_name in table_names

        cols = {col["name"] for col in insp.get_columns(table_name)}
        assert cols == {"id", "plain_name"}


@pytest.mark.quoting
def test_quoted_mixed_case_identifiers_round_trip(db_builder, engine, qident):
    table_name = "MixCaseT001"
    quoted_table = qident(table_name)
    create_sql = f"""
    CREATE TABLE {quoted_table} (
        "Id" INTEGER NOT NULL PRIMARY KEY,
        "CamelName" VARCHAR(20)
    )
    """
    drop_sql = f"DROP TABLE {quoted_table}"

    with engine.connect() as connection:
        connection.exec_driver_sql(create_sql)
        connection.commit()

        try:
            insp = inspect(connection)

            assert insp.has_table(quoted_name(table_name, True)) is True

            cols = {
                col["name"]
                for col in insp.get_columns(quoted_name(table_name, True))
            }
            assert "Id" in cols
            assert "CamelName" in cols
        finally:
            try:
                connection.exec_driver_sql(drop_sql)
                connection.commit()
            except Exception:
                connection.rollback()
                raise

@pytest.mark.quoting
def test_get_columns_with_unquoted_name_does_not_find_quoted_mixed_case_table(
    engine, quoting_objects
):
    table_name = quoting_objects["quoted_table"]

    with engine.connect() as conn:
        insp = inspect(conn)
        with pytest.raises(exc.NoSuchTableError):
            insp.get_columns(table_name)


@pytest.mark.quoting
def test_get_view_definition_with_unquoted_name_does_not_find_quoted_mixed_case_view(
    engine, quoting_objects
):
    view_name = quoting_objects["quoted_view"]

    with engine.connect() as conn:
        insp = inspect(conn)
        assert insp.get_view_definition(view_name) is None

@pytest.mark.quoting
def test_unquoted_lookup_for_real_unquoted_objects_stays_case_insensitive(
    engine, quoting_objects
):
    table_name = quoting_objects["unquoted_table"]
    view_name = quoting_objects["unquoted_view"]

    with engine.connect() as conn:
        insp = inspect(conn)

        assert insp.has_table(table_name.upper()) is True
        assert insp.has_table(view_name.upper()) is True

        cols = {col["name"] for col in insp.get_columns(table_name.upper())}
        assert cols == {"id", "plain_name", "created_on"}

        ddl = insp.get_view_definition(view_name.upper())
        assert ddl is not None
        assert table_name.lower() in ddl.lower()


@pytest.mark.quoting
def test_quoted_reserved_words_round_trip_preserves_case_and_names(
    db_builder, engine, qident, name_factory
):
    suffix = name_factory("rw_")[-8:]
    table_name = f"Order_{suffix}"
    quoted_table = qident(table_name)

    create_sql = (
        f"CREATE TABLE {quoted_table} ("
        f'"Select" INTEGER NOT NULL PRIMARY KEY, '
        f'"From" VARCHAR(20)'
        f")"
    )
    drop_sql = f"DROP TABLE {quoted_table}"

    db_builder(create_sql, drop_sql)

    with engine.connect() as connection:
        insp = inspect(connection)

        assert insp.has_table(quoted_name(table_name, True)) is True
        assert insp.has_table(table_name) is False

        cols = {
            col["name"]
            for col in insp.get_columns(quoted_name(table_name, True))
        }
        assert cols == {"Select", "From"}

@pytest.mark.quoting
@pytest.mark.temp_tables
def test_unquoted_name_does_not_find_quoted_mixed_case_temp_table(
    pinned_connection_session, name_factory, qident
):
    connection, _session = pinned_connection_session
    table_name = f"MixCaseTmp_{name_factory('qt_')[-8:]}"
    quoted_table = qident(table_name)

    try:
        create_sql = (
            f'CREATE TEMP TABLE {quoted_table} ('
            f'"Id" INTEGER NOT NULL, '
            f'"CamelName" VARCHAR(20)'
            f')'
        )
        connection.exec_driver_sql(create_sql)
        connection.commit()

        insp = inspect(connection)
        assert insp.has_table(quoted_name(table_name, True)) is True
        assert insp.has_table(table_name) is False
    finally:
        try:
            connection.exec_driver_sql(f"DROP TABLE {quoted_table}")
            connection.commit()
        except Exception:
            connection.rollback()
