from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.sql import quoted_name


def test_has_table_finds_normal_table(engine, db_builder, name_factory):
    table_name = name_factory("sa_ht_")

    db_builder(
        f"CREATE TABLE {table_name} (id INTEGER NOT NULL PRIMARY KEY)",
        f"DROP TABLE {table_name}",
    )

    with engine.connect() as connection:
        assert inspect(connection).has_table(table_name) is True


def test_has_table_returns_false_for_missing_normal_table(engine, name_factory):
    table_name = name_factory("sa_missing_")

    with engine.connect() as connection:
        assert inspect(connection).has_table(table_name) is False


def test_has_table_finds_view(engine, db_builder, name_factory):
    table_name = name_factory("sa_htv_t_")
    view_name = name_factory("sa_htv_v_")

    db_builder(
        [
            f"CREATE TABLE {table_name} (id INTEGER NOT NULL PRIMARY KEY)",
            f"CREATE VIEW {view_name} AS SELECT id FROM {table_name}",
        ],
        [
            f"DROP VIEW {view_name}",
            f"DROP TABLE {table_name}",
        ],
    )

    with engine.connect() as connection:
        assert inspect(connection).has_table(view_name) is True


def test_has_table_returns_false_for_missing_view(engine, name_factory):
    view_name = name_factory("sa_missing_v_")

    with engine.connect() as connection:
        assert inspect(connection).has_table(view_name) is False


def test_has_table_finds_temporary_table_on_same_connection(
    pinned_connection_session,
    name_factory,
):
    connection, _session = pinned_connection_session
    table_name = name_factory("tmp_ht_")

    try:
        connection.exec_driver_sql(
            f"CREATE TEMP TABLE {table_name} (id INTEGER NOT NULL PRIMARY KEY)"
        )
        connection.commit()

        assert inspect(connection).has_table(table_name) is True
    finally:
        try:
            connection.exec_driver_sql(f"DROP TABLE {table_name}")
            connection.commit()
        except Exception:
            connection.rollback()


def test_has_table_temporary_table_is_not_visible_on_other_connection(
    engine,
    pinned_connection_session,
    name_factory,
):
    connection, _session = pinned_connection_session
    table_name = name_factory("tmp_ht_other_")

    try:
        connection.exec_driver_sql(
            f"CREATE TEMP TABLE {table_name} (id INTEGER NOT NULL PRIMARY KEY)"
        )
        connection.commit()

        with engine.connect() as other_connection:
            assert inspect(other_connection).has_table(table_name) is False
    finally:
        try:
            connection.exec_driver_sql(f"DROP TABLE {table_name}")
            connection.commit()
        except Exception:
            connection.rollback()


def test_has_table_finds_quoted_table(engine, db_builder, name_factory, qident):
    table_name = f"MixedTable_{name_factory('qt_')[-8:]}"

    db_builder(
        f"CREATE TABLE {qident(table_name)} (id INTEGER NOT NULL PRIMARY KEY)",
        f"DROP TABLE {qident(table_name)}",
    )

    with engine.connect() as connection:
        assert inspect(connection).has_table(quoted_name(table_name, True)) is True


def test_has_table_finds_quoted_view(engine, db_builder, name_factory, qident):
    table_name = f"MixedSource_{name_factory('qv_')[-8:]}"
    view_name = f"MixedView_{name_factory('qv_')[-8:]}"

    db_builder(
        [
            f"CREATE TABLE {qident(table_name)} (id INTEGER NOT NULL PRIMARY KEY)",
            (
                f"CREATE VIEW {qident(view_name)} AS "
                f"SELECT id FROM {qident(table_name)}"
            ),
        ],
        [
            f"DROP VIEW {qident(view_name)}",
            f"DROP TABLE {qident(table_name)}",
        ],
    )

    with engine.connect() as connection:
        assert inspect(connection).has_table(quoted_name(view_name, True)) is True
