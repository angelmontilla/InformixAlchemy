from __future__ import annotations

import pytest
from sqlalchemy import inspect, text

@pytest.mark.temp_tables
def test_temp_table_dml_on_pinned_connection(pinned_connection_session, name_factory):
    connection, session = pinned_connection_session
    table_name = name_factory("tmp_sa_")

    create_sql = f"""
    CREATE TEMP TABLE {table_name} (
        id INTEGER NOT NULL,
        name VARCHAR(20) NOT NULL,
        qty INTEGER NOT NULL
    )
    """

    insert_sql = text(
        f"INSERT INTO {table_name} (id, name, qty) VALUES (:id, :name, :qty)"
    )

    try:
        connection.exec_driver_sql(create_sql)
        connection.commit()

        session.execute(insert_sql, {"id": 1, "name": "alpha", "qty": 10})
        assert session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one() == 1

        session.rollback()
        assert session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one() == 0

        session.execute(insert_sql, {"id": 1, "name": "alpha", "qty": 10})
        session.commit()
        assert session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one() == 1

        session.execute(
            text(f"UPDATE {table_name} SET qty = :qty WHERE id = :id"),
            {"qty": 20, "id": 1},
        )
        session.commit()
        assert session.execute(
            text(f"SELECT qty FROM {table_name} WHERE id = :id"),
            {"id": 1},
        ).scalar_one() == 20

        session.execute(
            text(f"DELETE FROM {table_name} WHERE id = :id"),
            {"id": 1},
        )
        session.rollback()
        assert session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one() == 1

        session.execute(
            text(f"DELETE FROM {table_name} WHERE id = :id"),
            {"id": 1},
        )
        session.commit()
        assert session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one() == 0

    finally:
        try:
            connection.exec_driver_sql(f"DROP TABLE {table_name}")
            connection.commit()
        except Exception:
            connection.rollback()


def test_has_table_finds_temp_table_on_same_connection(pinned_connection_session, name_factory):
    connection, _session = pinned_connection_session
    table_name = name_factory("tmp_has_")

    try:
        connection.exec_driver_sql(
            f"CREATE TEMP TABLE {table_name} (id INTEGER NOT NULL PRIMARY KEY)"
        )
        connection.commit()

        insp = inspect(connection)
        assert insp.has_table(table_name) is True

    finally:
        try:
            connection.exec_driver_sql(f"DROP TABLE {table_name}")
            connection.commit()
        except Exception:
            connection.rollback()


@pytest.mark.temp_tables
def test_has_table_returns_false_after_temp_table_drop_on_same_connection(
    pinned_connection_session, name_factory
):
    connection, _session = pinned_connection_session
    table_name = name_factory("tmp_drop_")

    connection.exec_driver_sql(
        f"CREATE TEMP TABLE {table_name} (id INTEGER NOT NULL PRIMARY KEY)"
    )
    connection.commit()

    insp = inspect(connection)
    assert insp.has_table(table_name) is True

    connection.exec_driver_sql(f"DROP TABLE {table_name}")
    connection.commit()

    insp_after_drop = inspect(connection)
    assert insp_after_drop.has_table(table_name) is False


@pytest.mark.temp_tables
def test_get_temp_table_names_is_explicitly_unsupported_and_returns_empty_list(
    pinned_connection_session, name_factory
):
    connection, _session = pinned_connection_session
    table_name = name_factory("tmp_names_")

    try:
        connection.exec_driver_sql(
            f"CREATE TEMP TABLE {table_name} (id INTEGER NOT NULL PRIMARY KEY)"
        )
        connection.commit()

        insp = inspect(connection)
        assert insp.has_table(table_name) is True
        assert insp.get_temp_table_names() == []
    finally:
        try:
            connection.exec_driver_sql(f"DROP TABLE {table_name}")
            connection.commit()
        except Exception:
            connection.rollback()


def test_get_temp_view_names_is_explicitly_unsupported_and_returns_empty_list(engine):
    with engine.connect() as connection:
        insp = inspect(connection)
        assert insp.get_temp_view_names() == []
