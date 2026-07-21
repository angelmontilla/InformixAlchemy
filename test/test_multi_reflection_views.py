from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope


def test_multi_constraint_reflection_includes_views_with_empty_metadata(
    engine,
    name_factory,
):
    table_name = name_factory("sa_mrv_t_")
    view_name = name_factory("sa_mrv_v_")

    with engine.connect() as connection:
        connection.exec_driver_sql(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER NOT NULL PRIMARY KEY,
                code VARCHAR(20)
            )
            """
        )
        connection.exec_driver_sql(
            f"""
            CREATE VIEW {view_name}
            AS SELECT id, code FROM {table_name}
            """
        )
        connection.commit()

        try:
            inspector = inspect(connection)
            options = {
                "filter_names": [table_name, view_name],
                "kind": ObjectKind.TABLE | ObjectKind.VIEW,
                "scope": ObjectScope.DEFAULT,
            }

            foreign_keys = inspector.get_multi_foreign_keys(**options)
            indexes = inspector.get_multi_indexes(**options)
            primary_keys = inspector.get_multi_pk_constraint(**options)
            unique_constraints = inspector.get_multi_unique_constraints(
                **options
            )

            view_key = (None, view_name)
            table_key = (None, table_name)

            assert table_key in foreign_keys
            assert table_key in indexes
            assert table_key in primary_keys
            assert table_key in unique_constraints

            assert view_key in foreign_keys
            assert view_key in indexes
            assert view_key in primary_keys
            assert view_key in unique_constraints

            assert foreign_keys[view_key] == []
            assert indexes[view_key] == []
            assert unique_constraints[view_key] == []
            assert primary_keys[view_key] == {
                "name": None,
                "constrained_columns": [],
            }
        finally:
            try:
                connection.exec_driver_sql(f"DROP VIEW {view_name}")
                connection.commit()
            except Exception:
                connection.rollback()

            try:
                connection.exec_driver_sql(f"DROP TABLE {table_name}")
                connection.commit()
            except Exception:
                connection.rollback()
