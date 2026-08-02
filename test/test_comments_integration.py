from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, inspect
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import (
    DropColumnComment,
    DropTableComment,
    SetColumnComment,
    SetTableComment,
)
from sqlalchemy.sql import quoted_name

from IfxAlchemy.comments import (
    COLUMN_COMMENT_CATALOG,
    TABLE_COMMENT_CATALOG,
)


pytestmark = pytest.mark.requires_informix


def test_table_and_column_comments_round_trip(engine, name_factory, qident):
    suffix = name_factory("c_")[-8:]
    table_name = quoted_name(f"comment ' table {suffix}", True)
    column_name = quoted_name(f"value % ' column {suffix}", True)
    view_name = f"comment_view_{suffix}"
    missing_name = f"comment_missing_{suffix}"

    table_comment = "tabla 試蛇ẟΩ✨ 🎩🤷\u200d♀️\n% ' \\ final"
    column_comment = "columna é試☁️✨🐍🧙\u200d♂️\r\x0c\x0b% ' \\ final"
    updated_table_comment = "tabla actualizada 🝑"
    updated_column_comment = "columna actualizada 🧙‍♀️"

    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column(column_name, Integer, comment=column_comment),
        comment=table_comment,
    )

    try:
        with engine.begin() as connection:
            metadata.create_all(connection)
            connection.exec_driver_sql(
                f"CREATE VIEW {qident(view_name)} AS "
                f"SELECT id FROM {qident(str(table_name))}"
            )

        inspector = inspect(engine)

        assert inspector.get_table_comment(str(table_name)) == {
            "text": table_comment
        }
        columns = {
            str(column["name"]): column
            for column in inspector.get_columns(str(table_name))
        }
        assert columns[str(column_name)]["comment"] == column_comment
        assert inspector.get_table_comment(view_name) == {"text": None}

        multi = inspector.get_multi_table_comment(
            filter_names=[str(table_name), view_name, missing_name],
            kind=ObjectKind.ANY,
            scope=ObjectScope.ANY,
        )
        assert multi == {
            (None, str(table_name)): {"text": table_comment},
            (None, view_name): {"text": None},
        }

        reflected_table_names = {str(name) for name in inspector.get_table_names()}
        assert TABLE_COMMENT_CATALOG not in reflected_table_names
        assert COLUMN_COMMENT_CATALOG not in reflected_table_names

        with pytest.raises(NoSuchTableError):
            inspector.get_table_comment(missing_name)

        table.comment = updated_table_comment
        table.c[column_name].comment = updated_column_comment
        with engine.begin() as connection:
            connection.execute(SetTableComment(table))
            connection.execute(SetColumnComment(table.c[column_name]))

        inspector.clear_cache()
        assert inspector.get_table_comment(str(table_name)) == {
            "text": updated_table_comment
        }
        columns = {
            str(column["name"]): column
            for column in inspector.get_columns(str(table_name))
        }
        assert columns[str(column_name)]["comment"] == updated_column_comment

        with engine.begin() as connection:
            connection.execute(DropColumnComment(table.c[column_name]))
            connection.execute(DropTableComment(table))

        inspector.clear_cache()
        assert inspector.get_table_comment(str(table_name)) == {"text": None}
        columns = {
            str(column["name"]): column
            for column in inspector.get_columns(str(table_name))
        }
        assert columns[str(column_name)]["comment"] is None
    finally:
        with engine.connect() as connection:
            try:
                connection.exec_driver_sql(f"DROP VIEW {qident(view_name)}")
                connection.commit()
            except Exception:
                connection.rollback()
        metadata.drop_all(engine, checkfirst=True)
