from __future__ import annotations

import pytest
from sqlalchemy import column, literal_column, select, table
from sqlalchemy.sql import quoted_name

from IfxAlchemy.pyodbc import IfxDialect_pyodbc

try:
    from sqlalchemy.sql.ddl import CreateView
except ImportError:
    CreateView = None


def _sqlalchemy21_select_into_available() -> bool:
    stmt = select(column("id"))
    return hasattr(stmt, "into")


_SELECT_INTO_AVAILABLE = _sqlalchemy21_select_into_available()
_CREATE_VIEW_AVAILABLE = CreateView is not None


if not (_SELECT_INTO_AVAILABLE and _CREATE_VIEW_AVAILABLE):

    @pytest.mark.sqlalchemy_suite
    def test_sqlalchemy21_constructs_are_not_available_on_sqlalchemy20():
        assert _SELECT_INTO_AVAILABLE is False
        assert _CREATE_VIEW_AVAILABLE is False

else:

    def _compiled(stmt):
        return " ".join(str(stmt.compile(dialect=IfxDialect_pyodbc())).split())


    @pytest.mark.sqlalchemy_suite
    def test_sqlalchemy21_create_view_with_simple_columns_compiles_exact_shape():
        source = table("sa21_source", column("id"), column("name"))

        stmt = CreateView(
            select(source.c.id, source.c.name),
            "sa21_view",
        )

        compiled = _compiled(stmt)

        assert compiled.startswith("CREATE VIEW sa21_view ")
        assert " AS SELECT " in compiled.upper()
        assert compiled == (
            "CREATE VIEW sa21_view AS SELECT "
            "sa21_source.id, sa21_source.name FROM sa21_source"
        )

    @pytest.mark.sqlalchemy_suite
    def test_sqlalchemy21_create_view_with_quoted_name_compiles_exact_shape():
        source = table("sa21_source", column("id"))

        stmt = CreateView(
            select(source.c.id),
            quoted_name("MixedView", True),
        )

        compiled = _compiled(stmt)

        assert compiled.startswith('CREATE VIEW "MixedView" ')
        assert " AS SELECT " in compiled.upper()
        assert compiled == (
            'CREATE VIEW "MixedView" AS SELECT '
            "sa21_source.id FROM sa21_source"
        )

    @pytest.mark.sqlalchemy_suite
    def test_sqlalchemy21_create_view_with_where_compiles_exact_shape():
        source = table("sa21_source", column("id"))

        stmt = CreateView(
            select(source.c.id).where(source.c.id == 5),
            "sa21_filtered",
        )

        compiled = _compiled(stmt)

        assert compiled.startswith("CREATE VIEW sa21_filtered ")
        assert " AS SELECT " in compiled.upper()
        assert compiled == (
            "CREATE VIEW sa21_filtered AS SELECT "
            "sa21_source.id FROM sa21_source WHERE sa21_source.id = 5"
        )

    @pytest.mark.sqlalchemy_suite
    def test_sqlalchemy21_select_into_create_table_as_compiles_exact_shape():
        source = table("sa21_source", column("id"), column("name"))

        stmt = select(source.c.id, source.c.name).into("sa21_target")

        compiled = _compiled(stmt)

        assert compiled.startswith("CREATE TABLE sa21_target ")
        assert " AS SELECT " in compiled.upper()
        assert compiled == (
            "CREATE TABLE sa21_target AS SELECT "
            "sa21_source.id, sa21_source.name FROM sa21_source"
        )

    @pytest.mark.sqlalchemy_suite
    def test_sqlalchemy21_select_into_quoted_name_compiles_exact_shape():
        source = table("sa21_source", column("id"))

        stmt = select(source.c.id).into(quoted_name("TablaDestino", True))

        compiled = _compiled(stmt)

        assert compiled.startswith('CREATE TABLE "TablaDestino" ')
        assert " AS SELECT " in compiled.upper()
        assert compiled == (
            'CREATE TABLE "TablaDestino" AS SELECT '
            "sa21_source.id FROM sa21_source"
        )

    @pytest.mark.sqlalchemy_suite
    def test_sqlalchemy21_ctas_with_labeled_columns_compiles_exact_shape():
        source = table("sa21_source", column("id"), column("name"))

        stmt = select(
            source.c.id.label("ident"),
            source.c.name.label("nom"),
        ).into("sa21_labeled")

        compiled = _compiled(stmt)

        assert compiled.startswith("CREATE TABLE sa21_labeled ")
        assert " AS SELECT " in compiled.upper()
        assert compiled == (
            "CREATE TABLE sa21_labeled AS SELECT "
            "sa21_source.id AS ident, sa21_source.name AS nom "
            "FROM sa21_source"
        )

    @pytest.mark.sqlalchemy_suite
    def test_sqlalchemy21_ctas_with_literal_columns_compiles_exact_shape():
        stmt = select(
            literal_column("1").label("one"),
            literal_column("'x'").label("txt"),
        ).into("sa21_literal")

        compiled = _compiled(stmt)

        assert compiled.startswith("CREATE TABLE sa21_literal ")
        assert " AS SELECT " in compiled.upper()
        assert compiled == (
            "CREATE TABLE sa21_literal AS SELECT 1 AS one, 'x' AS txt "
            "FROM systables WHERE tabid = 1"
        )
