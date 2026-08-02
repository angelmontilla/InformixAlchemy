from __future__ import annotations

import re
import uuid

import pytest
from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    Table,
    inspect,
)

def _name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _check_tokens(sqltext: str) -> tuple[str, ...]:
    """Normalize CHECK text for semantic catalog comparisons.

    Informix can rewrite the SQL inequality operator ``<>`` as ``!=``
    during catalog storage. Both representations are normalized to the
    same token so the probe compares expressions semantically rather
    than by their original spelling.
    """
    tokens = re.findall(
        r"\b(?:a|b|and|or)\b|!=|<>|<=|>=|=|<|>|\d+",
        str(sqltext).casefold(),
    )

    return tuple(
        "<>" if token == "!=" else token
        for token in tokens
    )


@pytest.fixture(scope="module")
def official_engine(engine):
    """Reuse the mandatory live Informix engine used by the main test suite.

    These probes no longer depend on an optional ``.env.official-suites``
    file.  A normal ``pytest`` run already requires the configured Informix
    14.x database, so absence or invalid configuration is a test-session
    setup error rather than a skipped capability.
    """
    return engine


def test_explicit_use_alter_self_reference_works(official_engine):
    """Informix accepts a self reference after the table exists."""
    table_name = _name("ifx_alt_self")
    constraint_name = _name("ifx_alt_fk")
    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("parent_id", Integer),
        ForeignKeyConstraint(
            ["parent_id"],
            [f"{table_name}.id"],
            name=constraint_name,
            use_alter=True,
        ),
    )

    try:
        metadata.create_all(official_engine, checkfirst=False)

        with official_engine.connect() as connection:
            reflected = connection.exec_driver_sql(
                """
                SELECT FIRST 1 c.constrname
                FROM sysconstraints c
                JOIN systables t ON t.tabid = c.tabid
                WHERE LOWER(TRIM(t.tabname)) = LOWER(TRIM(?))
                  AND LOWER(TRIM(c.constrname)) = LOWER(TRIM(?))
                  AND c.constrtype = 'R'
                """,
                (table.name, constraint_name),
            ).scalar_one()

        assert str(reflected).strip().casefold() == constraint_name.casefold()
    finally:
        metadata.drop_all(official_engine, checkfirst=True)


def test_schema_namespaced_constraints_match_database_mode(official_engine):
    """Validate owner namespaces only where Informix actually isolates them.

    In an ANSI database, ``owner.table`` is the object identity and two owners
    can use the same table name.  A non-ANSI database has no equivalent schema
    contract for SQLAlchemy, so the dialect must advertise schemas as closed
    instead of attempting a schema-qualified duplicate creation.
    """
    dialect = official_engine.dialect

    assert dialect.supports_schemas is dialect.is_ansi_database

    if not dialect.is_ansi_database:
        assert dialect.supports_schemas is False
        return

    table_name = _name("ifx_ns_users")
    check_name = _name("ifx_same_check")
    fk_name = _name("ifx_same_fk")
    default_owner = str(dialect.default_schema_name).strip()
    alternate_owner = (
        "test_schema_2"
        if default_owner.casefold() == "test_schema"
        else "test_schema"
    )
    metadata = MetaData()

    def add_table(schema: str | None) -> Table:
        qualified = (
            f"{schema}.{table_name}.id"
            if schema
            else f"{table_name}.id"
        )
        return Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("parent_id", Integer),
            CheckConstraint("id > 0", name=check_name),
            ForeignKeyConstraint(
                ["parent_id"],
                [qualified],
                name=fk_name,
            ),
            schema=schema,
        )

    local_table = add_table(None)
    schema_table = add_table(alternate_owner)

    try:
        metadata.create_all(official_engine, checkfirst=False)

        with official_engine.connect() as connection:
            rows = connection.exec_driver_sql(
                """
                SELECT
                    TRIM(c.constrname),
                    TRIM(t.tabname),
                    TRIM(t.owner)
                FROM sysconstraints c
                JOIN systables t ON t.tabid = c.tabid
                WHERE LOWER(TRIM(t.tabname)) = LOWER(TRIM(?))
                  AND c.constrtype IN ('C', 'R')
                ORDER BY TRIM(t.owner), TRIM(c.constrname)
                """,
                (table_name,),
            ).fetchall()

        found = {
            (str(name).strip(), str(owner).strip().casefold())
            for name, _table, owner in rows
        }

        assert (check_name, default_owner.casefold()) in found
        assert (fk_name, default_owner.casefold()) in found
        assert (check_name, alternate_owner.casefold()) in found
        assert (fk_name, alternate_owner.casefold()) in found
        assert local_table.schema is None
        assert schema_table.schema == alternate_owner
    finally:
        metadata.drop_all(official_engine, checkfirst=True)


def test_check_constraint_catalog_storage(official_engine):
    """Inspect how Informix stores named, unnamed and inline CHECKs."""

    table_name = _name("ifx_check_probe")
    inline_name = _name("ifx_inline_ck")
    standalone_name = _name("ifx_table_ck")

    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        Column(
            "a",
            Integer,
            CheckConstraint(
                "a > 1 AND a < 5",
                name=inline_name,
            ),
        ),
        Column(
            "b",
            Integer,
        ),
        CheckConstraint(
            "b = 1 OR (b > 2 AND b < 5)",
            name=standalone_name,
        ),
        CheckConstraint(
            "a <> b",
        ),
    )

    try:
        metadata.create_all(
            official_engine,
            checkfirst=False,
        )

        with official_engine.connect() as connection:
            rows = connection.exec_driver_sql(
                """
                SELECT
                    c.constrid,
                    TRIM(c.constrname),
                    TRIM(c.owner),
                    ch.type,
                    ch.seqno,
                    ch.checktext
                FROM sysconstraints c
                JOIN systables t
                  ON t.tabid = c.tabid
                JOIN syschecks ch
                  ON ch.constrid = c.constrid
                WHERE LOWER(TRIM(t.tabname)) =
                      LOWER(TRIM(?))
                  AND c.constrtype = 'C'
                ORDER BY
                    c.constrid,
                    ch.type,
                    ch.seqno
                """,
                (table_name,),
            ).fetchall()

        print("\nCHECK CATALOG ROWS")

        for row in rows:
            print(
                {
                    "constrid": row[0],
                    "constrname": (
                        str(row[1]).strip()
                        if row[1] is not None
                        else None
                    ),
                    "owner": (
                        str(row[2]).strip()
                        if row[2] is not None
                        else None
                    ),
                    "type": (
                        str(row[3]).strip()
                        if row[3] is not None
                        else None
                    ),
                    "seqno": row[4],
                    "checktext_repr": repr(row[5]),
                }
            )

        assert rows

        text_rows = [
            row
            for row in rows
            if (
                row[3] is not None
                and str(row[3]).strip().upper() == "T"
            )
        ]

        assert text_rows

        reflected_names = {
            str(row[1]).strip().casefold()
            for row in text_rows
            if row[1] is not None
        }

        assert inline_name.casefold() in reflected_names
        assert standalone_name.casefold() in reflected_names

        # Informix must also assign a physical name to the unnamed CHECK.
        assert len(reflected_names) == 3

        assert table.name == table_name

    finally:
        metadata.drop_all(
            official_engine,
            checkfirst=True,
        )

def test_check_constraint_reflection_roundtrip(official_engine):
    """Reflect named, unnamed, inline and fragmented CHECK constraints."""

    table_name = _name("ifx_check_reflect")
    inline_name = _name("ifx_inline_ck")
    standalone_name = _name("ifx_table_ck")

    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=False,
        ),
        Column(
            "a",
            Integer,
            CheckConstraint(
                "a > 1 AND a < 5",
                name=inline_name,
            ),
        ),
        Column(
            "b",
            Integer,
        ),
        CheckConstraint(
            (
                "b = 1 OR "
                "(b > 2 AND b < 5000) OR "
                "b = 9000"
            ),
            name=standalone_name,
        ),
        CheckConstraint(
            "a <> b",
        ),
    )

    expected_inline = _check_tokens(
        "a > 1 AND a < 5"
    )
    expected_standalone = _check_tokens(
        (
            "b = 1 OR "
            "(b > 2 AND b < 5000) OR "
            "b = 9000"
        )
    )
    expected_unnamed = _check_tokens(
        "a <> b"
    )

    try:
        metadata.create_all(
            official_engine,
            checkfirst=False,
        )

        with official_engine.connect() as connection:
            inspector = inspect(connection)

            reflected = inspector.get_check_constraints(
                table_name
            )

        print("\nREFLECTED CHECK CONSTRAINTS")

        for item in reflected:
            print(
                {
                    "name": (
                        str(item.get("name"))
                        if item.get("name") is not None
                        else None
                    ),
                    "sqltext": item.get("sqltext"),
                    "tokens": _check_tokens(
                        item.get("sqltext", "")
                    ),
                }
            )

        assert len(reflected) == 3

        by_expression = {
            _check_tokens(item["sqltext"]): item
            for item in reflected
        }

        assert set(by_expression) == {
            expected_inline,
            expected_standalone,
            expected_unnamed,
        }

        assert (
            str(
                by_expression[
                    expected_inline
                ]["name"]
            ).casefold()
            == inline_name.casefold()
        )

        assert (
            str(
                by_expression[
                    expected_standalone
                ]["name"]
            ).casefold()
            == standalone_name.casefold()
        )

        unnamed = by_expression[expected_unnamed]

        assert "name" in unnamed
        assert unnamed["name"] is not None
        assert str(unnamed["name"]).strip()

        assert table.name == table_name

    finally:
        metadata.drop_all(
            official_engine,
            checkfirst=True,
        )
