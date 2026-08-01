from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Sequence, String, Table, inspect, select
from sqlalchemy import exc as sa_exc
from sqlalchemy.sql import quoted_name

from IfxAlchemy import (
    CreateSynonym,
    DropSynonym,
    SynonymName,
    SynonymTarget,
)
from IfxAlchemy.base import IfxDialect
from IfxAlchemy.reflection import IfxInspector, IfxReflector


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)


class _SynonymCatalogConnection:
    def __init__(self, rows_by_name=None, all_rows=None):
        self.rows_by_name = rows_by_name or {}
        self.all_rows = list(all_rows or [])
        self.statements = []

    def exec_driver_sql(self, statement, parameters=()):
        self.statements.append((statement, tuple(parameters)))
        if "FROM systables s" not in statement or "JOIN syssyntable y" not in statement:
            raise AssertionError(statement)

        # Queries for one synonym append the requested name after owner.
        if "s.tabname = ?" in statement:
            return _Rows(self.rows_by_name.get(str(parameters[-1]), []))
        return _Rows(self.all_rows)


def _dialect(*, ansi=False):
    dialect = IfxDialect()
    dialect.is_ansi_database = ansi
    dialect.default_schema_name = "informix"
    return dialect


def _compile(element, *, ansi=False):
    return str(element.compile(dialect=_dialect(ansi=ansi)))


def test_synonym_constructs_are_publicly_exported():
    from IfxAlchemy import CreateSynonym as exported_create
    from IfxAlchemy import DropSynonym as exported_drop

    assert exported_create is CreateSynonym
    assert exported_drop is DropSynonym


def test_create_private_local_table_synonym_compiles():
    metadata = MetaData()
    customers = Table("customers", metadata, Column("id", Integer))

    sql = _compile(
        CreateSynonym(
            "customer_alias",
            customers,
            public=False,
            if_not_exists=True,
        )
    )

    assert sql == (
        "CREATE PRIVATE SYNONYM IF NOT EXISTS "
        "customer_alias FOR customers"
    )


def test_create_public_synonym_compiles_only_for_non_ansi_database():
    construct = CreateSynonym(
        "customer_alias",
        SynonymTarget("customers", owner="app"),
        public=True,
    )

    assert _compile(construct) == (
        "CREATE PUBLIC SYNONYM customer_alias FOR app.customers"
    )

    with pytest.raises(sa_exc.CompileError, match="ANSI databases"):
        _compile(construct, ansi=True)


def test_omitted_modifier_uses_native_database_default():
    construct = CreateSynonym("customer_alias", "customers")

    # The SQL is intentionally identical: Informix treats the omitted
    # modifier as PUBLIC in non-ANSI databases and PRIVATE in MODE ANSI.
    assert _compile(construct) == "CREATE SYNONYM customer_alias FOR customers"
    assert _compile(construct, ansi=True) == (
        "CREATE SYNONYM customer_alias FOR customers"
    )


def test_local_sequence_and_view_targets_are_structured():
    sequence = Sequence("order_numbers", schema="app")
    sequence_sql = _compile(CreateSynonym("order_no", sequence))
    view_sql = _compile(
        CreateSynonym(
            "active_customers",
            SynonymTarget("v_active_customers", owner="app", kind="view"),
        )
    )

    assert sequence_sql == "CREATE SYNONYM order_no FOR app.order_numbers"
    assert view_sql == (
        "CREATE SYNONYM active_customers FOR app.v_active_customers"
    )


def test_remote_database_and_server_target_compiles_natively():
    same_server = CreateSynonym(
        "summary_alias",
        SynonymTarget(
            "summary_data",
            owner="jean",
            database="payables",
            kind="table",
        ),
    )
    remote_server = CreateSynonym(
        "summary_alias",
        SynonymTarget(
            "summary_data",
            owner="jean",
            database="payables",
            server="phoenix",
            kind="table",
        ),
    )

    assert _compile(same_server) == (
        "CREATE SYNONYM summary_alias FOR payables:jean.summary_data"
    )
    assert _compile(remote_server) == (
        "CREATE SYNONYM summary_alias FOR payables@phoenix:jean.summary_data"
    )


def test_quoted_identifiers_are_quoted_component_by_component():
    sql = _compile(
        CreateSynonym(
            SynonymName(
                quoted_name("Customer Alias", True),
                owner=quoted_name("App Owner", True),
            ),
            SynonymTarget(
                quoted_name("Customer View", True),
                owner=quoted_name("Target Owner", True),
                database=quoted_name("Sales DB", True),
            ),
            public=False,
        )
    )

    assert sql == (
        'CREATE PRIVATE SYNONYM "App Owner"."Customer Alias" '
        'FOR "Sales DB":"Target Owner"."Customer View"'
    )


def test_drop_synonym_if_exists_compiles_idempotently():
    assert _compile(
        DropSynonym(
            SynonymName("customer_alias", owner="app"),
            if_exists=True,
        )
    ) == "DROP SYNONYM IF EXISTS app.customer_alias"


def test_qualified_raw_strings_are_rejected_instead_of_executed():
    with pytest.raises(sa_exc.ArgumentError, match="structured identifier"):
        CreateSynonym("app.customer_alias", "customers")

    with pytest.raises(sa_exc.ArgumentError, match="structured identifier"):
        CreateSynonym(
            "customer_alias",
            "payables@phoenix:jean.summary_data",
        )

    with pytest.raises(sa_exc.ArgumentError, match="sequence objects"):
        SynonymTarget(
            "sequence_name",
            database="otherdb",
            kind="sequence",
        )


def test_table_accepts_opt_in_synonym_resolution_option():
    table = Table(
        "customer_alias",
        MetaData(),
        Column("id", Integer),
        informix_resolve_synonyms=True,
    )

    assert table.dialect_options["informix"]["resolve_synonyms"] is True
    assert "informix_resolve_synonyms" in IfxDialect.reflection_options
    assert IfxDialect.inspector is IfxInspector


def test_reflection_returns_private_public_local_and_remote_metadata():
    rows = [
        (
            101,
            "customer_alias",
            "informix",
            "P",
            None,
            None,
            None,
            None,
            200,
            "customers",
            "informix",
            "T",
        ),
        (
            102,
            "remote_summary",
            "creator",
            "S",
            "phoenix",
            "payables",
            "jean",
            "summary",
            None,
            None,
            None,
            None,
        ),
    ]
    connection = _SynonymCatalogConnection(all_rows=rows)
    reflector = IfxReflector(_dialect())

    synonyms = reflector.get_synonyms(connection)

    assert synonyms[0] == {
        "name": "customer_alias",
        "schema": None,
        "owner": "informix",
        "public": False,
        "target": {
            "name": "customers",
            "owner": "informix",
            "database": None,
            "server": None,
            "type": "table",
            "local": True,
        },
        "target_name": "customers",
        "target_schema": "informix",
        "target_database": None,
        "target_server": None,
        "target_type": "table",
    }
    assert synonyms[1]["public"] is True
    assert synonyms[1]["target"] == {
        "name": "summary",
        "owner": "jean",
        "database": "payables",
        "server": "phoenix",
        "type": None,
        "local": False,
    }


def test_get_synonym_names_and_has_synonym_use_catalog_without_tables():
    private_row = (
        101,
        "customer_alias",
        "informix",
        "P",
        None,
        None,
        None,
        None,
        200,
        "customers",
        "informix",
        "T",
    )
    connection = _SynonymCatalogConnection(
        rows_by_name={"customer_alias": [private_row]},
        all_rows=[private_row],
    )
    reflector = IfxReflector(_dialect())

    assert reflector.get_synonym_names(connection) == ["customer_alias"]
    assert reflector.has_synonym(connection, "customer_alias") is True
    assert reflector.has_synonym(connection, "missing_alias") is False


def test_private_synonym_precedes_same_named_public_synonym():
    private_row = (
        105,
        "customer_alias",
        "informix",
        "P",
        None,
        None,
        None,
        None,
        201,
        "private_customers",
        "informix",
        "T",
    )
    public_row = (
        106,
        "customer_alias",
        "other_user",
        "S",
        None,
        None,
        None,
        None,
        202,
        "public_customers",
        "other_user",
        "T",
    )
    connection = _SynonymCatalogConnection(
        rows_by_name={"customer_alias": [private_row, public_row]}
    )
    reflector = IfxReflector(_dialect())

    name, schema, synonym = reflector._resolve_reflection_target(
        connection,
        "customer_alias",
        None,
        {"informix_resolve_synonyms": True},
    )

    assert name == "private_customers"
    assert schema is None
    assert synonym["public"] is False


def test_local_synonym_chain_resolution_and_cycle_detection():
    alias_one = (
        101,
        "alias_one",
        "informix",
        "P",
        None,
        None,
        None,
        None,
        102,
        "alias_two",
        "informix",
        "P",
    )
    alias_two_to_table = (
        102,
        "alias_two",
        "informix",
        "P",
        None,
        None,
        None,
        None,
        200,
        "customers",
        "informix",
        "T",
    )
    connection = _SynonymCatalogConnection(
        rows_by_name={
            "alias_one": [alias_one],
            "alias_two": [alias_two_to_table],
        }
    )
    reflector = IfxReflector(_dialect())

    name, schema, first = reflector._resolve_reflection_target(
        connection,
        "alias_one",
        None,
        {"informix_resolve_synonyms": True},
    )

    assert name == "customers"
    assert schema is None
    assert first["name"] == "alias_one"

    alias_two_to_one = (
        102,
        "alias_two",
        "informix",
        "P",
        None,
        None,
        None,
        None,
        101,
        "alias_one",
        "informix",
        "P",
    )
    cycle_connection = _SynonymCatalogConnection(
        rows_by_name={
            "alias_one": [alias_one],
            "alias_two": [alias_two_to_one],
        }
    )

    with pytest.raises(sa_exc.UnreflectableTableError, match="cycle"):
        reflector._resolve_reflection_target(
            cycle_connection,
            "alias_one",
            None,
            {"informix_resolve_synonyms": True},
        )


def test_has_table_does_not_mask_sequence_or_cycle_resolution_errors():
    sequence_alias = (
        104,
        "sequence_alias",
        "informix",
        "P",
        None,
        None,
        None,
        None,
        300,
        "order_numbers",
        "informix",
        "Q",
    )
    reflector = IfxReflector(_dialect())
    connection = _SynonymCatalogConnection(
        rows_by_name={"sequence_alias": [sequence_alias]}
    )

    with pytest.raises(sa_exc.UnreflectableTableError, match="sequence"):
        reflector.has_table(
            connection,
            "sequence_alias",
            informix_resolve_synonyms=True,
        )


def test_remote_synonym_resolution_is_explicitly_unreflectable():
    remote = (
        103,
        "remote_summary",
        "informix",
        "P",
        "phoenix",
        "payables",
        "jean",
        "summary",
        None,
        None,
        None,
        None,
    )
    reflector = IfxReflector(_dialect())
    connection = _SynonymCatalogConnection(
        rows_by_name={"remote_summary": [remote]}
    )

    with pytest.raises(sa_exc.UnreflectableTableError, match="remote synonym"):
        reflector._resolve_reflection_target(
            connection,
            "remote_summary",
            None,
            {"informix_resolve_synonyms": True},
        )


@pytest.mark.requires_informix
def test_synonym_table_view_sequence_reflection_autoload_and_drop(
    engine,
    name_factory,
):
    suffix = name_factory("syn_")[-10:]
    table_name = f"sa_syn_table_{suffix}"
    view_name = f"sa_syn_view_{suffix}"
    sequence_name = f"sa_syn_seq_{suffix}"
    table_alias = f"sa_syn_ta_{suffix}"
    view_alias = f"sa_syn_va_{suffix}"
    sequence_alias = f"sa_syn_qa_{suffix}"

    metadata = MetaData()
    target = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(40), nullable=False),
    )
    sequence = Sequence(sequence_name)

    aliases = [table_alias, view_alias, sequence_alias]

    try:
        with engine.begin() as connection:
            for alias in aliases:
                connection.execute(DropSynonym(alias, if_exists=True))
            target.create(connection)
            connection.execute(
                target.insert(),
                [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            )
            connection.exec_driver_sql(
                f"CREATE VIEW {view_name} AS "
                f"SELECT id, name FROM {table_name} WHERE id >= 2"
            )
            sequence.create(connection)
            connection.execute(
                CreateSynonym(table_alias, target, if_not_exists=True)
            )
            connection.execute(
                CreateSynonym(
                    view_alias,
                    SynonymTarget(view_name, kind="view"),
                    if_not_exists=True,
                )
            )
            connection.execute(
                CreateSynonym(sequence_alias, sequence, if_not_exists=True)
            )

        with engine.connect() as connection:
            inspector = inspect(connection)
            synonym_names = inspector.get_synonym_names()
            assert table_alias in synonym_names
            assert view_alias in synonym_names
            assert sequence_alias in synonym_names
            assert inspector.has_synonym(table_alias)
            assert table_alias not in inspector.get_table_names()
            assert view_alias not in inspector.get_view_names()

            synonym_map = {
                item["name"]: item for item in inspector.get_synonyms()
            }
            assert synonym_map[table_alias]["target_type"] == "table"
            assert synonym_map[view_alias]["target_type"] == "view"
            assert synonym_map[sequence_alias]["target_type"] == "sequence"

            reflected_table = Table(
                table_alias,
                MetaData(),
                informix_resolve_synonyms=True,
                autoload_with=connection,
            )
            assert list(reflected_table.c.keys()) == ["id", "name"]
            assert connection.execute(
                select(reflected_table.c.id, reflected_table.c.name)
                .order_by(reflected_table.c.id)
            ).all() == [(1, "Alice"), (2, "Bob")]

            reflected_view = Table(
                view_alias,
                MetaData(),
                informix_resolve_synonyms=True,
                autoload_with=connection,
            )
            assert list(reflected_view.c.keys()) == ["id", "name"]
            assert connection.execute(
                select(reflected_view.c.id, reflected_view.c.name)
            ).all() == [(2, "Bob")]

        with engine.begin() as connection:
            for alias in aliases:
                connection.execute(DropSynonym(alias, if_exists=True))
                connection.execute(DropSynonym(alias, if_exists=True))

        with engine.connect() as connection:
            inspector = inspect(connection)
            assert all(not inspector.has_synonym(alias) for alias in aliases)
    finally:
        with engine.begin() as connection:
            for alias in aliases:
                try:
                    connection.execute(DropSynonym(alias, if_exists=True))
                except Exception:
                    pass
            try:
                connection.exec_driver_sql(f"DROP VIEW {view_name}")
            except Exception:
                pass
            try:
                sequence.drop(connection)
            except Exception:
                pass
            try:
                target.drop(connection)
            except Exception:
                pass


@pytest.mark.requires_informix
def test_public_private_synonym_reflection_on_non_ansi_database(
    engine,
    name_factory,
):
    if engine.dialect.is_ansi_database:
        pytest.skip("PUBLIC and PRIVATE are invalid in an ANSI database")

    suffix = name_factory("synvis_")[-10:]
    table_name = f"sa_synvis_t_{suffix}"
    private_alias = f"sa_synvis_p_{suffix}"
    public_alias = f"sa_synvis_s_{suffix}"
    metadata = MetaData()
    target = Table(table_name, metadata, Column("id", Integer))

    try:
        with engine.begin() as connection:
            target.create(connection)
            connection.execute(
                CreateSynonym(private_alias, target, public=False)
            )
            connection.execute(
                CreateSynonym(public_alias, target, public=True)
            )

        with engine.connect() as connection:
            synonym_map = {
                item["name"]: item for item in inspect(connection).get_synonyms()
            }
            assert synonym_map[private_alias]["public"] is False
            assert synonym_map[public_alias]["public"] is True
    finally:
        with engine.begin() as connection:
            connection.execute(DropSynonym(private_alias, if_exists=True))
            connection.execute(DropSynonym(public_alias, if_exists=True))
            try:
                target.drop(connection)
            except Exception:
                pass


@pytest.mark.requires_informix
def test_quoted_synonym_reflection_and_autoload(engine, name_factory):
    suffix = name_factory("synq_")[-8:]
    table_name = f"sa_synq_t_{suffix}"
    alias = quoted_name(f"Synonym Alias {suffix}", True)
    metadata = MetaData()
    target = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("label", String(30)),
    )

    try:
        with engine.begin() as connection:
            connection.execute(DropSynonym(alias, if_exists=True))
            target.create(connection)
            connection.execute(target.insert().values(id=1, label="quoted"))
            connection.execute(CreateSynonym(alias, target))

        with engine.connect() as connection:
            inspector = inspect(connection)
            assert inspector.has_synonym(alias)
            assert str(alias) in {str(name) for name in inspector.get_synonym_names()}

            reflected = Table(
                alias,
                MetaData(),
                informix_resolve_synonyms=True,
                autoload_with=connection,
            )
            assert list(reflected.c.keys()) == ["id", "label"]
            assert connection.execute(
                select(reflected.c.id, reflected.c.label)
            ).one() == (1, "quoted")
    finally:
        with engine.begin() as connection:
            connection.execute(DropSynonym(alias, if_exists=True))
            try:
                target.drop(connection)
            except Exception:
                pass
