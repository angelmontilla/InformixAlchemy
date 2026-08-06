from __future__ import annotations

from contextlib import suppress
from decimal import Decimal

import pytest
from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    func,
    insert,
    inspect,
    text,
)
from sqlalchemy import exc
from sqlalchemy.schema import CreateIndex, CreateTable
from sqlalchemy.sql import quoted_name

from IfxAlchemy import (
    AlterIndexCluster,
    DisableIndex,
    EnableIndex,
    SetIndexMode,
    SetIndexVisibility,
)
from IfxAlchemy.base import IfxDialect
from IfxAlchemy.reflection import IfxReflector


@pytest.fixture
def dialect():
    value = IfxDialect()
    value.default_schema_name = "informix"
    return value


def _table(metadata=None):
    metadata = metadata or MetaData()
    return Table(
        "orders",
        metadata,
        Column("id", Integer),
        Column("customer_id", Integer),
        Column("status", String(20)),
        Column("payload", String(100)),
    )


def _sql(index, dialect):
    return str(CreateIndex(index).compile(dialect=dialect))


def test_index_construct_arguments_register_public_options(dialect):
    table = _table()
    index = Index("ix_options", table.c.id)
    options = index.dialect_options["informix"]
    for name in (
        "where", "online", "fillfactor", "dbspace", "using", "opclass",
        "fragment_by", "hash_on", "buckets", "compressed", "mode", "visible",
        "amparam",
    ):
        assert name in options


def test_online_fillfactor_and_dbspace_compile(dialect):
    table = _table()
    index = Index(
        "ix_orders_customer_online",
        table.c.customer_id,
        informix_online=True,
        informix_fillfactor=80,
        informix_dbspace="idxspace",
    )
    assert _sql(index, dialect) == (
        "CREATE INDEX ix_orders_customer_online ON orders (customer_id) "
        "FILLFACTOR 80 IN idxspace ONLINE"
    )


def test_compressed_dbspace_and_visibility_compile(dialect):
    table = _table()
    index = Index(
        "ix_orders_customer_compressed",
        table.c.customer_id,
        informix_dbspace="idxspace",
        informix_compressed=True,
        informix_visible=False,
    )
    assert _sql(index, dialect) == (
        "CREATE INDEX ix_orders_customer_compressed ON orders (customer_id) "
        "IN idxspace COMPRESSED INVISIBLE"
    )


def test_partial_index_compiles_as_native_fragment_expression(dialect):
    table = _table()
    index = Index(
        "ix_open_orders",
        table.c.customer_id,
        informix_where=table.c.status == "OPEN",
        informix_dbspace="idxspace",
    )
    assert _sql(index, dialect) == (
        "CREATE INDEX ix_open_orders ON orders (customer_id) "
        "FRAGMENT BY EXPRESSION "
        "PARTITION ix_open_orders__ifx_on (status = 'OPEN') IN idxspace, "
        "PARTITION ix_open_orders__ifx_off "
        "REMAINDER IN idxspace INDEX OFF"
    )


def test_partial_compressed_index_uses_native_tail_order(dialect):
    table = _table()
    index = Index(
        "ix_open_orders_advanced",
        table.c.id,
        informix_where=table.c.status == "OPEN",
        informix_dbspace="idxspace",
        informix_fillfactor=80,
        informix_compressed=True,
    )
    assert _sql(index, dialect) == (
        "CREATE INDEX ix_open_orders_advanced ON orders (id) "
        "FILLFACTOR 80 FRAGMENT BY EXPRESSION "
        "PARTITION ix_open_orders_advanced__ifx_on "
        "(status = 'OPEN') IN idxspace, "
        "PARTITION ix_open_orders_advanced__ifx_off "
        "REMAINDER IN idxspace INDEX OFF COMPRESSED"
    )


def test_online_and_compressed_are_rejected(dialect):
    table = _table()
    index = Index(
        "ix_online_compressed",
        table.c.id,
        informix_online=True,
        informix_compressed=True,
    )
    with pytest.raises(exc.CompileError, match="ONLINE and COMPRESSED"):
        _sql(index, dialect)


def test_using_and_per_key_opclasses_compile(dialect):
    table = _table()
    index = Index(
        "ix_rtree",
        table.c.payload,
        informix_using="rtree",
        informix_opclass="bbox_ops",
    )
    assert _sql(index, dialect) == (
        "CREATE INDEX ix_rtree ON orders (payload bbox_ops) USING rtree"
    )


def test_extensible_access_method_parameters_compile_safely(dialect):
    table = _table()
    index = Index(
        "ix_bts",
        table.c.payload,
        informix_using="bts",
        informix_amparam={
            "delete": "deferred",
            "max_docs": 2500,
            "weight": 0.75,
            "quote_test": "O'Reilly",
        },
    )
    assert _sql(index, dialect) == (
        "CREATE INDEX ix_bts ON orders (payload) USING bts "
        "(delete='deferred', max_docs=2500, weight=0.75, "
        "quote_test='O''Reilly')"
    )


def test_extensible_access_method_parameters_reject_arbitrary_sql(dialect):
    table = _table()
    raw = Index(
        "ix_raw_params",
        table.c.payload,
        informix_using="bts",
        informix_amparam="delete='deferred'); DROP TABLE orders; --",
    )
    with pytest.raises(exc.CompileError, match="non-empty mapping"):
        _sql(raw, dialect)

    missing_method = Index(
        "ix_missing_method",
        table.c.payload,
        informix_amparam={"delete": "deferred"},
    )
    with pytest.raises(exc.CompileError, match="requires informix_using"):
        _sql(missing_method, dialect)


@pytest.mark.parametrize(
    "value",
    [None, True, float("inf"), float("nan"), Decimal("NaN"), object()],
)
def test_extensible_access_method_parameter_values_are_validated(
    dialect, value
):
    table = _table()
    index = Index(
        "ix_bad_param",
        table.c.payload,
        informix_using="bts",
        informix_amparam={"value": value},
    )
    with pytest.raises(exc.CompileError, match="finite"):
        _sql(index, dialect)


def test_composite_functional_index_and_opclasses_compile(dialect):
    table = _table()
    index = Index(
        "ix_functional",
        func.normalize_customer(table.c.customer_id),
        table.c.status.desc(),
        informix_functional=True,
        informix_using="btree",
        informix_opclass=("integer_ops", "varchar_ops"),
    )
    assert _sql(index, dialect) == (
        "CREATE INDEX ix_functional ON orders "
        "(normalize_customer(customer_id) integer_ops, status varchar_ops DESC) "
        "USING btree"
    )


def test_forest_of_trees_compiles_hash_prefix_and_buckets(dialect):
    table = _table()
    index = Index(
        "ix_fot",
        table.c.customer_id,
        table.c.status,
        informix_hash_on=(table.c.customer_id,),
        informix_buckets=32,
    )
    assert _sql(index, dialect) == (
        "CREATE INDEX ix_fot ON orders (customer_id, status) "
        "HASH ON (customer_id) WITH 32 BUCKETS"
    )


@pytest.mark.parametrize("value", [0, 101, True, "90"])
def test_fillfactor_validation(dialect, value):
    table = _table()
    index = Index("ix_bad", table.c.id, informix_fillfactor=value)
    with pytest.raises(exc.CompileError, match="fillfactor"):
        _sql(index, dialect)


def test_partial_index_rejects_raw_text_and_missing_dbspace(dialect):
    table = _table()
    raw = Index("ix_raw", table.c.id, informix_where=text("status = 'OPEN'"))
    with pytest.raises(exc.CompileError, match="structured"):
        _sql(raw, dialect)

    missing = Index("ix_missing", table.c.id, informix_where=table.c.id > 0)
    with pytest.raises(exc.CompileError, match="requires informix_dbspace"):
        _sql(missing, dialect)


def test_hash_on_validation_and_incompatible_options(dialect):
    table = _table()
    wrong_prefix = Index(
        "ix_wrong_prefix",
        table.c.customer_id,
        table.c.status,
        informix_hash_on=(table.c.status,),
        informix_buckets=4,
    )
    with pytest.raises(exc.CompileError, match="prefix"):
        _sql(wrong_prefix, dialect)

    fillfactor = Index(
        "ix_hash_fill",
        table.c.customer_id,
        informix_hash_on=("customer_id",),
        informix_buckets=4,
        informix_fillfactor=80,
    )
    with pytest.raises(exc.CompileError, match="FILLFACTOR"):
        _sql(fillfactor, dialect)


def test_online_is_emitted_and_rejects_unsupported_index_kinds(dialect):
    table = _table()
    ordinary = Index(
        "ix_online",
        table.c.id,
        informix_online=True,
    )
    assert _sql(ordinary, dialect).endswith(" ONLINE")

    functional = Index(
        "ix_online_functional",
        func.normalize_customer(table.c.customer_id),
        informix_functional=True,
        informix_online=True,
    )
    with pytest.raises(exc.CompileError, match="functional indexes"):
        _sql(functional, dialect)

    rtree = Index(
        "ix_online_rtree",
        table.c.payload,
        informix_using="rtree",
        informix_online=True,
    )
    with pytest.raises(exc.CompileError, match="R-tree"):
        _sql(rtree, dialect)


def test_identifier_injection_is_rejected(dialect):
    table = _table()
    for kwargs in (
        {"informix_using": "btree; drop table orders"},
        {"informix_opclass": "ops) malicious("},
    ):
        index = Index("ix_safe", table.c.id, **kwargs)
        with pytest.raises(exc.CompileError, match="safe identifier"):
            _sql(index, dialect)


def test_create_modes_and_set_index_ddl_compile(dialect):
    table = _table()
    index = Index("ix_mode", table.c.id, informix_mode="DISABLED")
    assert _sql(index, dialect).endswith(" DISABLED")
    assert str(EnableIndex(index).compile(dialect=dialect)) == "SET INDEXES ix_mode ENABLED"
    assert str(DisableIndex(index).compile(dialect=dialect)) == "SET INDEXES ix_mode DISABLED"
    assert str(SetIndexMode(index, "filtering with error").compile(dialect=dialect)) == (
        "SET INDEXES ix_mode FILTERING WITH ERROR"
    )
    assert str(SetIndexVisibility(index, False).compile(dialect=dialect)) == (
        "SET INDEXES ix_mode INVISIBLE"
    )
    assert str(AlterIndexCluster(index).compile(dialect=dialect)) == (
        "ALTER INDEX ix_mode TO CLUSTER"
    )


def test_invalid_mode_and_uncluster_are_rejected(dialect):
    table = _table()
    index = Index("ix_mode", table.c.id)
    with pytest.raises(exc.ArgumentError, match="index mode"):
        SetIndexMode(index, "DROP TABLE")
    with pytest.raises(exc.CompileError, match="requires rebuilding"):
        AlterIndexCluster(index, clustered=False).compile(dialect=dialect)


def test_quoted_schema_names_compile(dialect):
    metadata = MetaData()
    table = Table(
        quoted_name("Order Data", True),
        metadata,
        Column(quoted_name("Customer Id", True), Integer),
        schema=quoted_name("Sales", True),
    )
    index = Index(
        quoted_name("Ix Customer", True),
        table.c["Customer Id"],
        informix_online=True,
    )
    assert _sql(index, dialect) == (
        'CREATE INDEX "Sales__Ix Customer" ON "Sales"."Order Data" '
        '("Customer Id") ONLINE'
    )


def test_sysindices_query_reads_advanced_metadata(dialect):
    reflector = IfxReflector(dialect)

    class Result:
        def fetchall(self):
            return []

    class Connection:
        statement = None
        def exec_driver_sql(self, statement, params=()):
            self.statement = statement
            return Result()

    connection = Connection()
    reflector._index_rows(connection, 1)
    lowered = connection.statement.lower()
    for token in (
        "i.amparam",
        "i.nhashcols",
        "i.nbuckets",
        "i.indexattr",
        "o.state",
        "sysobjstate",
    ):
        assert token in lowered


def test_reflection_reports_fot_compression_visibility_and_method(dialect, monkeypatch):
    reflector = IfxReflector(dialect)
    connection = object()
    monkeypatch.setattr(reflector, "_require_table_row", lambda *a, **k: (42, "orders", "informix", "T"))
    monkeypatch.setattr(reflector, "_constraint_duplicates_by_index", lambda *a: {})
    monkeypatch.setattr(reflector, "_get_column_name_map", lambda *a: {1: "customer_id", 2: "status"})
    monkeypatch.setattr(
        reflector,
        "_index_rows",
        lambda *a: [(
            "ix_fot", "informix", "D", "1 [1], 2 [1]", 7, "rtree",
            "en_US.819", 42, "custom=1", 1, 32, 0x12, "D",
        )],
    )
    monkeypatch.setattr(reflector, "_index_procedure_map", lambda *a: {})
    monkeypatch.setattr(
        reflector,
        "_index_opclass_map",
        lambda *a: {1: {"name": "rtree_ops", "owner": "informix", "amid": 7}},
    )
    monkeypatch.setattr(reflector, "_reflect_fragmentation", lambda *a, **k: (None, None))

    [reflected] = reflector.get_indexes(connection, "orders")
    options = reflected["dialect_options"]
    assert options["informix_using"] == "rtree"
    assert options["informix_opclass"] == "rtree_ops"
    assert options["informix_hash_on"] == ("customer_id",)
    assert options["informix_buckets"] == 32
    assert options["informix_compressed"] is True
    assert options["informix_visible"] is False
    assert options["informix_amparam"] == "custom=1"
    assert options["informix_mode"] == "DISABLED"

    table = _table()
    round_trip = Index(
        reflected["name"],
        table.c.customer_id,
        informix_using=options["informix_using"],
        informix_opclass=options["informix_opclass"],
        informix_amparam=options["informix_amparam"],
    )
    assert _sql(round_trip, dialect) == (
        "CREATE INDEX \"ix_fot\" ON orders (customer_id rtree_ops) "
        "USING rtree (custom=1)"
    )


def test_partial_index_reflection_round_trips_trusted_catalog_predicate(dialect, monkeypatch):
    reflector = IfxReflector(dialect)
    connection = object()
    monkeypatch.setattr(reflector, "_require_table_row", lambda *a, **k: (42, "orders", "informix", "T"))
    monkeypatch.setattr(reflector, "_constraint_duplicates_by_index", lambda *a: {})
    monkeypatch.setattr(reflector, "_get_column_name_map", lambda *a: {1: "customer_id"})
    monkeypatch.setattr(
        reflector,
        "_index_rows",
        lambda *a: [("ix_partial", "informix", "D", "1 [1]", 1, "btree", "en_US.819", 42, None, 0, 0, 0)],
    )
    monkeypatch.setattr(reflector, "_index_procedure_map", lambda *a: {})
    monkeypatch.setattr(reflector, "_index_opclass_map", lambda *a: {1: {"name": "btree_ops", "owner": "informix", "amid": 1}})
    monkeypatch.setattr(
        reflector,
        "_fragment_rows",
        lambda *a, **k: [
            (
                "I",
                "ix_partial",
                "E",
                0,
                "status = 'OPEN'",
                "idxspace",
                "ix_partial__ifx_on",
                0,
            ),
            (
                "I",
                "ix_partial",
                "E",
                1,
                "REMAINDER",
                "idxspace",
                "ix_partial__ifx_off",
                0,
            ),
        ],
    )
    monkeypatch.setattr(reflector, "_fragment_udr_dependencies", lambda *a, **k: {})

    [reflected] = reflector.get_indexes(connection, "orders")
    options = reflected["dialect_options"]
    assert options["informix_dbspace"] == "idxspace"
    assert options["informix_where"].sql == "status = 'OPEN'"

    table = _table()
    index = Index(
        reflected["name"],
        table.c.customer_id,
        informix_where=options["informix_where"],
        informix_dbspace=options["informix_dbspace"],
    )
    assert _sql(index, dialect).endswith(
        "FRAGMENT BY EXPRESSION "
        "PARTITION ix_partial__ifx_on (status = 'OPEN') IN idxspace, "
        "PARTITION ix_partial__ifx_off REMAINDER IN idxspace INDEX OFF"
    )


def test_partial_index_reflection_accepts_legacy_index_off_catalog_shape(
    dialect,
    monkeypatch,
):
    reflector = IfxReflector(dialect)
    connection = object()
    monkeypatch.setattr(
        reflector,
        "_fragment_rows",
        lambda *a, **k: [
            ("I", "ix_partial", "E", 0, "status = 'OPEN'", "idxspace", None),
            ("I", "ix_partial", "E", 1, "REMAINDER", "INDEX OFF", None),
        ],
    )
    monkeypatch.setattr(
        reflector,
        "_fragment_udr_dependencies",
        lambda *a, **k: {},
    )

    predicate, dbspace = reflector._reflect_partial_index(
        connection,
        42,
        index_name="ix_partial",
    )

    assert predicate.sql == "status = 'OPEN'"
    assert dbspace == "idxspace"


def _ordinary_dbspace(engine) -> str:
    with engine.connect() as connection:
        row = connection.exec_driver_sql(
            """
            SELECT FIRST 1 name
            FROM sysmaster:sysdbspaces
            WHERE is_blobspace = 0
              AND is_sbspace = 0
              AND is_temp = 0
            ORDER BY dbsnum
            """
        ).first()
    if row is None or row[0] is None:
        pytest.skip("an ordinary dbspace is required for index tests")
    return str(row[0]).strip()


def _index_by_name(indexes, name):
    return next(
        value
        for value in indexes
        if str(value["name"]).casefold() == str(name).casefold()
    )


@pytest.mark.requires_informix
def test_advanced_indexes_native_execute_reflect_and_alembic_round_trip(
    engine,
    name_factory,
):
    """Exercise persistent advanced-index metadata on Informix itself."""
    table_name = name_factory("sa_idx_adv_")
    partial_name = name_factory("ix_partial_")
    compressed_name = name_factory("ix_compressed_")
    online_name = name_factory("ix_online_")
    fot_name = name_factory("ix_fot_")
    mode_name = name_factory("ix_mode_")
    dbspace = _ordinary_dbspace(engine)

    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, nullable=False),
        Column("tenant_id", Integer, nullable=False),
        Column("status", String(12), nullable=False),
        # Keep every advanced-index feature on a distinct key signature.
        # Informix rejects a second user index on the same column set with
        # SQLCODE -350, even when the first index is partial or fragmented.
        Column("partial_key", Integer, nullable=False),
        Column("compressed_key", Integer, nullable=False),
        Column("online_key", Integer, nullable=False),
    )
    partial = Index(
        partial_name,
        table.c.partial_key,
        informix_where=table.c.status == "OPEN",
        informix_dbspace=dbspace,
        informix_fillfactor=80,
    )
    compressed = Index(
        compressed_name,
        table.c.compressed_key,
        informix_dbspace=dbspace,
        informix_compressed=True,
    )
    online = Index(
        online_name,
        table.c.online_key,
        informix_online=True,
    )
    fot = Index(
        fot_name,
        table.c.tenant_id,
        table.c.id,
        informix_hash_on=(table.c.tenant_id,),
        informix_buckets=8,
    )
    mode = Index(mode_name, table.c.status)

    try:
        with engine.begin() as connection:
            connection.execute(CreateTable(table))
            connection.execute(
                insert(table),
                [
                    {
                        "id": value,
                        "tenant_id": value % 17,
                        "status": "OPEN" if value <= 2100 else "CLOSED",
                        "partial_key": value,
                        "compressed_key": value,
                        "online_key": value,
                    }
                    for value in range(1, 2201)
                ],
            )
            partial.create(connection)
            compressed.create(connection)
            online.create(connection)
            fot.create(connection)
            mode.create(connection)

        with engine.connect() as connection:
            reflected = inspect(connection).get_indexes(table_name)
            partial_info = _index_by_name(reflected, partial_name)
            partial_options = partial_info["dialect_options"]
            assert partial_options["informix_where"].sql
            assert partial_options["informix_dbspace"].casefold() == dbspace.casefold()
            assert partial_options["informix_mode"] == "ENABLED"

            compressed_info = _index_by_name(reflected, compressed_name)
            compressed_options = compressed_info["dialect_options"]
            assert compressed_options["informix_compressed"] is True
            assert compressed_options["informix_dbspace"].casefold() == dbspace.casefold()

            online_info = _index_by_name(reflected, online_name)
            assert online_info["dialect_options"]["informix_mode"] == "ENABLED"

            fot_info = _index_by_name(reflected, fot_name)
            fot_options = fot_info["dialect_options"]
            assert fot_options["informix_hash_on"] == ("tenant_id",)
            assert fot_options["informix_buckets"] == 8
            assert fot_options["informix_mode"] == "ENABLED"

            connection.execute(DisableIndex(mode))
            connection.commit()

        with engine.connect() as connection:
            mode_options = _index_by_name(
                inspect(connection).get_indexes(table_name), mode_name
            )["dialect_options"]
            assert mode_options["informix_mode"] == "DISABLED"
            connection.execute(EnableIndex(mode))
            connection.commit()

        server_version = tuple(engine.dialect.server_version_info or ())
        if server_version and server_version[0] >= 15:
            with engine.connect() as connection:
                connection.execute(SetIndexVisibility(mode, False))
                connection.commit()
            with engine.connect() as connection:
                invisible_options = _index_by_name(
                    inspect(connection).get_indexes(table_name), mode_name
                )["dialect_options"]
                assert invisible_options["informix_visible"] is False
                connection.execute(SetIndexVisibility(mode, True))
                connection.commit()

        alembic = pytest.importorskip("alembic")
        from alembic.autogenerate import compare_metadata
        from alembic.migration import MigrationContext

        _ = alembic
        with engine.connect() as connection:
            def include_name(name, type_, parent_names):
                _ = parent_names
                if type_ == "table":
                    return (
                        name is not None
                        and str(name).casefold() == table_name.casefold()
                    )
                return True

            context = MigrationContext.configure(
                connection,
                opts={"include_name": include_name},
            )
            differences = compare_metadata(context, metadata)

        index_differences = [
            difference
            for difference in differences
            if isinstance(difference, tuple)
            and difference
            and difference[0] in {"add_index", "remove_index"}
        ]
        assert index_differences == []
    finally:
        with engine.connect() as connection:
            with suppress(Exception):
                table.drop(connection, checkfirst=True)
                connection.commit()
