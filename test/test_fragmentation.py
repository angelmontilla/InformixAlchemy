from __future__ import annotations

from dataclasses import FrozenInstanceError
import re

import pytest
from sqlalchemy import (
    Column,
    Date,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    inspect,
    quoted_name,
)
from sqlalchemy.exc import ArgumentError, CompileError
from sqlalchemy.schema import CreateIndex, CreateTable

from IfxAlchemy import (
    AddFragment,
    AttachFragment,
    AttachedIndexFragmentation,
    DetachFragment,
    DropFragment,
    ExpressionFragmentation,
    Fragment,
    InitFragmentation,
    ListFragmentation,
    ModifyFragment,
    RangeFragmentation,
    RangeIntervalFragmentation,
    RoundRobinFragmentation,
)
from IfxAlchemy.base import IfxDialect
from IfxAlchemy.reflection import IfxReflector


def _sql(element) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(element.compile(dialect=IfxDialect())).strip(),
    )


def _table(name="orders"):
    return Table(
        name,
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("year", Integer),
        Column("region", String(20)),
    )


def _set_fragmentation(table, fragmentation):
    table.dialect_options["informix"]["fragment_by"] = fragmentation
    return table


def test_round_robin_table_compiles_for_multiple_dbspaces():
    table = _set_fragmentation(
        _table(),
        RoundRobinFragmentation(dbspaces=("dbs_a", "dbs_b")),
    )

    assert _sql(CreateTable(table)).endswith(
        ") FRAGMENT BY ROUND ROBIN IN dbs_a, dbs_b"
    )


def test_named_round_robin_partitions_compile_without_free_sql():
    table = _set_fragmentation(
        _table(),
        RoundRobinFragmentation(
            fragments=(
                Fragment(name="part_a", dbspace="dbs_a"),
                Fragment(name="part_b", dbspace="dbs_b"),
            ),
            partition_by=True,
        ),
    )

    assert _sql(CreateTable(table)).endswith(
        ") PARTITION BY ROUND ROBIN "
        "PARTITION part_a IN dbs_a, PARTITION part_b IN dbs_b"
    )


def test_expression_fragmentation_compiles_bound_literals():
    table = _table()
    _set_fragmentation(
        table,
        ExpressionFragmentation(
            fragments=(
                Fragment(
                    name="p_old",
                    expression=table.c.year < 2026,
                    dbspace="dbs_old",
                ),
                Fragment(name="p_rest", remainder=True, dbspace="dbs_rest"),
            )
        ),
    )

    sql = _sql(CreateTable(table))
    assert sql.endswith(
        ") FRAGMENT BY EXPRESSION "
        "PARTITION p_old (\"year\" < 2026) IN dbs_old, "
        "PARTITION p_rest REMAINDER IN dbs_rest"
    )
    assert "?" not in sql


def test_range_fragmentation_is_typed_expression_fragmentation():
    table = _table()
    _set_fragmentation(
        table,
        RangeFragmentation(
            fragments=(
                Fragment(
                    name="p_2025",
                    expression=table.c.year < 2026,
                    dbspace="dbs_2025",
                ),
                Fragment(name="p_future", remainder=True, dbspace="dbs_future"),
            )
        ),
    )

    sql = _sql(CreateTable(table))
    assert "FRAGMENT BY EXPRESSION" in sql
    assert "FRAGMENT BY RANGE" not in sql


def test_list_fragmentation_compiles_values_null_and_remainder():
    table = _table()
    _set_fragmentation(
        table,
        ListFragmentation(
            key=table.c.region,
            fragments=(
                Fragment(
                    name="p_es",
                    values=("ES", "PT"),
                    dbspace="dbs_iberia",
                ),
                Fragment(name="p_null", is_null=True, dbspace="dbs_null"),
                Fragment(name="p_other", remainder=True, dbspace="dbs_other"),
            ),
        ),
    )

    assert _sql(CreateTable(table)).endswith(
        ") FRAGMENT BY LIST (region) "
        "PARTITION p_es VALUES ('ES', 'PT') IN dbs_iberia, "
        "PARTITION p_null VALUES (NULL) IN dbs_null, "
        "PARTITION p_other REMAINDER IN dbs_other"
    )


def test_range_interval_compiles_store_in_and_named_fragments():
    table = _table()
    _set_fragmentation(
        table,
        RangeIntervalFragmentation(
            key=table.c.year,
            interval=100,
            store_in=("dbs_3", "dbs_4"),
            fragments=(
                Fragment(name="p0", upper_bound=0, dbspace="dbs_0"),
                Fragment(name="p1", upper_bound=1000, dbspace="dbs_1"),
                Fragment(name="p_null", is_null=True, dbspace="dbs_null"),
            ),
        ),
    )

    assert _sql(CreateTable(table)).endswith(
        ") FRAGMENT BY RANGE (\"year\") INTERVAL (100) "
        "STORE IN (dbs_3, dbs_4) "
        "PARTITION p0 VALUES < 0 IN dbs_0, "
        "PARTITION p1 VALUES < 1000 IN dbs_1, "
        "PARTITION p_null VALUES IS NULL IN dbs_null"
    )


def test_range_interval_requires_one_supported_native_key_column():
    table = _table()

    multiple_columns = RangeIntervalFragmentation(
        key=table.c.year + table.c.id,
        interval=100,
        fragments=(Fragment("p0", upper_bound=1000, dbspace="db0"),),
    )
    with pytest.raises(CompileError, match="exactly one column"):
        _sql(InitFragmentation(table, multiple_columns))

    string_key = RangeIntervalFragmentation(
        key=table.c.region,
        interval=100,
        fragments=(Fragment("p0", upper_bound="M", dbspace="db0"),),
    )
    with pytest.raises(CompileError, match="numeric, DATE, or DATETIME"):
        _sql(InitFragmentation(table, string_key))

    dated = Table(
        "dated_rows",
        MetaData(),
        Column("created_on", Date),
    )
    valid_date_key = RangeIntervalFragmentation(
        key=dated.c.created_on,
        interval=1,
        fragments=(Fragment("p0", upper_bound="2027-01-01", dbspace="db0"),),
    )
    assert "FRAGMENT BY RANGE (created_on)" in _sql(
        InitFragmentation(dated, valid_date_key)
    )


def test_create_table_option_order_is_stable():
    table = _table()
    table.dialect_options["informix"].update(
        {
            "first_extent": 64,
            "next_extent": 32,
            "dbspace": "data_dbs",
            "compressed": True,
            "lock_level": "ROW",
        }
    )

    assert _sql(CreateTable(table)).endswith(
        ") EXTENT SIZE 64 NEXT SIZE 32 IN data_dbs COMPRESSED LOCK MODE ROW"
    )


def test_dbspace_and_fragmentation_are_mutually_exclusive():
    table = _table()
    table.dialect_options["informix"].update(
        {
            "dbspace": "data_dbs",
            "fragment_by": RoundRobinFragmentation(
                dbspaces=("dbs_a", "dbs_b")
            ),
        }
    )

    with pytest.raises(CompileError, match="mutually exclusive"):
        _sql(CreateTable(table))


def test_compressed_requires_boolean():
    table = _table()
    table.dialect_options["informix"]["compressed"] = "yes"

    with pytest.raises(CompileError, match="informix_compressed must be a boolean"):
        _sql(CreateTable(table))


def test_detached_index_compiles_its_own_expression_fragmentation():
    table = _table()
    index = Index("ix_orders_year", table.c.year)
    index.dialect_options["informix"]["fragment_by"] = ExpressionFragmentation(
        fragments=(
            Fragment("i_old", table.c.year < 2026, "idx_dbs_1"),
            Fragment("i_rest", remainder=True, dbspace="idx_dbs_2"),
        )
    )

    assert _sql(CreateIndex(index)).endswith(
        "FRAGMENT BY EXPRESSION "
        "PARTITION i_old (\"year\" < 2026) IN idx_dbs_1, "
        "PARTITION i_rest REMAINDER IN idx_dbs_2"
    )


def test_round_robin_index_is_rejected_by_native_rules():
    table = _table()
    index = Index(
        "ix_orders_year",
        table.c.year,
        informix_fragment_by=RoundRobinFragmentation(
            dbspaces=("idx_a", "idx_b")
        ),
    )

    with pytest.raises(CompileError, match="ROUND ROBIN.*indexes"):
        _sql(CreateIndex(index))


def test_attached_index_reflection_marker_emits_no_detached_clause():
    table = _table()
    index = Index(
        "ix_orders_year",
        table.c.year,
        informix_fragment_by=AttachedIndexFragmentation(),
    )

    sql = _sql(CreateIndex(index))
    assert sql == 'CREATE INDEX ix_orders_year ON orders ("year")'


def test_index_dbspace_compiles_after_index_definition():
    table = _table()
    index = Index(
        "ix_orders_year",
        table.c.year,
        informix_dbspace="idx_dbs",
    )

    assert _sql(CreateIndex(index)).endswith('ON orders ("year") IN idx_dbs')


def test_quoted_fragment_and_dbspace_names_are_preserved():
    table = _table(quoted_name("Order Data", quote=True))
    _set_fragmentation(
        table,
        RoundRobinFragmentation(
            fragments=(
                Fragment(
                    name=quoted_name("Part A", quote=True),
                    dbspace=quoted_name("Data A", quote=True),
                ),
                Fragment(
                    name=quoted_name("Part B", quote=True),
                    dbspace=quoted_name("Data B", quote=True),
                ),
            )
        ),
    )

    sql = _sql(CreateTable(table))
    assert 'CREATE TABLE "Order Data"' in sql
    assert 'PARTITION "Part A" IN "Data A"' in sql


def test_raw_text_fragment_expression_is_rejected():
    from sqlalchemy import text

    with pytest.raises(ArgumentError, match=r"text\(\) is not accepted"):
        ExpressionFragmentation(
            fragments=(
                Fragment("p1", text("year < 2026"), "db1"),
                Fragment("p2", remainder=True, dbspace="db2"),
            )
        )


def test_fragment_expression_subquery_is_rejected():
    from sqlalchemy import select

    table = _table()
    strategy = ExpressionFragmentation(
        fragments=(
            Fragment(
                "p1",
                table.c.year < select(table.c.year).scalar_subquery(),
                "db1",
            ),
            Fragment("p2", remainder=True, dbspace="db2"),
        )
    )

    with pytest.raises(CompileError, match="cannot contain a subquery"):
        _sql(InitFragmentation(table, strategy))


def test_fragment_names_and_list_values_must_be_unique():
    table = _table()

    with pytest.raises(ArgumentError, match="fragment names must be unique"):
        ExpressionFragmentation(
            fragments=(
                Fragment("same", table.c.year < 2026, "db1"),
                Fragment("same", remainder=True, dbspace="db2"),
            )
        )

    with pytest.raises(ArgumentError, match="LIST fragment values must be unique"):
        ListFragmentation(
            key=table.c.region,
            fragments=(
                Fragment("p1", values=("ES", "PT"), dbspace="db1"),
                Fragment("p2", values=("ES",), dbspace="db2"),
            ),
        )

    with pytest.raises(ArgumentError, match="dbspaces must not contain duplicates"):
        RoundRobinFragmentation(dbspaces=("db1", "db1"))


def test_list_and_range_values_reject_column_expressions():
    table = _table()
    list_strategy = ListFragmentation(
        key=table.c.region,
        fragments=(
            Fragment("p1", values=(table.c.year,), dbspace="db1"),
            Fragment("p2", remainder=True, dbspace="db2"),
        ),
    )
    with pytest.raises(CompileError, match="must be a constant expression"):
        _sql(InitFragmentation(table, list_strategy))

    range_strategy = RangeIntervalFragmentation(
        key=table.c.year,
        interval=table.c.year,
        fragments=(Fragment("p1", upper_bound=100, dbspace="db1"),),
    )
    with pytest.raises(CompileError, match="must be a constant expression"):
        _sql(InitFragmentation(table, range_strategy))


def test_expression_fragmentation_supports_explicit_null_fragment():
    table = _table()
    strategy = ExpressionFragmentation(
        fragments=(
            Fragment("p_null", is_null=True, dbspace="db_null"),
            Fragment("p_rest", remainder=True, dbspace="db_rest"),
        )
    )
    assert _sql(InitFragmentation(table, strategy)).endswith(
        "FRAGMENT BY EXPRESSION "
        "PARTITION p_null VALUES (NULL) IN db_null, "
        "PARTITION p_rest REMAINDER IN db_rest"
    )


def test_fragmentation_models_are_immutable():
    fragment = Fragment(name="p1", dbspace="db1")

    with pytest.raises(FrozenInstanceError):
        fragment.dbspace = "db2"


def test_init_fragmentation_compiles_for_table_and_index():
    table = _table()
    strategy = ExpressionFragmentation(
        fragments=(
            Fragment("p1", table.c.year < 2026, "db1"),
            Fragment("p2", remainder=True, dbspace="db2"),
        )
    )
    assert _sql(InitFragmentation(table, strategy)).startswith(
        "ALTER FRAGMENT ON TABLE orders INIT FRAGMENT BY EXPRESSION"
    )

    index = Index("ix_orders_year", table.c.year)
    assert _sql(InitFragmentation(index, dbspace="idx_dbs")) == (
        "ALTER FRAGMENT ON INDEX ix_orders_year INIT IN idx_dbs"
    )


def test_add_drop_modify_fragment_compile():
    table = _table()
    add = AddFragment(
        table,
        Fragment("p_new", table.c.year == 2026, "db_new"),
        before="p_rest",
    )
    assert _sql(add) == (
        'ALTER FRAGMENT ON TABLE orders ADD '
        'PARTITION p_new ("year" = 2026) IN db_new BEFORE p_rest'
    )

    assert _sql(DropFragment(table, "p_old", partition=True)) == (
        "ALTER FRAGMENT ON TABLE orders DROP PARTITION p_old"
    )

    modify = ModifyFragment(
        table,
        "p_old",
        Fragment("p_archive", table.c.year < 2020, "db_archive"),
        old_partition=True,
    )
    assert _sql(modify) == (
        'ALTER FRAGMENT ON TABLE orders MODIFY PARTITION p_old TO '
        'PARTITION p_archive ("year" < 2020) IN db_archive'
    )


def test_modify_range_interval_fragment_compiles_typed_upper_bound():
    table = _table()
    statement = ModifyFragment(
        table,
        "p_transition",
        Fragment("p_transition", upper_bound=2000, dbspace="db_archive"),
        old_partition=True,
    )

    assert _sql(statement) == (
        "ALTER FRAGMENT ON TABLE orders MODIFY PARTITION p_transition TO "
        "PARTITION p_transition VALUES < 2000 IN db_archive"
    )


def test_interval_store_add_and_drop_compile():
    table = _table()
    assert _sql(AddFragment(table, interval_dbspaces=("db3", "db4"))) == (
        "ALTER FRAGMENT ON TABLE orders ADD INTERVAL STORE IN (db3, db4)"
    )
    assert _sql(DropFragment(table, interval_dbspaces=("db3", "db4"))) == (
        "ALTER FRAGMENT ON TABLE orders DROP INTERVAL STORE IN (db3, db4)"
    )


def test_attach_and_detach_compile_with_online_keyword():
    surviving = _table("surviving")
    consumed = _table("consumed")
    detached = _table("detached")

    attach = AttachFragment(
        surviving,
        consumed,
        fragment=Fragment(
            "p_2026",
            surviving.c.year == 2026,
        ),
        online=True,
    )
    assert _sql(attach) == (
        'ALTER FRAGMENT ONLINE ON TABLE surviving ATTACH consumed AS '
        'PARTITION p_2026 ("year" = 2026)'
    )

    detach = DetachFragment(
        surviving,
        "p_2026",
        detached,
        online=True,
    )
    assert _sql(detach) == (
        "ALTER FRAGMENT ONLINE ON TABLE surviving "
        "DETACH PARTITION p_2026 detached"
    )


def test_attach_rejects_dbspace_and_position_without_as_clause():
    surviving = _table("surviving")
    consumed = _table("consumed")

    with pytest.raises(ArgumentError, match="does not accept a fragment dbspace"):
        AttachFragment(
            surviving,
            consumed,
            fragment=Fragment("p", surviving.c.year == 1, "db1"),
        )

    with pytest.raises(ArgumentError, match="requires an AS fragment"):
        AttachFragment(surviving, consumed, before="p_old")


def test_online_rejects_explicit_non_interval_table_strategy():
    table = _table()
    table.dialect_options["informix"]["fragment_by"] = ExpressionFragmentation(
        fragments=(
            Fragment("p1", table.c.year < 2026, "db1"),
            Fragment("p2", remainder=True, dbspace="db2"),
        )
    )
    detached = _table("detached")

    with pytest.raises(CompileError, match="requires a range-interval table"):
        _sql(DetachFragment(table, "p1", detached, online=True))


def test_online_is_restricted_to_native_operations():
    table = _table()
    with pytest.raises(CompileError, match="INIT does not support ONLINE"):
        _sql(InitFragmentation(table, dbspace="db1", online=True))
    with pytest.raises(CompileError, match="ADD does not support ONLINE"):
        _sql(
            AddFragment(
                table,
                Fragment("p", table.c.year == 1, "db1"),
                online=True,
            )
        )


def _row(strategy, evalpos, exprtext, dbspace, partition, indexname=None):
    return ("T" if indexname is None else "I", indexname, strategy, evalpos, exprtext, dbspace, partition)


def test_reflection_reconstructs_expression_fragmentation_immutably():
    reflector = IfxReflector(IfxDialect())
    fragment_by, dbspace = reflector._fragmentation_from_rows(
        [
            _row("E", 0, '("year" < 2026)', "db1", "p_old"),
            _row("E", 1, "REMAINDER", "db2", "p_rest"),
        ],
        {0: ("normalize_year",)},
    )

    assert dbspace is None
    assert isinstance(fragment_by, ExpressionFragmentation)
    assert fragment_by.fragments[0].name == "p_old"
    assert fragment_by.fragments[0].expression.sql == '("year" < 2026)'
    assert fragment_by.fragments[0].expression.udr_dependencies == (
        "normalize_year",
    )
    assert fragment_by.fragments[1].remainder is True


def test_reflection_reconstructs_explicit_expression_null_fragment():
    reflector = IfxReflector(IfxDialect())
    fragment_by, _ = reflector._fragmentation_from_rows(
        [
            _row("E", 0, "VALUES (NULL)", "db_null", "p_null"),
            _row("E", 1, "REMAINDER", "db_rest", "p_rest"),
        ]
    )

    assert isinstance(fragment_by, ExpressionFragmentation)
    assert fragment_by.fragments[0].is_null is True
    assert fragment_by.fragments[0].expression is None
    assert fragment_by.fragments[1].remainder is True


def test_reflection_reconstructs_list_and_range_interval_metadata():
    reflector = IfxReflector(IfxDialect())
    list_by, _ = reflector._fragmentation_from_rows(
        [
            _row("L", -3, "region", None, None),
            _row("L", 0, "VALUES ('ES', 'PT')", "db1", "p_es"),
            _row("L", 1, "REMAINDER", "db2", "p_rest"),
        ]
    )
    assert isinstance(list_by, ListFragmentation)
    assert list_by.key.sql == "region"
    assert list_by.fragments[0]._catalog_selector.sql == "VALUES ('ES', 'PT')"

    list_with_null, _ = reflector._fragmentation_from_rows(
        [
            _row("L", -3, "region", None, None),
            _row("L", 0, "VALUES (NULL)", "db0", "p_null"),
            _row("L", 1, "REMAINDER", "db1", "p_rest"),
        ]
    )
    assert list_with_null.fragments[0].is_null is True
    assert list_with_null.fragments[1].remainder is True

    range_by, _ = reflector._fragmentation_from_rows(
        [
            _row("N", -3, '"year"', None, None),
            _row("N", -2, "100", None, None),
            _row("N", -1, "STORE IN (db3, db4)", None, None),
            _row("N", 0, "VALUES < 0", "db0", "p0"),
            _row("N", 1, "VALUES < 1000", "db1", "p1"),
        ]
    )
    assert isinstance(range_by, RangeIntervalFragmentation)
    assert range_by.key.sql == '"year"'
    assert range_by.interval.sql == "100"
    assert range_by.store_in == ("db3", "db4")
    assert range_by.fragments[1]._catalog_selector.sql == "VALUES < 1000"

    range_with_null, _ = reflector._fragmentation_from_rows(
        [
            _row("N", -3, '"year"', None, None),
            _row("N", -2, "100", None, None),
            _row("N", 0, "VALUES < 1000", "db0", "p0"),
            _row("N", 1, "VALUES IS NULL", "db1", "p_null"),
        ]
    )
    assert range_with_null.fragments[1].is_null is True


def test_reflection_distinguishes_dbspace_and_attached_index():
    reflector = IfxReflector(IfxDialect())
    fragment_by, dbspace = reflector._fragmentation_from_rows(
        [_row("I", 0, None, "data_dbs", None)]
    )
    assert fragment_by is None
    assert dbspace == "data_dbs"

    attached, dbspace = reflector._fragmentation_from_rows(
        [_row("T", 0, None, "idx_dbs", None, "ix_orders")]
    )
    assert isinstance(attached, AttachedIndexFragmentation)
    assert dbspace is None


def test_reflected_expression_fragmentation_recompiles_without_parsing_sql():
    reflector = IfxReflector(IfxDialect())
    fragment_by, _ = reflector._fragmentation_from_rows(
        [
            _row("E", 0, '("year" < 2026)', "db1", "p_old"),
            _row("E", 1, "REMAINDER", "db2", "p_rest"),
        ]
    )
    table = _set_fragmentation(_table(), fragment_by)

    sql = _sql(CreateTable(table))
    assert 'PARTITION "p_old" ("year" < 2026) IN db1' in sql
    assert "REMAINDER IN db2" in sql


def _ordinary_dbspace(engine) -> str:
    with engine.connect() as connection:
        rows = connection.exec_driver_sql(
            """
            SELECT FIRST 1 name
            FROM sysmaster:sysdbspaces
            WHERE is_blobspace = 0
              AND is_sbspace = 0
              AND is_temp = 0
            ORDER BY dbsnum
            """
        ).fetchall()
    dbspaces = tuple(str(row[0]).strip() for row in rows if row[0])
    if not dbspaces:
        pytest.skip("an ordinary dbspace is required for fragmentation")
    return dbspaces[0]


@pytest.mark.requires_informix
def test_table_and_index_fragmentation_create_alter_reflect_autoload_and_drop(
    engine,
    name_factory,
):
    """Exercise native table/index fragmentation in one ordinary dbspace."""
    table_name = name_factory("sa_frag_")
    attached_name = name_factory("ix_fa_")
    detached_name = name_factory("ix_fd_")
    middle_name = name_factory("p_mid_")
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, nullable=False),
        Column("bucket", Integer, nullable=False),
    )
    dbspace = _ordinary_dbspace(engine)

    table.dialect_options["informix"]["fragment_by"] = ExpressionFragmentation(
        fragments=(
            Fragment("p_low", table.c.bucket < 100, dbspace),
            Fragment("p_rest", remainder=True, dbspace=dbspace),
        )
    )

    try:
        with engine.begin() as connection:
            # Create the table first so ALTER FRAGMENT can be tested before the
            # two explicit indexes are associated with it.
            table.create(connection)

            options = inspect(connection).get_table_options(table_name)
            fragment_by = options["informix_fragment_by"]
            assert isinstance(fragment_by, ExpressionFragmentation)
            assert len(fragment_by.fragments) == 2
            assert fragment_by.fragments[0].dbspace == dbspace

            connection.execute(
                AddFragment(
                    table,
                    Fragment(
                        middle_name,
                        (table.c.bucket >= 100) & (table.c.bucket < 200),
                        dbspace,
                    ),
                    before="p_rest",
                )
            )
            expanded = inspect(connection).get_table_options(table_name)[
                "informix_fragment_by"
            ]
            assert [fragment.name for fragment in expanded.fragments] == [
                "p_low",
                middle_name,
                "p_rest",
            ]

            connection.execute(DropFragment(table, middle_name, partition=True))
            reduced = inspect(connection).get_table_options(table_name)[
                "informix_fragment_by"
            ]
            assert [fragment.name for fragment in reduced.fragments] == [
                "p_low",
                "p_rest",
            ]

            attached_index = Index(attached_name, table.c.bucket)
            detached_index = Index(detached_name, table.c.id)
            detached_index.dialect_options["informix"]["fragment_by"] = (
                ExpressionFragmentation(
                    fragments=(
                        Fragment("i_low", table.c.id < 1000, dbspace),
                        Fragment("i_rest", remainder=True, dbspace=dbspace),
                    )
                )
            )
            attached_index.create(connection)
            detached_index.create(connection)

            reflected_indexes = {
                item["name"]: item
                for item in inspect(connection).get_indexes(table_name)
            }
            attached_options = reflected_indexes[attached_name]["dialect_options"]
            detached_options = reflected_indexes[detached_name]["dialect_options"]
            assert isinstance(
                attached_options["informix_fragment_by"],
                AttachedIndexFragmentation,
            )
            assert isinstance(
                detached_options["informix_fragment_by"],
                ExpressionFragmentation,
            )

            autoloaded = Table(
                table_name,
                MetaData(),
                autoload_with=connection,
            )
            assert isinstance(
                autoloaded.dialect_options["informix"]["fragment_by"],
                ExpressionFragmentation,
            )
            reflected_by_name = {index.name: index for index in autoloaded.indexes}
            assert isinstance(
                reflected_by_name[attached_name].dialect_options["informix"][
                    "fragment_by"
                ],
                AttachedIndexFragmentation,
            )
            assert isinstance(
                reflected_by_name[detached_name].dialect_options["informix"][
                    "fragment_by"
                ],
                ExpressionFragmentation,
            )
    finally:
        with engine.begin() as connection:
            table.drop(connection, checkfirst=True)


@pytest.mark.requires_informix
def test_attach_detach_fragment_round_trip(engine, name_factory):
    """Verify native ATTACH/DETACH semantics without emulating data movement."""
    surviving_name = name_factory("sa_fsurv_")
    consumed_name = name_factory("sa_fcons_")
    detached_name = name_factory("sa_fdet_")
    partition_name = name_factory("p_att_")
    dbspace = _ordinary_dbspace(engine)

    surviving = Table(
        surviving_name,
        MetaData(),
        Column("id", Integer, nullable=False),
        Column("bucket", Integer, nullable=False),
    )
    consumed = Table(
        consumed_name,
        MetaData(),
        Column("id", Integer, nullable=False),
        Column("bucket", Integer, nullable=False),
    )
    detached = Table(
        detached_name,
        MetaData(),
        Column("id", Integer, nullable=False),
        Column("bucket", Integer, nullable=False),
    )
    surviving.dialect_options["informix"]["fragment_by"] = (
        ExpressionFragmentation(
            fragments=(
                Fragment("p_low", surviving.c.bucket < 100, dbspace),
                Fragment("p_rest", remainder=True, dbspace=dbspace),
            )
        )
    )
    consumed.dialect_options["informix"]["dbspace"] = dbspace

    try:
        with engine.begin() as connection:
            surviving.create(connection)
            consumed.create(connection)
            connection.execute(consumed.insert().values(id=1, bucket=150))

            connection.execute(
                AttachFragment(
                    surviving,
                    consumed,
                    fragment=Fragment(
                        partition_name,
                        (surviving.c.bucket >= 100)
                        & (surviving.c.bucket < 200),
                    ),
                    before="p_rest",
                )
            )
            assert not inspect(connection).has_table(consumed_name)
            attached = inspect(connection).get_table_options(surviving_name)[
                "informix_fragment_by"
            ]
            assert partition_name in {
                fragment.name for fragment in attached.fragments
            }

            connection.execute(
                DetachFragment(
                    surviving,
                    partition_name,
                    detached,
                    partition=True,
                )
            )
            assert inspect(connection).has_table(detached_name)
            detached_reflected = Table(
                detached_name,
                MetaData(),
                autoload_with=connection,
            )
            assert connection.execute(
                detached_reflected.select()
            ).all() == [(1, 150)]
    finally:
        with engine.begin() as connection:
            detached.drop(connection, checkfirst=True)
            consumed.drop(connection, checkfirst=True)
            surviving.drop(connection, checkfirst=True)
