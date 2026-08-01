from __future__ import annotations

import re

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    column,
    exc,
    func,
    literal,
    quoted_name,
    select,
    table,
    text,
    values,
)

from IfxAlchemy import InformixMerge, merge
from IfxAlchemy.base import IfxDialect


def _compile(statement, *, literal_binds=False):
    return statement.compile(
        dialect=IfxDialect(),
        compile_kwargs={"literal_binds": literal_binds},
    )


def _normalized_sql(statement, *, literal_binds=False):
    return re.sub(
        r"\s+",
        " ",
        str(_compile(statement, literal_binds=literal_binds)),
    ).strip()


def _compile_tables():
    target = table(
        "merge_target",
        column("id", Integer),
        column("payload", String(40)),
        column("amount", Integer),
    ).alias("t")
    source = table(
        "merge_source",
        column("id", Integer),
        column("payload", String(40)),
        column("amount", Integer),
    ).alias("s")
    return target, source


def test_merge_factory_returns_informix_merge():
    target, source = _compile_tables()

    statement = merge(target, source, target.c.id == source.c.id)

    assert isinstance(statement, InformixMerge)
    assert statement.target is target
    assert statement.source is source


def test_merge_matched_update_compilation_and_parameters():
    target, source = _compile_tables()
    statement = merge(
        target,
        source,
        target.c.id == source.c.id,
    ).when_matched_update(
        values={
            target.c.payload: source.c.payload,
            target.c.amount: 17,
        }
    )

    compiled = _compile(statement)

    assert _normalized_sql(statement) == (
        "MERGE INTO merge_target AS t "
        "USING merge_source AS s "
        "ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET "
        "t.payload = s.payload, t.amount = ?"
    )
    assert compiled.params == {"param_1": 17}


def test_merge_not_matched_insert_compilation_and_bound_value():
    target, source = _compile_tables()
    unsafe_value = "Robert'); DROP TABLE customers;--"
    statement = merge(
        target,
        source,
        target.c.id == source.c.id,
    ).when_not_matched_insert(
        values={
            target.c.id: source.c.id,
            target.c.payload: unsafe_value,
            target.c.amount: source.c.amount,
        }
    )

    compiled = _compile(statement)
    sql_text = _normalized_sql(statement)

    assert sql_text == (
        "MERGE INTO merge_target AS t "
        "USING merge_source AS s "
        "ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT "
        "(id, payload, amount) VALUES (s.id, ?, s.amount)"
    )
    assert unsafe_value not in sql_text
    assert compiled.params == {"param_1": unsafe_value}


def test_merge_update_and_insert_compilation():
    target, source = _compile_tables()
    statement = (
        merge(target, source, target.c.id == source.c.id)
        .when_matched_update(
            values={
                target.c.payload: source.c.payload,
                target.c.amount: source.c.amount,
            }
        )
        .when_not_matched_insert(
            values={
                target.c.id: source.c.id,
                target.c.payload: source.c.payload,
                target.c.amount: source.c.amount,
            }
        )
    )

    assert _normalized_sql(statement) == (
        "MERGE INTO merge_target AS t "
        "USING merge_source AS s "
        "ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET "
        "t.payload = s.payload, t.amount = s.amount "
        "WHEN NOT MATCHED THEN INSERT "
        "(id, payload, amount) "
        "VALUES (s.id, s.payload, s.amount)"
    )


def test_merge_delete_and_insert_compilation():
    target, source = _compile_tables()
    statement = (
        merge(target, source, target.c.id == source.c.id)
        .when_matched_delete()
        .when_not_matched_insert(
            values={
                target.c.id: source.c.id,
                target.c.payload: source.c.payload,
                target.c.amount: source.c.amount,
            }
        )
    )

    assert _normalized_sql(statement) == (
        "MERGE INTO merge_target AS t "
        "USING merge_source AS s "
        "ON t.id = s.id "
        "WHEN MATCHED THEN DELETE "
        "WHEN NOT MATCHED THEN INSERT "
        "(id, payload, amount) "
        "VALUES (s.id, s.payload, s.amount)"
    )


def test_merge_rejects_join_or_derived_target():
    left = table("left_table", column("id", Integer))
    right = table("right_table", column("id", Integer))
    joined = left.join(right, left.c.id == right.c.id)

    with pytest.raises(exc.ArgumentError, match="not valid targets"):
        merge(joined, right, left.c.id == right.c.id)

    derived_target = select(left.c.id).subquery("derived_target")
    with pytest.raises(exc.ArgumentError, match="subject table"):
        merge(derived_target, right, derived_target.c.id == right.c.id)


def test_merge_rejects_direct_join_and_cte_sources():
    target = table("target", column("id", Integer))
    left = table("left_table", column("id", Integer))
    right = table("right_table", column("id", Integer))
    joined = left.join(right, left.c.id == right.c.id)

    with pytest.raises(exc.ArgumentError, match="wrapped in a SELECT"):
        merge(target, joined, target.c.id == left.c.id)

    source_cte = select(left.c.id).cte("source_cte")
    with pytest.raises(exc.ArgumentError, match="does not yet support a CTE"):
        merge(target, source_cte, target.c.id == source_cte.c.id)


def test_merge_rejects_textual_source_sql():
    target = table("target", column("id", Integer))

    with pytest.raises(exc.ArgumentError, match="textual SQL is not accepted"):
        merge(target, text("source_table"), target.c.id == 1)


def test_merge_rejects_equal_source_and_target_aliases():
    target = table("target", column("id", Integer)).alias("same_alias")
    source = table("source", column("id", Integer)).alias("SAME_ALIAS")

    with pytest.raises(exc.ArgumentError, match="aliases must be different"):
        merge(target, source, target.c.id == source.c.id)


def test_merge_uses_sqlalchemy_quoting_for_tables_columns_and_aliases():
    target = table(
        quoted_name("Order", True),
        column(quoted_name("Key", True), Integer),
        column(quoted_name("Value", True), String(20)),
    ).alias(quoted_name("Target Alias", True))
    source = table(
        quoted_name("Incoming Order", True),
        column(quoted_name("Key", True), Integer),
        column(quoted_name("Value", True), String(20)),
    ).alias(quoted_name("Source Alias", True))

    statement = merge(
        target,
        source,
        target.c.Key == source.c.Key,
    ).when_matched_update(
        values={target.c.Value: source.c.Value}
    )

    assert _normalized_sql(statement) == (
        'MERGE INTO "Order" AS "Target Alias" '
        'USING "Incoming Order" AS "Source Alias" '
        'ON "Target Alias"."Key" = "Source Alias"."Key" '
        'WHEN MATCHED THEN UPDATE SET '
        '"Target Alias"."Value" = "Source Alias"."Value"'
    )


def test_merge_plain_select_source_is_aliased_and_references_are_adapted():
    target, source_table = _compile_tables()
    source_table = source_table.element
    source_select = select(
        source_table.c.id,
        source_table.c.payload,
        source_table.c.amount,
    ).where(source_table.c.amount > 10)

    statement = merge(
        target,
        source_select,
        target.c.id == source_select.selected_columns.id,
    ).when_matched_update(
        values={
            target.c.payload: source_select.selected_columns.payload,
            target.c.amount: source_select.selected_columns.amount,
        }
    )

    compiled = _compile(statement)
    sql_text = _normalized_sql(statement)

    assert "USING (SELECT" in sql_text
    assert ") AS merge_source ON t.id = merge_source.id" in sql_text
    assert (
        "UPDATE SET t.payload = merge_source.payload, "
        "t.amount = merge_source.amount"
    ) in sql_text
    assert compiled.params == {"amount_1": 10}


def test_merge_true_collection_derived_table_source_compilation():
    target, _ = _compile_tables()
    collection_rows = table(
        "collection_rows",
        column("id", Integer),
        column("payload", String(40)),
        column("amount", Integer),
    )
    collection_source = (
        func.table(
            func.multiset(
                select(
                    collection_rows.c.id,
                    collection_rows.c.payload,
                    collection_rows.c.amount,
                ).scalar_subquery()
            )
        )
        .table_valued(
            column("id", Integer),
            column("payload", String(40)),
            column("amount", Integer),
        )
        .render_derived("collection_source")
    )

    statement = merge(
        target,
        collection_source,
        target.c.id == collection_source.c.id,
    ).when_matched_update(
        values={
            target.c.payload: collection_source.c.payload,
            target.c.amount: collection_source.c.amount,
        }
    )

    sql_text = _normalized_sql(statement)

    assert "USING table(multiset((SELECT" in sql_text
    assert (
        ")) AS collection_source(id, payload, amount) "
        "ON t.id = collection_source.id"
    ) in sql_text
    assert (
        "UPDATE SET t.payload = collection_source.payload, "
        "t.amount = collection_source.amount"
    ) in sql_text


def test_merge_named_values_source_compiles_as_typed_union_all():
    target, _ = _compile_tables()
    source = values(
        column("id", Integer),
        column("payload", String(40)),
        column("amount", Integer),
        name="incoming",
    ).data(
        [
            (1, "one", 10),
            (2, "two", 20),
        ]
    )

    statement = merge(
        target,
        source,
        target.c.id == source.c.id,
    ).when_not_matched_insert(
        values={
            target.c.id: source.c.id,
            target.c.payload: source.c.payload,
            target.c.amount: source.c.amount,
        }
    )

    compiled = _compile(statement)
    sql_text = _normalized_sql(statement)

    assert sql_text.startswith("MERGE INTO merge_target AS t USING (")
    assert (
        "SELECT CAST(? AS INTEGER) AS id, "
        "CAST(? AS VARCHAR(40)) AS payload, "
        "CAST(? AS INTEGER) AS amount "
        "FROM sysmaster:informix.sysdual"
    ) in sql_text
    assert " UNION ALL SELECT " in sql_text
    assert ") AS incoming ON t.id = incoming.id" in sql_text
    assert compiled.params == {
        "param_1": 1,
        "param_2": "one",
        "param_3": 10,
        "param_4": 2,
        "param_5": "two",
        "param_6": 20,
    }


def test_merge_values_source_requires_a_name():
    target, _ = _compile_tables()
    source = values(column("id", Integer)).data([(1,)])
    statement = merge(
        target,
        source,
        target.c.id == source.c.id,
    ).when_matched_delete()

    with pytest.raises(exc.CompileError, match="named Values source"):
        _compile(statement)


def test_merge_requires_at_least_one_action():
    target, source = _compile_tables()
    statement = merge(target, source, target.c.id == source.c.id)

    with pytest.raises(exc.CompileError, match="requires UPDATE, DELETE"):
        _compile(statement)


def test_merge_rejects_update_and_delete_combination():
    target, source = _compile_tables()
    statement = merge(target, source, target.c.id == source.c.id)
    statement = statement.when_matched_update(
        values={target.c.payload: source.c.payload}
    )

    with pytest.raises(exc.ArgumentError, match="mutually exclusive"):
        statement.when_matched_delete()


def test_merge_rejects_duplicate_action_and_invalid_mappings():
    target, source = _compile_tables()
    base = merge(target, source, target.c.id == source.c.id)

    with pytest.raises(exc.ArgumentError, match="must be a mapping"):
        base.when_matched_update(values=[("payload", "x")])

    with pytest.raises(exc.ArgumentError, match="at least one target column"):
        base.when_not_matched_insert(values={})

    with pytest.raises(exc.ArgumentError, match="no column named"):
        base.when_matched_update(values={"missing": "x"})

    inserted = base.when_not_matched_insert(values={"id": source.c.id})
    with pytest.raises(exc.ArgumentError, match="only one WHEN NOT MATCHED"):
        inserted.when_not_matched_insert(values={"id": source.c.id})


def test_merge_builder_is_generative():
    target, source = _compile_tables()
    base = merge(target, source, target.c.id == source.c.id)
    updated = base.when_matched_update(
        values={target.c.payload: source.c.payload}
    )
    complete = updated.when_not_matched_insert(
        values={target.c.id: source.c.id}
    )

    assert base._matched_update is None
    assert base._not_matched_insert is None
    assert updated._matched_update is not None
    assert updated._not_matched_insert is None
    assert complete._matched_update is not None
    assert complete._not_matched_insert is not None


def test_merge_statement_cache_is_explicitly_disabled():
    target, source = _compile_tables()
    statement = merge(
        target,
        source,
        target.c.id == source.c.id,
    ).when_matched_update(
        values={target.c.amount: literal(4)}
    )

    assert InformixMerge.inherit_cache is False
    assert statement._generate_cache_key() is None
    assert statement.get_execution_options()["preserve_rowcount"] is True


@pytest.fixture
def merge_tables(conn, name_factory):
    metadata = MetaData()
    target = Table(
        name_factory("sa_merge_target_"),
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(40), nullable=False),
        Column("amount", Integer, nullable=False),
    )
    source = Table(
        name_factory("sa_merge_source_"),
        metadata,
        Column("source_row", Integer, primary_key=True),
        Column("id", Integer, nullable=False),
        Column("payload", String(40), nullable=False),
        Column("amount", Integer, nullable=False),
    )

    metadata.create_all(conn)
    conn.execute(
        target.insert(),
        [
            {"id": 1, "payload": "old-one", "amount": 10},
            {"id": 2, "payload": "old-two", "amount": 20},
            {"id": 4, "payload": "old-four", "amount": 40},
        ],
    )
    conn.execute(
        source.insert(),
        [
            {"source_row": 1, "id": 1, "payload": "new-one", "amount": 100},
            {"source_row": 2, "id": 3, "payload": "new-three", "amount": 300},
        ],
    )
    conn.commit()

    try:
        yield target, source
    finally:
        conn.rollback()
        metadata.drop_all(conn, checkfirst=True)
        conn.commit()


def _target_rows(conn, target):
    return conn.execute(
        select(target.c.id, target.c.payload, target.c.amount).order_by(
            target.c.id
        )
    ).all()


@pytest.mark.requires_informix
def test_merge_matched_update_round_trip_and_rowcount(conn, merge_tables):
    target, source = merge_tables
    target_alias = target.alias("t")
    source_alias = source.alias("s")
    statement = merge(
        target_alias,
        source_alias,
        target_alias.c.id == source_alias.c.id,
    ).when_matched_update(
        values={
            target_alias.c.payload: source_alias.c.payload,
            target_alias.c.amount: source_alias.c.amount,
        }
    )

    result = conn.execute(statement)
    conn.commit()

    assert result.rowcount == 1
    assert _target_rows(conn, target) == [
        (1, "new-one", 100),
        (2, "old-two", 20),
        (4, "old-four", 40),
    ]


@pytest.mark.requires_informix
def test_merge_not_matched_insert_round_trip(conn, merge_tables):
    target, source = merge_tables
    target_alias = target.alias("t")
    source_alias = source.alias("s")
    statement = merge(
        target_alias,
        source_alias,
        target_alias.c.id == source_alias.c.id,
    ).when_not_matched_insert(
        values={
            target_alias.c.id: source_alias.c.id,
            target_alias.c.payload: source_alias.c.payload,
            target_alias.c.amount: source_alias.c.amount,
        }
    )

    result = conn.execute(statement)
    conn.commit()

    assert result.rowcount == 1
    assert _target_rows(conn, target) == [
        (1, "old-one", 10),
        (2, "old-two", 20),
        (3, "new-three", 300),
        (4, "old-four", 40),
    ]


@pytest.mark.requires_informix
def test_merge_update_and_insert_round_trip(conn, merge_tables):
    target, source = merge_tables
    target_alias = target.alias("t")
    source_alias = source.alias("s")
    statement = (
        merge(
            target_alias,
            source_alias,
            target_alias.c.id == source_alias.c.id,
        )
        .when_matched_update(
            values={
                target_alias.c.payload: source_alias.c.payload,
                target_alias.c.amount: source_alias.c.amount,
            }
        )
        .when_not_matched_insert(
            values={
                target_alias.c.id: source_alias.c.id,
                target_alias.c.payload: source_alias.c.payload,
                target_alias.c.amount: source_alias.c.amount,
            }
        )
    )

    result = conn.execute(statement)
    conn.commit()

    assert result.rowcount == 2
    assert _target_rows(conn, target) == [
        (1, "new-one", 100),
        (2, "old-two", 20),
        (3, "new-three", 300),
        (4, "old-four", 40),
    ]


@pytest.mark.requires_informix
def test_merge_matched_delete_round_trip(conn, merge_tables):
    target, source = merge_tables
    target_alias = target.alias("t")
    source_alias = source.alias("s")
    statement = merge(
        target_alias,
        source_alias,
        target_alias.c.id == source_alias.c.id,
    ).when_matched_delete()

    result = conn.execute(statement)
    conn.commit()

    assert result.rowcount == 1
    assert _target_rows(conn, target) == [
        (2, "old-two", 20),
        (4, "old-four", 40),
    ]


@pytest.mark.requires_informix
def test_merge_delete_and_insert_round_trip(conn, merge_tables):
    target, source = merge_tables
    target_alias = target.alias("t")
    source_alias = source.alias("s")
    statement = (
        merge(
            target_alias,
            source_alias,
            target_alias.c.id == source_alias.c.id,
        )
        .when_matched_delete()
        .when_not_matched_insert(
            values={
                target_alias.c.id: source_alias.c.id,
                target_alias.c.payload: source_alias.c.payload,
                target_alias.c.amount: source_alias.c.amount,
            }
        )
    )

    result = conn.execute(statement)
    conn.commit()

    assert result.rowcount == 2
    assert _target_rows(conn, target) == [
        (2, "old-two", 20),
        (3, "new-three", 300),
        (4, "old-four", 40),
    ]


@pytest.mark.requires_informix
def test_merge_select_source_round_trip(conn, merge_tables):
    target, source = merge_tables
    target_alias = target.alias("t")
    source_select = select(
        source.c.id,
        source.c.payload,
        source.c.amount,
    ).where(source.c.id == 1)
    statement = merge(
        target_alias,
        source_select,
        target_alias.c.id == source_select.selected_columns.id,
    ).when_matched_update(
        values={
            target_alias.c.payload: source_select.selected_columns.payload,
            target_alias.c.amount: source_select.selected_columns.amount,
        }
    )

    result = conn.execute(statement)
    conn.commit()

    assert result.rowcount == 1
    assert _target_rows(conn, target)[0] == (1, "new-one", 100)


@pytest.mark.requires_informix
def test_merge_values_collection_source_round_trip(conn, merge_tables):
    target, _ = merge_tables
    target_alias = target.alias("t")
    source = values(
        column("id", Integer),
        column("payload", String(40)),
        column("amount", Integer),
        name="incoming_values",
    ).data(
        [
            (2, "values-two", 222),
            (5, "values-five", 500),
        ]
    )
    statement = (
        merge(
            target_alias,
            source,
            target_alias.c.id == source.c.id,
        )
        .when_matched_update(
            values={
                target_alias.c.payload: source.c.payload,
                target_alias.c.amount: source.c.amount,
            }
        )
        .when_not_matched_insert(
            values={
                target_alias.c.id: source.c.id,
                target_alias.c.payload: source.c.payload,
                target_alias.c.amount: source.c.amount,
            }
        )
    )

    result = conn.execute(statement)
    conn.commit()

    assert result.rowcount == 2
    assert _target_rows(conn, target) == [
        (1, "old-one", 10),
        (2, "values-two", 222),
        (4, "old-four", 40),
        (5, "values-five", 500),
    ]


@pytest.mark.requires_informix
def test_merge_is_rolled_back_as_one_statement(conn, merge_tables):
    target, source = merge_tables
    target_alias = target.alias("t")
    source_alias = source.alias("s")
    before = _target_rows(conn, target)
    conn.commit()
    statement = (
        merge(
            target_alias,
            source_alias,
            target_alias.c.id == source_alias.c.id,
        )
        .when_matched_update(
            values={target_alias.c.payload: source_alias.c.payload}
        )
        .when_not_matched_insert(
            values={
                target_alias.c.id: source_alias.c.id,
                target_alias.c.payload: source_alias.c.payload,
                target_alias.c.amount: source_alias.c.amount,
            }
        )
    )

    transaction = conn.begin()
    conn.execute(statement)
    transaction.rollback()

    assert _target_rows(conn, target) == before


@pytest.mark.requires_informix
def test_merge_duplicate_source_match_fails_atomically(conn, merge_tables):
    target, source = merge_tables
    conn.execute(
        source.insert(),
        {
            "source_row": 3,
            "id": 1,
            "payload": "duplicate-one",
            "amount": 999,
        },
    )
    conn.commit()

    target_alias = target.alias("t")
    source_alias = source.alias("s")
    before = _target_rows(conn, target)
    statement = merge(
        target_alias,
        source_alias,
        target_alias.c.id == source_alias.c.id,
    ).when_matched_update(
        values={
            target_alias.c.payload: source_alias.c.payload,
            target_alias.c.amount: source_alias.c.amount,
        }
    )

    with pytest.raises(exc.DBAPIError):
        conn.execute(statement)
    conn.rollback()

    assert _target_rows(conn, target) == before


@pytest.mark.requires_informix
def test_merge_activates_update_and_insert_triggers(
    conn,
    merge_tables,
    name_factory,
    qident,
):
    target, source = merge_tables
    metadata = MetaData()
    audit = Table(
        name_factory("sa_merge_audit_"),
        metadata,
        Column("event_kind", String(1), nullable=False),
        Column("row_id", Integer, nullable=False),
        Column("old_payload", String(40)),
        Column("new_payload", String(40)),
    )
    audit.create(conn)
    conn.commit()

    update_trigger = name_factory("tr_merge_upd_")
    insert_trigger = name_factory("tr_merge_ins_")
    qt = qident(target.name)
    qa = qident(audit.name)
    qu = qident(update_trigger)
    qi = qident(insert_trigger)

    try:
        conn.exec_driver_sql(
            f"CREATE TRIGGER {qu} UPDATE ON {qt} "
            "REFERENCING OLD AS old_row NEW AS new_row "
            "FOR EACH ROW ("
            f"INSERT INTO {qa} "
            "(event_kind, row_id, old_payload, new_payload) "
            "VALUES ('U', new_row.id, old_row.payload, new_row.payload)"
            ")"
        )
        conn.exec_driver_sql(
            f"CREATE TRIGGER {qi} INSERT ON {qt} "
            "REFERENCING NEW AS new_row "
            "FOR EACH ROW ("
            f"INSERT INTO {qa} "
            "(event_kind, row_id, old_payload, new_payload) "
            "VALUES ('I', new_row.id, NULL, new_row.payload)"
            ")"
        )
        conn.commit()

        target_alias = target.alias("t")
        source_alias = source.alias("s")
        statement = (
            merge(
                target_alias,
                source_alias,
                target_alias.c.id == source_alias.c.id,
            )
            .when_matched_update(
                values={target_alias.c.payload: source_alias.c.payload}
            )
            .when_not_matched_insert(
                values={
                    target_alias.c.id: source_alias.c.id,
                    target_alias.c.payload: source_alias.c.payload,
                    target_alias.c.amount: source_alias.c.amount,
                }
            )
        )
        conn.execute(statement)
        conn.commit()

        rows = conn.execute(
            select(
                audit.c.event_kind,
                audit.c.row_id,
                audit.c.old_payload,
                audit.c.new_payload,
            ).order_by(audit.c.event_kind, audit.c.row_id)
        ).all()
        assert rows == [
            ("I", 3, None, "new-three"),
            ("U", 1, "old-one", "new-one"),
        ]
    finally:
        conn.rollback()
        for trigger_name in (insert_trigger, update_trigger):
            try:
                conn.exec_driver_sql(f"DROP TRIGGER {qident(trigger_name)}")
                conn.commit()
            except Exception:
                conn.rollback()
        audit.drop(conn, checkfirst=True)
        conn.commit()
