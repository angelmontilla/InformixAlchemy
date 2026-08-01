from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    insert,
    literal,
    select,
    update,
)
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import ArgumentError, CompileError
from sqlalchemy.pool import QueuePool

from IfxAlchemy.base import IfxDialect, IfxExecutionContext
from IfxAlchemy.optimizer import (
    AllRows,
    AvoidIndex,
    FirstRows,
    INFORMIX_EXPLAIN,
    INFORMIX_OPTIMIZATION,
    INFORMIX_PDQPRIORITY,
    INFORMIX_STATEMENT_CACHE,
    JoinOrder,
    UseIndex,
    insert_optimizer_comment,
    normalize_optimizer_directives,
    optimizer_directives_cache_key,
    session_option_sql,
)


def _table():
    metadata = MetaData()
    return Table(
        "customer",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(30)),
    )


def _compile(statement):
    return str(statement.compile(dialect=IfxDialect()))


def test_select_directive_position_is_immediately_after_select():
    table = _table()
    statement = select(table).execution_options(
        informix_optimizer_directives=[FirstRows()]
    )

    assert _compile(statement).startswith(
        "SELECT {+FIRST_ROWS} customer.id, customer.name"
    )


def test_update_directive_position_is_immediately_after_update():
    table = _table()
    statement = update(table).values(name="updated").execution_options(
        informix_optimizer_directives=[UseIndex(table, "ix_customer")]
    )

    assert _compile(statement).startswith(
        "UPDATE {+INDEX(customer ix_customer)} customer SET"
    )


def test_delete_directive_position_is_immediately_after_delete():
    table = _table()
    statement = delete(table).execution_options(
        informix_optimizer_directives=[AvoidIndex(table, "ix_customer")]
    )

    assert _compile(statement).startswith(
        "DELETE {+AVOID_INDEX(customer ix_customer)} FROM customer"
    )


def test_cte_directive_is_on_root_select_not_cte_select():
    table = _table()
    rows = select(table.c.id).cte("customer_ids")
    statement = select(rows.c.id).execution_options(
        informix_optimizer_directives=[FirstRows()]
    )
    rendered = _compile(statement)

    assert rendered.count("{+FIRST_ROWS}") == 1
    assert "(SELECT customer.id AS id" in rendered
    assert "SELECT {+FIRST_ROWS} customer_ids.id" in rendered


def test_subquery_does_not_receive_outer_directive():
    table = _table()
    scalar = select(table.c.id).where(table.c.id == 1).scalar_subquery()
    statement = select(scalar).execution_options(
        informix_optimizer_directives=[AllRows()]
    )
    rendered = _compile(statement)

    assert rendered.count("{+ALL_ROWS}") == 1
    assert rendered.startswith("SELECT {+ALL_ROWS}")
    assert "(SELECT {+ALL_ROWS}" not in rendered


def test_multiple_directives_preserve_user_order():
    table = _table()
    statement = select(table).execution_options(
        informix_optimizer_directives=[
            FirstRows(),
            UseIndex(table, "ix_customer"),
            JoinOrder(),
        ]
    )

    assert "{+FIRST_ROWS, INDEX(customer ix_customer), ORDERED}" in _compile(
        statement
    )


def test_update_and_delete_ctes_receive_only_root_directives():
    table = _table()
    rows = select(table.c.id).where(table.c.id == 1).cte("customer_ids")
    statements = (
        update(table)
        .where(table.c.id.in_(select(rows.c.id)))
        .values(name="updated")
        .add_cte(rows),
        delete(table)
        .where(table.c.id.in_(select(rows.c.id)))
        .add_cte(rows),
    )

    rendered_update = _compile(
        statements[0].execution_options(
            informix_optimizer_directives=[FirstRows()]
        )
    )
    rendered_delete = _compile(
        statements[1].execution_options(
            informix_optimizer_directives=[AllRows()]
        )
    )

    assert rendered_update.count("{+FIRST_ROWS}") == 1
    assert "UPDATE {+FIRST_ROWS} customer SET" in rendered_update
    assert "(SELECT {+FIRST_ROWS}" not in rendered_update
    assert rendered_delete.count("{+ALL_ROWS}") == 1
    assert "DELETE {+ALL_ROWS} FROM customer" in rendered_delete
    assert "(SELECT {+ALL_ROWS}" not in rendered_delete


def test_explicit_alias_is_rendered_in_access_directive():
    alias = _table().alias("c")
    statement = select(alias).execution_options(
        informix_optimizer_directives=[UseIndex(alias, "ix_customer")]
    )

    assert "{+INDEX(c ix_customer)}" in _compile(statement)
    assert "FROM customer AS c" in _compile(statement)


def test_index_object_is_supported_and_must_belong_to_table():
    table = _table()
    index = Index("ix_customer", table.c.name)
    statement = select(table).execution_options(
        informix_optimizer_directives=[UseIndex(table, index)]
    )

    assert "{+INDEX(customer ix_customer)}" in _compile(statement)

    other = Table(
        "other_customer",
        MetaData(),
        Column("id", Integer),
        Column("name", String(30)),
    )
    with pytest.raises(ArgumentError, match="does not belong"):
        UseIndex(other, index)


def test_anonymous_alias_is_rejected_for_stable_directive_sql():
    alias = _table().alias()

    with pytest.raises(ArgumentError, match="explicit stable name"):
        UseIndex(alias, "ix_customer")


@pytest.mark.parametrize(
    "unsafe",
    [
        "ix_customer) FULL(customer",
        "ix_customer*/ DELETE FROM customer --",
        "ix-customer",
        "ix customer",
        "",
    ],
)
def test_index_name_injection_is_rejected(unsafe):
    with pytest.raises(ArgumentError, match="Informix index name"):
        UseIndex(_table(), unsafe)


def test_arbitrary_directive_text_is_rejected():
    with pytest.raises(ArgumentError, match="do not accept arbitrary text"):
        normalize_optimizer_directives(["FIRST_ROWS */ DELETE FROM customer"])


def test_conflicting_optimization_goals_are_rejected():
    with pytest.raises(ArgumentError, match="mutually exclusive"):
        normalize_optimizer_directives([FirstRows(), AllRows()])


def test_duplicate_directives_are_rejected():
    with pytest.raises(ArgumentError, match="Duplicate"):
        normalize_optimizer_directives([JoinOrder(), JoinOrder()])


def test_directives_are_immutable_hashable_and_have_stable_cache_keys():
    table = _table()
    directives = (
        FirstRows(),
        AllRows(),
        JoinOrder(),
        UseIndex(table, "ix_customer"),
        AvoidIndex(table, "ix_customer"),
    )

    assert all(isinstance(hash(directive), int) for directive in directives)
    assert optimizer_directives_cache_key([directives[3]]) == (
        ("INDEX", ("Table", "", "customer", ""), "ix_customer"),
    )
    with pytest.raises(Exception):
        directives[3].index = "changed"


def test_structural_statement_cache_key_remains_sqlalchemy_native():
    table = _table()
    first = select(table).execution_options(
        informix_optimizer_directives=[FirstRows()]
    )
    all_rows = select(table).execution_options(
        informix_optimizer_directives=[AllRows()]
    )

    # SQLAlchemy intentionally excludes execution options from its structural
    # statement key. Runtime injection must therefore supply the directives.
    assert first._generate_cache_key().key == all_rows._generate_cache_key().key


def test_cached_compilation_is_generic_and_runtime_injection_is_specific():
    table = _table()
    statement = select(table).execution_options(
        informix_optimizer_directives=[FirstRows()]
    )
    cache_key = statement._generate_cache_key()
    compiled = statement.compile(dialect=IfxDialect(), cache_key=cache_key)

    assert "FIRST_ROWS" not in str(compiled)

    context = SimpleNamespace(
        execution_options=statement.get_execution_options(),
        compiled=compiled,
        isselect=True,
        isupdate=False,
        isdelete=False,
        statement=str(compiled),
    )
    IfxExecutionContext._ifx_apply_runtime_optimizer_directives(context)

    assert context.statement.startswith("SELECT {+FIRST_ROWS}")


def test_engine_compiled_cache_reuses_structure_but_not_directive_sql():
    raw = _FakeConnection()
    engine = _fake_engine(raw)
    table = _table()
    first = select(table.c.id).execution_options(
        informix_optimizer_directives=[FirstRows()]
    )
    all_rows = select(table.c.id).execution_options(
        informix_optimizer_directives=[AllRows()]
    )

    with engine.connect() as connection:
        connection.execute(first).all()
        connection.execute(all_rows).all()

    sent_selects = [
        sql for sql in raw.statements if sql.lstrip().upper().startswith("SELECT")
    ]
    assert sent_selects[0].startswith("SELECT {+FIRST_ROWS}")
    assert sent_selects[1].startswith("SELECT {+ALL_ROWS}")
    assert len(engine._compiled_cache) == 1


def test_disabled_compiled_cache_does_not_duplicate_directive_comment():
    raw = _FakeConnection()
    engine = _fake_engine(raw)
    table = _table()
    statement = select(table.c.id).execution_options(
        informix_optimizer_directives=[FirstRows()]
    )

    with engine.connect().execution_options(compiled_cache=None) as connection:
        connection.execute(statement).all()

    sent = [
        sql for sql in raw.statements if sql.lstrip().upper().startswith("SELECT")
    ]
    assert len(sent) == 1
    assert sent[0].count("{+FIRST_ROWS}") == 1


def test_insert_comment_finds_root_statement_after_cte_and_quoted_text():
    sql = (
        "WITH q AS (SELECT 'UPDATE fake' AS value FROM t) "
        "UPDATE target SET value = 'SELECT fake'"
    )

    assert insert_optimizer_comment(sql, "UPDATE", "{+FIRST_ROWS}") == (
        "WITH q AS (SELECT 'UPDATE fake' AS value FROM t) "
        "UPDATE {+FIRST_ROWS} target SET value = 'SELECT fake'"
    )


def test_insert_comment_rejects_missing_root_keyword():
    with pytest.raises(CompileError, match="Could not locate"):
        insert_optimizer_comment("INSERT INTO t VALUES (1)", "SELECT", "{+ALL_ROWS}")


@pytest.mark.parametrize(
    ("name", "value", "expected"),
    [
        (INFORMIX_OPTIMIZATION, "first rows", "SET OPTIMIZATION FIRST_ROWS"),
        (INFORMIX_OPTIMIZATION, "ALL_ROWS", "SET OPTIMIZATION ALL_ROWS"),
        (INFORMIX_PDQPRIORITY, -1, "SET PDQPRIORITY -1"),
        (INFORMIX_PDQPRIORITY, 100, "SET PDQPRIORITY 100"),
        (INFORMIX_STATEMENT_CACHE, True, "SET STATEMENT CACHE ON"),
        (INFORMIX_STATEMENT_CACHE, False, "SET STATEMENT CACHE OFF"),
        (INFORMIX_EXPLAIN, True, "SET EXPLAIN ON"),
        (INFORMIX_EXPLAIN, False, "SET EXPLAIN OFF"),
        (INFORMIX_EXPLAIN, "AVOID_EXECUTE", "SET EXPLAIN ON AVOID_EXECUTE"),
    ],
)
def test_session_option_sql(name, value, expected):
    assert session_option_sql(name, value) == expected


@pytest.mark.parametrize("value", [-2, 101, 1.5, "20", True])
def test_pdqpriority_validation(value):
    with pytest.raises(ArgumentError, match="informix_pdqpriority"):
        session_option_sql(INFORMIX_PDQPRIORITY, value)


@pytest.mark.parametrize("value", ["FIRST; DELETE", "", 1, None])
def test_optimization_validation_prevents_injection(value):
    with pytest.raises(ArgumentError, match="informix_optimization"):
        session_option_sql(INFORMIX_OPTIMIZATION, value)


class _FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.closed = False
        self.description = None
        self.rowcount = -1

    def execute(self, statement, parameters=None):
        self.connection.statements.append(statement)
        if statement.lstrip().upper().startswith("SELECT"):
            self.description = [
                ("id", None, None, None, None, None, None)
            ]
        return self

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def close(self):
        self.closed = True


class _FakeConnection:
    def __init__(self):
        self.statements = []
        self.closed = False

    def cursor(self):
        return _FakeCursor(self)

    def rollback(self):
        self.statements.append("ROLLBACK")

    def commit(self):
        self.statements.append("COMMIT")

    def close(self):
        self.closed = True


def _fake_engine(raw_connection):
    dialect = IfxDialect()
    pool = QueuePool(lambda: raw_connection)
    return Engine(pool, dialect, URL.create("informix"))


def test_option_engine_applies_independent_controls_and_restores_pool():
    raw = _FakeConnection()
    engine = _fake_engine(raw)
    configured = engine.execution_options(
        informix_optimization="FIRST_ROWS",
        informix_pdqpriority=20,
        informix_statement_cache=True,
        informix_explain=True,
    )

    with configured.connect() as connection:
        assert connection.get_execution_options()[
            "informix_optimization"
        ] == "FIRST_ROWS"
        applied = set(raw.statements)
        assert "SET OPTIMIZATION FIRST_ROWS" in applied
        assert "SET PDQPRIORITY 20" in applied
        assert "SET STATEMENT CACHE ON" in applied
        assert "SET EXPLAIN ON" in applied

    restored = set(raw.statements)
    assert "SET OPTIMIZATION ALL_ROWS" in restored
    assert "SET PDQPRIORITY -1" in restored
    assert "SET STATEMENT CACHE OFF" in restored
    assert "SET EXPLAIN OFF" in restored


def test_connection_execution_options_restore_on_checkin():
    raw = _FakeConnection()
    engine = _fake_engine(raw)

    with engine.connect() as connection:
        returned = connection.execution_options(informix_pdqpriority=33)
        assert returned is connection
        assert "SET PDQPRIORITY 33" in raw.statements

    assert "SET PDQPRIORITY -1" in raw.statements


def test_session_controls_cannot_change_after_transaction_started():
    raw = _FakeConnection()
    engine = _fake_engine(raw)

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            with pytest.raises(Exception, match="may not be altered"):
                connection.execution_options(informix_pdqpriority=20)
        finally:
            transaction.rollback()


@pytest.mark.requires_informix
def test_native_directives_session_controls_and_set_explain(
    engine,
    name_factory,
):
    metadata = MetaData()
    table_name = name_factory("opt_directive_")
    index_name = name_factory("ix_opt_")
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("value", String(30)),
    )
    Index(index_name, table.c.value)
    metadata.create_all(engine)

    configured = engine.execution_options(
        informix_optimization="FIRST_ROWS",
        informix_pdqpriority=1,
        informix_statement_cache=True,
        informix_explain=True,
    )
    try:
        with configured.begin() as connection:
            connection.execute(
                insert(table),
                [
                    {"id": 1, "value": "alpha"},
                    {"id": 2, "value": "beta"},
                ],
            )

            selected = connection.execute(
                select(table.c.id)
                .where(table.c.value == "alpha")
                .execution_options(
                    informix_optimizer_directives=[
                        FirstRows(),
                        UseIndex(table, index_name),
                    ]
                )
            ).scalar_one()
            assert selected == 1

            connection.execute(
                update(table)
                .where(table.c.id == 1)
                .values(value="updated")
                .execution_options(
                    informix_optimizer_directives=[
                        UseIndex(table, index_name)
                    ]
                )
            )
            connection.execute(
                delete(table)
                .where(table.c.id == 2)
                .execution_options(
                    informix_optimizer_directives=[
                        AvoidIndex(table, index_name)
                    ]
                )
            )

            cte = select(table.c.id).cte("opt_ids")
            assert connection.execute(
                select(cte.c.id).execution_options(
                    informix_optimizer_directives=[AllRows()]
                )
            ).scalar_one() == 1
    finally:
        metadata.drop_all(engine)
