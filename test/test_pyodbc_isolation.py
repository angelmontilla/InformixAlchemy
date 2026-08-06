from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy.exc import ArgumentError

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


class _FakeDBAPI:
    paramstyle = "qmark"
    SQL_ATTR_TXN_ISOLATION = 9108
    SQL_DEFAULT_TXN_ISOLATION = 9026
    SQL_TXN_READ_UNCOMMITTED = 101
    SQL_TXN_READ_COMMITTED = 102
    SQL_TXN_REPEATABLE_READ = 104
    SQL_TXN_SERIALIZABLE = 108
    SQL_LONGVARCHAR = -1


class _FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.executed = []
        self.closed = False

    def execute(self, statement):
        if self.connection.execute_error is not None:
            raise self.connection.execute_error

        self.executed.append(statement)

        # The SELECT used by pool pre-ping starts a driver transaction when
        # autocommit is disabled. SET ISOLATION is a complete-connection
        # setting and must not be modelled as application DML.
        if statement.lstrip().upper().startswith("SELECT"):
            if not self.connection.autocommit:
                self.connection.active_transaction = True

        return self

    def fetchone(self):
        return ("systables",)

    def close(self):
        self.closed = True


class _FakeConnection:
    def __init__(
        self,
        default_isolation=None,
        getinfo_error=None,
        execute_error=None,
        rollback_error=None,
    ):
        self.autocommit = False
        self.default_isolation = default_isolation
        self.getinfo_error = getinfo_error
        self.execute_error = execute_error
        self.rollback_error = rollback_error
        self.set_attr_calls = []
        self.getinfo_calls = []
        self.rollback_calls = 0
        self.active_transaction = False
        self.cursors = []
        self.closed = False

    def set_attr(self, attr, value):
        # Kept only to prove that the dialect no longer relies on the generic
        # ODBC attribute, which cannot distinguish Informix CURSOR STABILITY.
        self.set_attr_calls.append((attr, value))

    def getinfo(self, info_code):
        self.getinfo_calls.append(info_code)
        if self.getinfo_error is not None:
            raise self.getinfo_error
        return self.default_isolation

    def cursor(self):
        cursor = _FakeCursor(self)
        self.cursors.append(cursor)
        return cursor

    def rollback(self):
        self.rollback_calls += 1
        if self.rollback_error is not None:
            raise self.rollback_error
        self.active_transaction = False

    def close(self):
        self.closed = True


def _dialect():
    return IfxDialect_pyodbc(dbapi=_FakeDBAPI)


@pytest.mark.parametrize(
    ("level", "expected_sql"),
    [
        ("DIRTY READ", "SET ISOLATION TO DIRTY READ"),
        ("UNCOMMITTED READ", "SET ISOLATION TO DIRTY READ"),
        ("UR", "SET ISOLATION TO DIRTY READ"),
        ("READ UNCOMMITTED", "SET ISOLATION TO DIRTY READ"),
        ("COMMITTED READ", "SET ISOLATION TO COMMITTED READ"),
        ("READ COMMITTED", "SET ISOLATION TO COMMITTED READ"),
        ("CURSOR STABILITY", "SET ISOLATION TO CURSOR STABILITY"),
        ("CS", "SET ISOLATION TO CURSOR STABILITY"),
        ("READ STABILITY", "SET ISOLATION TO REPEATABLE READ"),
        ("RS", "SET ISOLATION TO REPEATABLE READ"),
        ("REPEATABLE READ", "SET ISOLATION TO REPEATABLE READ"),
        ("RR", "SET ISOLATION TO REPEATABLE READ"),
        ("SERIALIZABLE", "SET ISOLATION TO REPEATABLE READ"),
    ],
)
def test_set_isolation_level_uses_native_informix_sql(level, expected_sql):
    dialect = _dialect()
    connection = _FakeConnection()

    dialect.set_isolation_level(connection, level)

    assert connection.autocommit is False
    assert connection.set_attr_calls == []
    assert len(connection.cursors) == 1
    assert connection.cursors[0].executed == [expected_sql]
    assert connection.cursors[0].closed is True
    assert dialect.get_isolation_level(connection) == level


def test_cursor_stability_is_not_collapsed_to_committed_read():
    dialect = _dialect()
    committed = _FakeConnection()
    cursor = _FakeConnection()

    dialect.set_isolation_level(committed, "COMMITTED READ")
    dialect.set_isolation_level(cursor, "CURSOR STABILITY")

    assert committed.cursors[0].executed == [
        "SET ISOLATION TO COMMITTED READ"
    ]
    assert cursor.cursors[0].executed == [
        "SET ISOLATION TO CURSOR STABILITY"
    ]


def test_read_stability_is_a_conservative_compatibility_alias():
    dialect = _dialect()
    connection = _FakeConnection()

    dialect.set_isolation_level(connection, "READ STABILITY")

    # Informix has no separate READ STABILITY mode. The legacy spelling is
    # retained but mapped to the stricter native REPEATABLE READ level.
    assert connection.cursors[0].executed == [
        "SET ISOLATION TO REPEATABLE READ"
    ]
    assert dialect.get_isolation_level(connection) == "READ STABILITY"


def test_isolation_level_normalization_accepts_sqlalchemy_spellings():
    dialect = _dialect()
    connection = _FakeConnection()

    dialect._assert_and_set_isolation_level(
        connection,
        "cursor_stability",
    )

    assert connection.cursors[0].executed == [
        "SET ISOLATION TO CURSOR STABILITY"
    ]
    assert dialect.get_isolation_level(connection) == "CURSOR STABILITY"


def test_direct_set_normalizes_case_hyphens_and_whitespace():
    dialect = _dialect()
    connection = _FakeConnection()

    dialect.set_isolation_level(
        connection,
        "  read-stability  ",
    )

    assert connection.cursors[0].executed == [
        "SET ISOLATION TO REPEATABLE READ"
    ]
    assert dialect.get_isolation_level(connection) == "READ STABILITY"


def test_get_isolation_level_values_advertises_standard_native_and_aliases():
    dialect = _dialect()

    assert dialect.get_isolation_level_values(None) == [
        "AUTOCOMMIT",
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE",
        "DIRTY READ",
        "UNCOMMITTED READ",
        "UR",
        "COMMITTED READ",
        "CURSOR STABILITY",
        "CS",
        "READ STABILITY",
        "RS",
        "RR",
    ]


@pytest.mark.parametrize(
    ("odbc_value", "expected_level"),
    [
        (_FakeDBAPI.SQL_TXN_READ_UNCOMMITTED, "READ UNCOMMITTED"),
        (_FakeDBAPI.SQL_TXN_READ_COMMITTED, "READ COMMITTED"),
        (_FakeDBAPI.SQL_TXN_REPEATABLE_READ, "READ STABILITY"),
        (_FakeDBAPI.SQL_TXN_SERIALIZABLE, "SERIALIZABLE"),
    ],
)
def test_default_isolation_level_comes_from_odbc_driver(
    odbc_value,
    expected_level,
):
    dialect = _dialect()
    connection = _FakeConnection(default_isolation=odbc_value)

    assert dialect.get_default_isolation_level(connection) == expected_level
    assert connection.getinfo_calls == [_FakeDBAPI.SQL_DEFAULT_TXN_ISOLATION]
    assert dialect.get_isolation_level(connection) == expected_level


def test_default_isolation_level_falls_back_without_leaking_driver_error():
    dialect = _dialect()
    connection = _FakeConnection(
        getinfo_error=RuntimeError("driver cannot report default")
    )

    assert dialect.get_default_isolation_level(connection) == "READ COMMITTED"
    assert dialect.get_isolation_level(connection) == "READ COMMITTED"


def test_reset_isolation_level_restores_dialect_default():
    dialect = _dialect()
    connection = _FakeConnection(
        default_isolation=_FakeDBAPI.SQL_TXN_READ_COMMITTED
    )
    dialect.default_isolation_level = dialect.get_default_isolation_level(
        connection
    )

    dialect.set_isolation_level(connection, "RR")
    dialect.reset_isolation_level(connection)

    assert [cursor.executed for cursor in connection.cursors] == [
        ["SET ISOLATION TO REPEATABLE READ"],
        ["SET ISOLATION TO COMMITTED READ"],
    ]
    assert dialect.get_isolation_level(connection) == "READ COMMITTED"


def test_autocommit_does_not_erase_actual_isolation_level():
    dialect = _dialect()
    connection = _FakeConnection()

    dialect.set_isolation_level(connection, "RS")
    dialect.set_isolation_level(connection, "AUTOCOMMIT")

    assert connection.autocommit is True
    assert dialect.get_isolation_level(connection) == "RS"

    dialect.set_isolation_level(connection, "READ COMMITTED")

    assert connection.autocommit is False
    assert connection.cursors[-1].executed == [
        "SET ISOLATION TO COMMITTED READ"
    ]
    assert dialect.get_isolation_level(connection) == "READ COMMITTED"


def test_reset_isolation_level_leaves_autocommit_and_restores_default():
    dialect = _dialect()
    connection = _FakeConnection(
        default_isolation=_FakeDBAPI.SQL_TXN_READ_COMMITTED
    )
    dialect.default_isolation_level = dialect.get_default_isolation_level(
        connection
    )

    dialect.set_isolation_level(connection, "AUTOCOMMIT")
    dialect.reset_isolation_level(connection)

    assert connection.autocommit is False
    assert connection.cursors[-1].executed == [
        "SET ISOLATION TO COMMITTED READ"
    ]
    assert dialect.get_isolation_level(connection) == "READ COMMITTED"


def test_invalid_direct_level_raises_sqlalchemy_argument_error():
    dialect = _dialect()
    connection = _FakeConnection()

    with pytest.raises(ArgumentError, match="Invalid value 'CHAOS'"):
        dialect.set_isolation_level(connection, "chaos")

    assert connection.cursors == []
    assert connection.set_attr_calls == []


def test_engine_level_isolation_becomes_sqlalchemy_reset_baseline():
    dialect = IfxDialect_pyodbc(
        dbapi=_FakeDBAPI,
        isolation_level="cursor_stability",
    )
    connection = _FakeConnection(
        default_isolation=_FakeDBAPI.SQL_TXN_READ_COMMITTED
    )

    # Reproduce SQLAlchemy's create_engine() lifecycle: its built-in connect
    # listener applies the engine-wide setting before dialect.initialize()
    # asks for the default isolation level.
    dialect._builtin_onconnect()(connection, None)
    dialect.default_isolation_level = dialect.get_default_isolation_level(
        connection
    )

    assert dialect._on_connect_isolation_level == "CURSOR STABILITY"
    assert dialect.default_isolation_level == "CURSOR STABILITY"
    assert dialect.get_isolation_level(connection) == "CURSOR STABILITY"

    # A per-Connection override must be reset to the Engine baseline, not to
    # the physical ODBC driver default.
    dialect.set_isolation_level(connection, "READ COMMITTED")
    dialect.reset_isolation_level(connection)

    assert connection.cursors[-1].executed == [
        "SET ISOLATION TO CURSOR STABILITY"
    ]
    assert dialect.get_isolation_level(connection) == "CURSOR STABILITY"


def test_engine_autocommit_keeps_underlying_driver_default_baseline():
    dialect = IfxDialect_pyodbc(
        dbapi=_FakeDBAPI,
        isolation_level="AUTOCOMMIT",
    )
    connection = _FakeConnection(
        default_isolation=_FakeDBAPI.SQL_TXN_READ_COMMITTED
    )

    dialect._builtin_onconnect()(connection, None)
    dialect.default_isolation_level = dialect.get_default_isolation_level(
        connection
    )

    assert connection.autocommit is True
    assert dialect.default_isolation_level == "READ COMMITTED"
    assert dialect.get_isolation_level(connection) == "READ COMMITTED"

    dialect.set_isolation_level(connection, "CURSOR STABILITY")
    dialect.reset_isolation_level(connection)

    # SQLAlchemy's engine-wide AUTOCOMMIT mode is restored while the
    # underlying transactional isolation remains available for reporting.
    assert connection.autocommit is True
    assert dialect.get_isolation_level(connection) == "CURSOR STABILITY"


def test_close_forgets_tracked_state_before_closing_dbapi_connection():
    dialect = _dialect()
    connection = _FakeConnection(
        default_isolation=_FakeDBAPI.SQL_TXN_READ_COMMITTED
    )

    dialect.set_isolation_level(connection, "RR")
    assert dialect.get_isolation_level(connection) == "RR"

    dialect.do_close(connection)

    assert connection.closed is True
    assert dialect._remembered_isolation_level(connection) is None


def test_failed_native_statement_does_not_publish_unapplied_level():
    dialect = _dialect()
    connection = _FakeConnection(
        default_isolation=_FakeDBAPI.SQL_TXN_READ_COMMITTED,
        execute_error=RuntimeError("Informix rejected SET ISOLATION"),
    )
    assert dialect.get_default_isolation_level(connection) == "READ COMMITTED"

    with pytest.raises(RuntimeError, match="rejected SET ISOLATION"):
        dialect.set_isolation_level(connection, "RR")

    assert dialect.get_isolation_level(connection) == "READ COMMITTED"
    assert connection.cursors[0].closed is True


def test_failed_native_statement_restores_autocommit_mode():
    dialect = _dialect()
    connection = _FakeConnection(
        execute_error=RuntimeError("Informix rejected SET ISOLATION")
    )
    connection.autocommit = True

    with pytest.raises(RuntimeError, match="rejected SET ISOLATION"):
        dialect.set_isolation_level(connection, "CS")

    assert connection.autocommit is True


def test_fallback_constants_work_with_minimal_dbapi_double():
    dialect = IfxDialect_pyodbc(
        dbapi=SimpleNamespace(SQL_LONGVARCHAR=-1, paramstyle="qmark")
    )
    connection = _FakeConnection(
        default_isolation=IfxDialect_pyodbc._odbc_sql_txn_read_committed
    )

    assert dialect.get_default_isolation_level(connection) == "READ COMMITTED"
    dialect.set_isolation_level(connection, "CS")

    assert connection.cursors[-1].executed == [
        "SET ISOLATION TO CURSOR STABILITY"
    ]


def test_sqlalchemy_official_isolation_requirement_is_open():
    from IfxAlchemy.requirements import Requirements

    assert Requirements().isolation_level.enabled is True


def test_do_ping_rolls_back_manual_commit_select_before_isolation_change():
    dialect = _dialect()
    connection = _FakeConnection()

    assert dialect.do_ping(connection) is True

    assert connection.rollback_calls == 1
    assert connection.active_transaction is False
    assert len(connection.cursors) == 1
    assert connection.cursors[0].executed == [
        "SELECT FIRST 1 tabname FROM systables ORDER BY tabname"
    ]
    assert connection.cursors[0].closed is True

    # Regression for the observed pool_pre_ping path. Even though the native
    # SET ISOLATION implementation avoids SQLSetConnectAttr HY011, pre-ping
    # must not leak a hidden transaction into normal application work.
    dialect.set_isolation_level(connection, "CURSOR STABILITY")
    assert connection.cursors[-1].executed == [
        "SET ISOLATION TO CURSOR STABILITY"
    ]


def test_do_ping_rolls_back_after_failed_select_and_reraises_error():
    dialect = _dialect()
    ping_error = RuntimeError("Informix ping SELECT failed")
    connection = _FakeConnection(execute_error=ping_error)

    with pytest.raises(RuntimeError) as raised:
        dialect.do_ping(connection)

    assert raised.value is ping_error
    assert connection.rollback_calls == 1
    assert len(connection.cursors) == 1
    assert connection.cursors[0].closed is True


def test_do_ping_preserves_select_error_when_rollback_also_fails():
    dialect = _dialect()
    ping_error = RuntimeError("Informix ping SELECT failed")
    rollback_error = RuntimeError("Informix rollback failed")
    connection = _FakeConnection(
        execute_error=ping_error,
        rollback_error=rollback_error,
    )

    with pytest.raises(RuntimeError) as raised:
        dialect.do_ping(connection)

    assert raised.value is ping_error
    assert connection.rollback_calls == 1
    assert connection.cursors[0].closed is True


def test_do_ping_does_not_rollback_an_autocommit_connection():
    dialect = _dialect()
    connection = _FakeConnection()
    connection.autocommit = True

    assert dialect.do_ping(connection) is True

    assert connection.rollback_calls == 0
    assert connection.active_transaction is False


def test_get_default_isolation_does_not_overwrite_engine_level_setting():
    dialect = _dialect()
    connection = _FakeConnection(
        default_isolation=_FakeDBAPI.SQL_TXN_READ_COMMITTED
    )

    # This is the ordering used by create_engine(isolation_level=...): the
    # requested level can be applied before first-connect initialization asks
    # the driver for its default level.
    dialect.set_isolation_level(connection, "CURSOR STABILITY")

    assert dialect.get_default_isolation_level(connection) == "READ COMMITTED"
    assert dialect.get_isolation_level(connection) == "CURSOR STABILITY"
