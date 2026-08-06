from __future__ import annotations

import pytest

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


class Error(Exception):
    pass


class ProgrammingError(Error):
    pass


class OperationalError(Error):
    pass


class FakeDBAPI:
    Error = Error
    ProgrammingError = ProgrammingError
    OperationalError = OperationalError
    paramstyle = "qmark"


class Connection:
    def __init__(self, closed=False):
        self.closed = closed


class ConnectionWrapper:
    def __init__(self, connection):
        self.dbapi_connection = connection


class Cursor:
    def __init__(self, connection):
        self.connection = connection


@pytest.fixture
def dialect():
    return IfxDialect_pyodbc(dbapi=FakeDBAPI)


@pytest.mark.parametrize(
    "sqlstate",
    [
        "08000",
        "08001",
        "08003",
        "08004",
        "08006",
        "08007",
        "08S01",
        "08S02",
        "08ZZZ",
    ],
)
def test_sqlstate_class_08_is_disconnect(dialect, sqlstate):
    error = OperationalError(
        sqlstate,
        f"[{sqlstate}] [IBM][Informix ODBC Driver] connection diagnostic",
    )

    assert dialect.is_disconnect(error, None, None) is True


def test_odbc_disconnect_warning_sqlstate_is_disconnect(dialect):
    error = Error("01002", "Disconnect error")

    assert dialect.is_disconnect(error, None, None) is True


@pytest.mark.parametrize(
    "diagnostic",
    [
        "driver wrapper failed: SQLSTATE=08S01; native=-11020",
        "[08003] connection does not exist",
        b"08s01",
    ],
)
def test_sqlstate_is_detected_in_nonstandard_diagnostics(
    dialect,
    diagnostic,
):
    error = RuntimeError(diagnostic)

    assert dialect.is_disconnect(error, None, None) is True


@pytest.mark.parametrize(
    "attribute_name",
    ["sqlstate", "sql_state"],
)
def test_sqlstate_attribute_is_detected(dialect, attribute_name):
    class DriverError(Exception):
        pass

    error = DriverError("failed")
    setattr(error, attribute_name, "08003")

    assert dialect.is_disconnect(error, None, None) is True


def test_nested_pyodbc_diagnostic_containers_are_flattened(dialect):
    error = RuntimeError(
        {
            "records": [
                (
                    "S1000",
                    {
                        "driver": "[IBM][Informix ODBC Driver]",
                        "message": "Communication link failure",
                    },
                )
            ]
        }
    )

    assert dialect.is_disconnect(error, None, None) is True


@pytest.mark.parametrize(
    "link_attribute",
    ["orig", "__cause__", "__context__"],
)
def test_wrapped_dbapi_exception_is_inspected(dialect, link_attribute):
    inner = OperationalError("08S01", "Communication link failure")
    outer = RuntimeError("driver wrapper")
    setattr(outer, link_attribute, inner)

    assert dialect.is_disconnect(outer, None, None) is True


def test_exception_and_diagnostic_cycles_do_not_recurse_forever(dialect):
    payload = []
    payload.append(payload)
    error = RuntimeError(payload)
    error.orig = error

    assert dialect.is_disconnect(error, None, None) is False


@pytest.mark.parametrize(
    "message",
    [
        "Connection is closed",
        "Connection has been closed",
        "Connection is no longer active",
        "Connection lost",
        "Communication link failure",
        "Communication failure",
        "Connection failure",
        "Network connection is broken",
        "General network error",
        "System error occurred in network function",
        "Server is not available",
        "Database server is offline",
        "Cannot connect to database server",
        "Attempt to connect to database server failed",
        "Connection refused",
        "Broken pipe",
        "Connection reset by peer",
        "Connection aborted by host",
        "Socket is not connected",
        "TCP/IP connection was terminated",
        "Server closed the connection",
        "Remote host reset the connection",
        "Cannot rollback on a closed connection",
    ],
)
def test_explicit_disconnect_message_patterns(dialect, message):
    error = Error("S1000", message)

    assert dialect.is_disconnect(error, None, None) is True


@pytest.mark.parametrize(
    "native_code",
    [-908, -930, -11020, -25580, -25582],
)
def test_informix_native_disconnect_codes_in_text(dialect, native_code):
    error = OperationalError(
        "HY000",
        "[IBM][Informix ODBC Driver] native diagnostic "
        f"({native_code}) (SQLExecDirectW)",
    )

    assert dialect.is_disconnect(error, None, None) is True


@pytest.mark.parametrize(
    "attribute_name",
    ["native_error", "native_code", "sqlcode"],
)
def test_informix_native_disconnect_code_attributes(
    dialect,
    attribute_name,
):
    error = OperationalError("HY000", "driver diagnostic")
    setattr(error, attribute_name, -25582)

    assert dialect.is_disconnect(error, None, None) is True


def test_driver_error_attribute_is_inspected(dialect):
    error = OperationalError("HY000")
    error.driver_error = "Network connection was closed"

    assert dialect.is_disconnect(error, None, None) is True


@pytest.mark.parametrize(
    "message",
    [
        "The cursor's connection has been closed.",
        "Attempt to use a closed connection.",
    ],
)
def test_inherited_closed_connection_messages_remain_supported(
    dialect,
    message,
):
    error = ProgrammingError(message)

    assert dialect.is_disconnect(error, None, None) is True


@pytest.mark.parametrize(
    "connection,cursor",
    [
        (Connection(closed=True), None),
        (Connection(closed=1), None),
        (ConnectionWrapper(Connection(closed=True)), None),
        (None, Cursor(Connection(closed=True))),
    ],
)
def test_explicit_closed_connection_state_is_disconnect(
    dialect,
    connection,
    cursor,
):
    assert dialect.is_disconnect(
        RuntimeError("unclassified driver failure"),
        connection,
        cursor,
    ) is True


@pytest.mark.parametrize(
    "error",
    [
        OperationalError("40001", "Deadlock detected"),
        OperationalError("HYT00", "Statement timeout expired"),
        OperationalError("23000", "Constraint violation"),
        OperationalError("HY000", "Network buffer size is invalid"),
        OperationalError("HY000", "Network protocol option not supported"),
        OperationalError("HY000", "Server network configuration invalid"),
        OperationalError("HY000", "Connection pool size exceeded"),
        RuntimeError("Informix operation failed"),
    ],
)
def test_non_connection_errors_are_not_disconnects(dialect, error):
    assert dialect.is_disconnect(error, Connection(), None) is False


@pytest.mark.parametrize(
    "diagnostic",
    [
        "X08S01Y",
        "108S010",
        "unrelated native diagnostic -255820",
        "unrelated native diagnostic -110200",
        "positive application value 25582",
        "application parameter -25582",
    ],
)
def test_similar_diagnostic_tokens_do_not_match(dialect, diagnostic):
    error = OperationalError("HY000", diagnostic)

    assert dialect.is_disconnect(error, None, None) is False


def test_wrapper_statement_values_are_not_treated_as_diagnostics(dialect):
    inner = OperationalError("23000", "Constraint violation")
    outer = RuntimeError(
        "statement failed: INSERT INTO sample VALUES (-25582, '08S01')"
    )
    outer.orig = inner

    assert dialect.is_disconnect(outer, None, None) is False


def test_malformed_driver_properties_and_str_do_not_escape(dialect):
    class MalformedError(Error):
        @property
        def sqlstate(self):
            raise ValueError("broken driver property")

        def __str__(self):
            raise ValueError("broken driver string conversion")

    assert dialect.is_disconnect(MalformedError(), None, None) is False


def test_callable_message_attribute_is_not_executed(dialect):
    class DriverError(Error):
        def message(self):
            raise AssertionError("must not be called")

    assert dialect.is_disconnect(DriverError("HY000"), None, None) is False
