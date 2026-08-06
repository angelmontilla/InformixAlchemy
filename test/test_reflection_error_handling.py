from __future__ import annotations

import logging

import pytest

from IfxAlchemy.pyodbc import IfxDialect_pyodbc


class FakeDBAPIError(Exception):
    pass


class BrokenMetadataCursor:
    def columns(self, **kwargs):
        raise FakeDBAPIError("metadata unavailable")

    def close(self):
        return None


class ProgrammingErrorCursor:
    def columns(self, **kwargs):
        raise ValueError("programming defect")

    def close(self):
        return None


def _connection_for(cursor):
    dbapi_connection = type("DBAPIConnection", (), {"cursor": lambda self: cursor})()
    proxy = type("Proxy", (), {"driver_connection": dbapi_connection})()
    return type("Connection", (), {"connection": proxy})()


def _dialect_with_fake_dbapi():
    dialect = IfxDialect_pyodbc()
    dialect.dbapi = type("DBAPI", (), {"Error": FakeDBAPIError})
    return dialect


def test_optional_odbc_metadata_error_uses_logged_fallback(caplog):
    dialect = _dialect_with_fake_dbapi()

    with caplog.at_level(logging.DEBUG, logger="IfxAlchemy.reflection"):
        result = dialect._reflector._odbc_column_metadata(
            _connection_for(BrokenMetadataCursor()),
            "events",
            "owner",
        )

    assert result == {}
    assert "using catalog reflection" in caplog.text


def test_programming_error_in_optional_metadata_is_not_hidden():
    dialect = _dialect_with_fake_dbapi()

    with pytest.raises(ValueError, match="programming defect"):
        dialect._reflector._odbc_column_metadata(
            _connection_for(ProgrammingErrorCursor()),
            "events",
            "owner",
        )
