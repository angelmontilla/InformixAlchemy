from __future__ import annotations

from contextlib import contextmanager

import pytest

from tools import official_suite_support as support


class _Result:
    def __init__(self, *, rows=(), scalar=None):
        self._rows = list(rows)
        self._scalar = scalar

    def fetchall(self):
        return list(self._rows)

    def scalar_one(self):
        return self._scalar


class _Connection:
    def __init__(
        self,
        *,
        actual_database="ifx_suite",
        default_owner="informix",
        rows_by_owner=None,
    ):
        self.actual_database = actual_database
        self.default_owner = default_owner
        self.rows_by_owner = rows_by_owner or {}
        self.calls = []

    def execute(self, statement):
        self.calls.append((str(statement), ()))
        return _Result(scalar=self.actual_database)

    def exec_driver_sql(self, statement, parameters=()):
        self.calls.append((statement, parameters))
        normalized = " ".join(statement.split()).upper()

        if normalized.startswith("SELECT USER FROM SYSTABLES"):
            return _Result(scalar=self.default_owner)

        if normalized.startswith("SELECT T.TABNAME"):
            owner = str(parameters[0]).casefold()
            return _Result(
                rows=self.rows_by_owner.get(owner, ())
            )

        raise AssertionError(f"Unexpected SQL in unit test: {statement}")


class _Engine:
    def __init__(self, connection):
        self.connection = connection
        self.disposed = False

    @contextmanager
    def connect(self):
        yield self.connection

    def dispose(self):
        self.disposed = True


def _configure_safe_environment(monkeypatch, expected="ifx_suite"):
    monkeypatch.setenv(
        "ALLOW_OFFICIAL_SUITE_DESTRUCTIVE_TESTS",
        "true",
    )
    monkeypatch.setenv(
        "OFFICIAL_SUITE_EXPECTED_DATABASE",
        expected,
    )
    monkeypatch.delenv(
        "FORBIDDEN_DATABASE_NAMES",
        raising=False,
    )
    monkeypatch.delenv(
        "OFFICIAL_SUITE_REQUIRE_EMPTY",
        raising=False,
    )


def test_collect_inventory_classifies_all_supported_catalog_types():
    connection = _Connection(
        rows_by_owner={
            "informix": [
                ("z_table", "T"),
                ("a_view", "V"),
            ],
            "test_schema": [
                ("user_id_seq", "Q"),
                ("users", "T"),
                ("ignored", "X"),
            ],
            "test_schema_2": [
                ("other_view", "V"),
                (None, "T"),
            ],
        }
    )

    inventory = support._collect_official_suite_inventory(
        connection
    )

    assert inventory == {
        "informix": {
            "tables": ["z_table"],
            "views": ["a_view"],
            "sequences": [],
        },
        "test_schema": {
            "tables": ["users"],
            "views": [],
            "sequences": ["user_id_seq"],
        },
        "test_schema_2": {
            "tables": [],
            "views": ["other_view"],
            "sequences": [],
        },
    }

    catalog_calls = [
        (statement, parameters)
        for statement, parameters in connection.calls
        if "SELECT\n            t.tabname" in statement
    ]
    assert len(catalog_calls) == 3
    assert all(
        "t.tabid >= 100" in statement
        and "t.tabtype IN ('T', 'V', 'Q')" in statement
        for statement, _ in catalog_calls
    )


def test_verify_strict_mode_rejects_objects_from_remote_owner(
    monkeypatch,
):
    _configure_safe_environment(monkeypatch)
    engine = _Engine(
        _Connection(
            rows_by_owner={
                "test_schema": [
                    ("users", "T"),
                ],
            }
        )
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: engine,
    )

    with pytest.raises(
        RuntimeError,
        match="objetos residuales",
    ) as captured:
        support.verify_official_suite_database(
            "informix+pyodbc://u:p@localhost/ifx_suite",
            require_empty=True,
        )

    assert "test_schema" in str(captured.value)
    assert "users" in str(captured.value)
    assert engine.disposed is True


def test_verify_non_strict_mode_returns_complete_dirty_inventory(
    monkeypatch,
):
    _configure_safe_environment(monkeypatch)
    engine = _Engine(
        _Connection(
            rows_by_owner={
                "informix": [
                    ("some_table", "T"),
                ],
                "test_schema": [
                    ("vv", "V"),
                    ("user_id_seq", "Q"),
                ],
            }
        )
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: engine,
    )

    result = support.verify_official_suite_database(
        "informix+pyodbc://u:p@localhost/ifx_suite",
        require_empty=False,
    )

    assert result["database"] == "ifx_suite"
    assert result["require_empty"] is False
    assert result["has_objects"] is True
    assert result["tables"] == ["some_table"]
    assert result["views"] == []
    assert result["sequences"] == []
    assert result["dirty_inventory"] == {
        "informix": {
            "tables": ["some_table"],
        },
        "test_schema": {
            "views": ["vv"],
            "sequences": ["user_id_seq"],
        },
    }
    assert engine.disposed is True


def test_verify_uses_environment_for_require_empty_when_not_overridden(
    monkeypatch,
):
    _configure_safe_environment(monkeypatch)
    monkeypatch.setenv(
        "OFFICIAL_SUITE_REQUIRE_EMPTY",
        "false",
    )
    engine = _Engine(
        _Connection(
            rows_by_owner={
                "test_schema_2": [
                    ("residual", "T"),
                ],
            }
        )
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: engine,
    )

    result = support.verify_official_suite_database(
        "informix+pyodbc://u:p@localhost/ifx_suite"
    )

    assert result["require_empty"] is False
    assert result["has_objects"] is True


def test_verify_rejects_forbidden_database_before_connecting(monkeypatch):
    _configure_safe_environment(
        monkeypatch,
        expected="prueba4db",
    )
    called = False

    def _unexpected_create_engine(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("create_engine must not be called")

    monkeypatch.setattr(
        support,
        "create_engine",
        _unexpected_create_engine,
    )

    with pytest.raises(
        RuntimeError,
        match="está prohibida",
    ):
        support.verify_official_suite_database(
            "informix+pyodbc://u:p@localhost/prueba4db",
            require_empty=False,
        )

    assert called is False


def test_verify_rejects_url_database_mismatch_before_connecting(monkeypatch):
    _configure_safe_environment(monkeypatch)
    called = False

    def _unexpected_create_engine(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("create_engine must not be called")

    monkeypatch.setattr(
        support,
        "create_engine",
        _unexpected_create_engine,
    )

    with pytest.raises(
        RuntimeError,
        match="La URL apunta",
    ):
        support.verify_official_suite_database(
            "informix+pyodbc://u:p@localhost/other_database",
            require_empty=False,
        )

    assert called is False


def test_verify_rejects_actual_database_mismatch(monkeypatch):
    _configure_safe_environment(monkeypatch)
    engine = _Engine(
        _Connection(actual_database="another_database")
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: engine,
    )

    with pytest.raises(
        RuntimeError,
        match="Informix conectó",
    ):
        support.verify_official_suite_database(
            "informix+pyodbc://u:p@localhost/ifx_suite",
            require_empty=False,
        )

    assert engine.disposed is True
