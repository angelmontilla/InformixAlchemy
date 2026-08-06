from __future__ import annotations

import re
from contextlib import contextmanager
from types import SimpleNamespace

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

    def first(self):
        return self._rows[0] if self._rows else None


class _Connection:
    def __init__(
        self,
        *,
        actual_database="ifxalchemy_test_ansi",
        default_owner="informix",
        rows_by_owner=None,
        database_states=None,
    ):
        self.actual_database = actual_database
        self.default_owner = default_owner
        self.rows_by_owner = rows_by_owner or {}
        self.database_states = {
            str(name).casefold(): tuple(state)
            for name, state in (database_states or {}).items()
        }
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
            return _Result(rows=self.rows_by_owner.get(owner, ()))

        if "FROM SYSMASTER:SYSDATABASES D" in normalized:
            database_name = str(parameters[0]).strip()
            state = self.database_states.get(database_name.casefold())
            if state is None:
                return _Result(rows=())
            is_ansi, is_logging = state
            return _Result(
                rows=[
                    (
                        database_name,
                        1 if is_ansi else 0,
                        1 if is_logging else 0,
                    )
                ]
            )

        if normalized.startswith("CREATE DATABASE "):
            match = re.match(
                r"CREATE\s+DATABASE\s+([A-Za-z_][A-Za-z0-9_$]*)",
                statement,
                re.IGNORECASE,
            )
            assert match is not None
            database_name = match.group(1)
            is_ansi = "WITH LOG MODE ANSI" in normalized
            is_logging = "WITH LOG" in normalized
            self.database_states[database_name.casefold()] = (
                is_ansi,
                is_logging,
            )
            return _Result()

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


def _configure_environment(monkeypatch):
    monkeypatch.setenv(
        "IFXALCHEMY_NON_ANSI_DATABASE",
        "ifxalchemy_test",
    )
    monkeypatch.setenv(
        "IFXALCHEMY_ANSI_DATABASE",
        "ifxalchemy_test_ansi",
    )
    monkeypatch.setenv(
        "IFXALCHEMY_CREATE_TEST_DATABASES_IF_MISSING",
        "true",
    )
    monkeypatch.setenv(
        "ALLOW_OFFICIAL_SUITE_DESTRUCTIVE_TESTS",
        "true",
    )
    monkeypatch.delenv(
        "FORBIDDEN_DATABASE_NAMES",
        raising=False,
    )
    monkeypatch.delenv(
        "OFFICIAL_SUITE_DATABASE_DBSPACE",
        raising=False,
    )
    monkeypatch.delenv(
        "IFXALCHEMY_NON_ANSI_DATABASE_DBSPACE",
        raising=False,
    )
    monkeypatch.delenv(
        "IFXALCHEMY_ANSI_DATABASE_DBSPACE",
        raising=False,
    )
    monkeypatch.setenv("IFXALCHEMY_DOCKER_CONTAINER", "ifx")
    monkeypatch.setenv("IFXALCHEMY_DOCKER_USER", "informix")
    monkeypatch.delenv("IFXALCHEMY_DOCKER_DBACCESS", raising=False)
    for variable in (
        "IFXALCHEMY_DOCKER_INFORMIXDIR",
        "IFXALCHEMY_DOCKER_INFORMIXSERVER",
        "IFXALCHEMY_DOCKER_CLIENT_LOCALE",
        "IFXALCHEMY_DOCKER_DB_LOCALE",
        "IFXALCHEMY_DOCKER_SERVER_LOCALE",
        "IFXALCHEMY_DOCKER_PATH",
        "IFXALCHEMY_DOCKER_LD_LIBRARY_PATH",
        "IFXALCHEMY_DOCKER_TERM",
    ):
        monkeypatch.delenv(variable, raising=False)


def _install_fake_database_creator(monkeypatch, connection):
    statements = []

    def _create(create_sql):
        statements.append(create_sql)
        match = re.match(
            r"CREATE\s+DATABASE\s+([A-Za-z_][A-Za-z0-9_$]*)",
            create_sql,
            re.IGNORECASE,
        )
        assert match is not None
        database_name = match.group(1)
        normalized = " ".join(create_sql.split()).upper()
        connection.database_states[database_name.casefold()] = (
            "WITH LOG MODE ANSI" in normalized,
            "WITH LOG" in normalized,
        )

    monkeypatch.setattr(support, "_create_database_with_docker", _create)
    return statements


def test_legacy_url_is_split_into_two_database_profiles(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setenv(
        "INFORMIX_SQLALCHEMY_SUITE_URL",
        "informix+pyodbc://u:p@localhost/old_database?DELIMIDENT=Y",
    )
    monkeypatch.delenv(
        "INFORMIX_SQLALCHEMY_NON_ANSI_URL",
        raising=False,
    )
    monkeypatch.delenv(
        "INFORMIX_SQLALCHEMY_ANSI_URL",
        raising=False,
    )
    monkeypatch.delenv(
        "INFORMIX_SQLALCHEMY_URL",
        raising=False,
    )

    assert support.make_url(
        support.non_ansi_test_dburi()
    ).database == "ifxalchemy_test"
    assert support.make_url(
        support.ansi_test_dburi()
    ).database == "ifxalchemy_test_ansi"
    assert support.official_suite_dburi() == support.ansi_test_dburi()


def test_explicit_profile_url_must_use_its_database(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setenv(
        "INFORMIX_SQLALCHEMY_ANSI_URL",
        "informix+pyodbc://u:p@localhost/ifxalchemy_test",
    )

    with pytest.raises(RuntimeError, match="debe apuntar"):
        support.ansi_test_dburi()


def test_ensure_missing_non_ansi_database_uses_with_log(monkeypatch):
    _configure_environment(monkeypatch)
    connection = _Connection(
        actual_database="sysmaster",
        database_states={"sysmaster": (False, True)},
    )
    engines = []

    def _create_engine(*args, **kwargs):
        engine = _Engine(connection)
        engines.append((engine, args, kwargs))
        return engine

    monkeypatch.setattr(support, "create_engine", _create_engine)
    create_statements = _install_fake_database_creator(
        monkeypatch, connection
    )

    result = support.ensure_non_ansi_test_database(
        "informix+pyodbc://u:p@localhost/ifxalchemy_test"
    )

    assert result["created"] is True
    assert result["is_ansi_database"] is False
    assert result["is_logging"] is True
    assert create_statements == [
        "CREATE DATABASE ifxalchemy_test WITH LOG"
    ]
    assert all(engine.disposed for engine, _, _ in engines)


def test_ensure_missing_ansi_database_uses_ansi_logging(monkeypatch):
    _configure_environment(monkeypatch)
    connection = _Connection(
        actual_database="sysmaster",
        database_states={"sysmaster": (False, True)},
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: _Engine(connection),
    )
    create_statements = _install_fake_database_creator(
        monkeypatch, connection
    )

    result = support.ensure_ansi_test_database(
        "informix+pyodbc://u:p@localhost/ifxalchemy_test_ansi"
    )

    assert result["created"] is True
    assert result["is_ansi_database"] is True
    assert create_statements == [
        "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
    ]


def test_database_provisioning_is_idempotent(monkeypatch):
    _configure_environment(monkeypatch)
    connection = _Connection(
        actual_database="sysmaster",
        database_states={
            "sysmaster": (False, True),
            "ifxalchemy_test": (False, True),
            "ifxalchemy_test_ansi": (True, True),
        },
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: _Engine(connection),
    )
    create_statements = _install_fake_database_creator(
        monkeypatch, connection
    )

    non_ansi = support.ensure_non_ansi_test_database(
        "informix+pyodbc://u:p@localhost/ifxalchemy_test"
    )
    ansi = support.ensure_ansi_test_database(
        "informix+pyodbc://u:p@localhost/ifxalchemy_test_ansi"
    )

    assert non_ansi["created"] is False
    assert ansi["created"] is False
    assert create_statements == []


@pytest.mark.parametrize(
    ("function_name", "url", "states", "message"),
    [
        (
            "ensure_non_ansi_test_database",
            "informix+pyodbc://u:p@localhost/ifxalchemy_test",
            {"ifxalchemy_test": (True, True)},
            "requiere modo no ANSI",
        ),
        (
            "ensure_ansi_test_database",
            "informix+pyodbc://u:p@localhost/ifxalchemy_test_ansi",
            {"ifxalchemy_test_ansi": (False, True)},
            "requiere modo ANSI",
        ),
    ],
)
def test_existing_database_with_wrong_mode_is_rejected(
    monkeypatch,
    function_name,
    url,
    states,
    message,
):
    _configure_environment(monkeypatch)
    connection = _Connection(
        actual_database="sysmaster",
        database_states={"sysmaster": (False, True), **states},
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: _Engine(connection),
    )

    with pytest.raises(RuntimeError, match=message):
        getattr(support, function_name)(url)


def test_existing_unlogged_database_is_rejected(monkeypatch):
    _configure_environment(monkeypatch)
    connection = _Connection(
        actual_database="sysmaster",
        database_states={
            "sysmaster": (False, True),
            "ifxalchemy_test": (False, False),
        },
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: _Engine(connection),
    )

    with pytest.raises(RuntimeError, match="existe sin logging"):
        support.ensure_non_ansi_test_database(
            "informix+pyodbc://u:p@localhost/ifxalchemy_test"
        )


def test_profile_specific_dbspaces_are_used(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setenv(
        "IFXALCHEMY_NON_ANSI_DATABASE_DBSPACE",
        "data_dbs",
    )
    monkeypatch.setenv(
        "IFXALCHEMY_ANSI_DATABASE_DBSPACE",
        "ansi_dbs",
    )
    connection = _Connection(
        actual_database="sysmaster",
        database_states={"sysmaster": (False, True)},
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: _Engine(connection),
    )
    create_statements = _install_fake_database_creator(
        monkeypatch, connection
    )

    support.ensure_non_ansi_test_database(
        "informix+pyodbc://u:p@localhost/ifxalchemy_test"
    )
    support.ensure_ansi_test_database(
        "informix+pyodbc://u:p@localhost/ifxalchemy_test_ansi"
    )

    assert create_statements == [
        "CREATE DATABASE ifxalchemy_test IN data_dbs WITH LOG",
        (
            "CREATE DATABASE ifxalchemy_test_ansi IN ansi_dbs "
            "WITH LOG MODE ANSI"
        ),
    ]


def test_docker_database_creation_discovers_and_executes_absolute_path(
    monkeypatch,
):
    _configure_environment(monkeypatch)
    observed = []

    def _run(command, **kwargs):
        observed.append((command, kwargs))
        if "find" in command[5]:
            return SimpleNamespace(
                returncode=0,
                stdout="/opt/ibm/informix/bin/dbaccess\n",
                stderr="",
            )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(support.subprocess, "run", _run)

    support._create_database_with_docker(
        "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
    )

    discovery_command, discovery_kwargs = observed[0]
    assert discovery_command == [
        "docker",
        "exec",
        "-u",
        "informix",
        "ifx",
        "/usr/bin/find",
        "/opt",
        "-type",
        "f",
        "-name",
        "dbaccess",
        "-print",
    ]
    assert discovery_kwargs["check"] is False

    execute_command, execute_kwargs = observed[1]
    assert execute_command == [
        "docker",
        "exec",
        "-i",
        "-u",
        "informix",
        "-e",
        "INFORMIXDIR=/opt/ibm/informix",
        "-e",
        "INFORMIXSERVER=informix",
        "-e",
        "CLIENT_LOCALE=en_US.819",
        "-e",
        "DB_LOCALE=en_US.819",
        "-e",
        "SERVER_LOCALE=en_US.819",
        "-e",
        (
            "PATH=/opt/ibm/informix/bin:/usr/local/sbin:/usr/local/bin:"
            "/usr/sbin:/usr/bin:/sbin:/bin"
        ),
        "-e",
        (
            "LD_LIBRARY_PATH=/opt/ibm/informix/lib:"
            "/opt/ibm/informix/lib/esql:/opt/ibm/informix/lib/cli"
        ),
        "-e",
        "TERM=dumb",
        "ifx",
        "/opt/ibm/informix/bin/dbaccess",
        "-",
        "-",
    ]
    assert execute_kwargs["input"] == (
        "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI;\n"
    )
    assert execute_kwargs["check"] is False


def test_docker_database_creation_accepts_absolute_dbaccess_path(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setenv(
        "IFXALCHEMY_DOCKER_DBACCESS",
        "/custom/informix/bin/dbaccess",
    )
    observed = {}

    def _run(command, **kwargs):
        observed["command"] = command
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(support.subprocess, "run", _run)
    support._create_database_with_docker(
        "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
    )

    assert observed["command"] == [
        "docker",
        "exec",
        "-i",
        "-u",
        "informix",
        "-e",
        "INFORMIXDIR=/custom/informix",
        "-e",
        "INFORMIXSERVER=informix",
        "-e",
        "CLIENT_LOCALE=en_US.819",
        "-e",
        "DB_LOCALE=en_US.819",
        "-e",
        "SERVER_LOCALE=en_US.819",
        "-e",
        (
            "PATH=/custom/informix/bin:/usr/local/sbin:/usr/local/bin:"
            "/usr/sbin:/usr/bin:/sbin:/bin"
        ),
        "-e",
        (
            "LD_LIBRARY_PATH=/custom/informix/lib:"
            "/custom/informix/lib/esql:/custom/informix/lib/cli"
        ),
        "-e",
        "TERM=dumb",
        "ifx",
        "/custom/informix/bin/dbaccess",
        "-",
        "-",
    ]


def test_docker_database_creation_allows_explicit_gls_environment(
    monkeypatch,
):
    _configure_environment(monkeypatch)
    monkeypatch.setenv(
        "IFXALCHEMY_DOCKER_DBACCESS",
        "/opt/ibm/informix/bin/dbaccess",
    )
    monkeypatch.setenv(
        "IFXALCHEMY_DOCKER_INFORMIXDIR",
        "/opt/ibm/informix",
    )
    monkeypatch.setenv(
        "IFXALCHEMY_DOCKER_INFORMIXSERVER",
        "ifxserver",
    )
    monkeypatch.setenv(
        "IFXALCHEMY_DOCKER_CLIENT_LOCALE",
        "es_es.8859-1",
    )
    monkeypatch.setenv(
        "IFXALCHEMY_DOCKER_DB_LOCALE",
        "es_es.8859-1",
    )
    monkeypatch.setenv(
        "IFXALCHEMY_DOCKER_SERVER_LOCALE",
        "en_us.819",
    )
    observed = {}

    def _run(command, **kwargs):
        observed["command"] = command
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(support.subprocess, "run", _run)
    support._create_database_with_docker(
        "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
    )

    command = observed["command"]
    assert "INFORMIXSERVER=ifxserver" in command
    assert "CLIENT_LOCALE=es_es.8859-1" in command
    assert "DB_LOCALE=es_es.8859-1" in command
    assert "SERVER_LOCALE=en_us.819" in command


def test_dbaccess_gls_error_adds_actionable_diagnostic(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setenv(
        "IFXALCHEMY_DOCKER_DBACCESS",
        "/opt/ibm/informix/bin/dbaccess",
    )
    monkeypatch.setattr(
        support.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=255,
            stdout="",
            stderr="-23101 Unable to load locale categories.",
        ),
    )

    with pytest.raises(RuntimeError, match=r"/opt/ibm/informix/gls"):
        support._create_database_with_docker(
            "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
        )


def test_dbaccess_path_must_be_absolute(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setenv("IFXALCHEMY_DOCKER_DBACCESS", "dbaccess")

    with pytest.raises(RuntimeError, match="ruta absoluta"):
        support._create_database_with_docker(
            "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
        )


def test_dbaccess_discovery_reports_cmd_command(monkeypatch):
    _configure_environment(monkeypatch)

    monkeypatch.setattr(
        support.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="not found",
        ),
    )

    with pytest.raises(RuntimeError, match=r"docker exec -u informix ifx"):
        support._create_database_with_docker(
            "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
        )


def test_docker_database_creation_reports_dbaccess_failure(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setattr(
        support.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="database creation failed",
        ),
    )

    with pytest.raises(RuntimeError, match="database creation failed"):
        support._create_database_with_docker(
            "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
        )


def test_docker_timeout_must_be_positive(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setenv("IFXALCHEMY_DOCKER_TIMEOUT", "0")

    with pytest.raises(RuntimeError, match="entero positivo"):
        support._create_database_with_docker(
            "CREATE DATABASE ifxalchemy_test_ansi WITH LOG MODE ANSI"
        )


def test_ensure_required_test_databases_provisions_both(monkeypatch):
    monkeypatch.setattr(
        support,
        "ensure_non_ansi_test_database",
        lambda: {"database": "ifxalchemy_test"},
    )
    monkeypatch.setattr(
        support,
        "ensure_ansi_test_database",
        lambda: {"database": "ifxalchemy_test_ansi"},
    )

    result = support.ensure_required_test_databases()

    assert result == {
        "non_ansi": {"database": "ifxalchemy_test"},
        "ansi": {"database": "ifxalchemy_test_ansi"},
    }


def test_official_suite_target_is_always_the_ansi_database(monkeypatch):
    _configure_environment(monkeypatch)
    # Previous patches used this variable for the non-ANSI target. It is now
    # intentionally ignored so stale local files cannot mix both profiles.
    monkeypatch.setenv(
        "OFFICIAL_SUITE_EXPECTED_DATABASE",
        "ifxalchemy_test",
    )

    expected, parsed = support._authorized_official_suite_target(
        "informix+pyodbc://u:p@localhost/ifxalchemy_test_ansi"
    )

    assert expected == "ifxalchemy_test_ansi"
    assert parsed.database == "ifxalchemy_test_ansi"


def test_official_suite_rejects_non_ansi_target(monkeypatch):
    _configure_environment(monkeypatch)

    with pytest.raises(RuntimeError, match="base ANSI autorizada"):
        support._authorized_official_suite_target(
            "informix+pyodbc://u:p@localhost/ifxalchemy_test"
        )


def test_same_name_for_both_profiles_is_rejected(monkeypatch):
    _configure_environment(monkeypatch)
    monkeypatch.setenv(
        "IFXALCHEMY_ANSI_DATABASE",
        "ifxalchemy_test",
    )

    with pytest.raises(RuntimeError, match="nombres diferentes"):
        support._authorized_official_suite_target(
            "informix+pyodbc://u:p@localhost/ifxalchemy_test"
        )


def test_collect_inventory_classifies_catalog_objects():
    connection = _Connection(
        rows_by_owner={
            "informix": [("z_table", "T"), ("a_view", "V")],
            "test_schema": [("user_id_seq", "Q"), ("users", "T")],
            "test_schema_2": [("other_view", "V")],
        }
    )

    inventory = support._collect_official_suite_inventory(connection)

    assert inventory["informix"] == {
        "tables": ["z_table"],
        "views": ["a_view"],
        "sequences": [],
    }
    assert inventory["test_schema"] == {
        "tables": ["users"],
        "views": [],
        "sequences": ["user_id_seq"],
    }


def test_verify_official_suite_database_requires_ansi_logging(monkeypatch):
    _configure_environment(monkeypatch)
    connection = _Connection(
        actual_database="ifxalchemy_test_ansi",
        database_states={
            "ifxalchemy_test_ansi": (True, True),
        },
    )
    engine = _Engine(connection)
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: engine,
    )

    result = support.verify_official_suite_database(
        "informix+pyodbc://u:p@localhost/ifxalchemy_test_ansi",
        require_empty=False,
    )

    assert result["database"] == "ifxalchemy_test_ansi"
    assert result["is_ansi_database"] is True
    assert result["is_logging"] is True
    assert engine.disposed is True


def test_verify_official_suite_strict_mode_rejects_residual_objects(
    monkeypatch,
):
    _configure_environment(monkeypatch)
    connection = _Connection(
        actual_database="ifxalchemy_test_ansi",
        database_states={
            "ifxalchemy_test_ansi": (True, True),
        },
        rows_by_owner={
            "test_schema": [("users", "T")],
        },
    )
    monkeypatch.setattr(
        support,
        "create_engine",
        lambda *args, **kwargs: _Engine(connection),
    )

    with pytest.raises(RuntimeError, match="objetos residuales"):
        support.verify_official_suite_database(
            "informix+pyodbc://u:p@localhost/ifxalchemy_test_ansi",
            require_empty=True,
        )


def test_non_ansi_environment_does_not_override_process_url(
    monkeypatch,
    tmp_path,
):
    env_file = tmp_path / ".env.informix"
    env_file.write_text(
        "INFORMIX_SQLALCHEMY_NON_ANSI_URL="
        "informix+pyodbc://file:file@filehost/ifxalchemy_test\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(support, "NON_ANSI_ENV_FILE", env_file)
    monkeypatch.setenv(
        "INFORMIX_SQLALCHEMY_NON_ANSI_URL",
        "informix+pyodbc://shell:shell@shellhost/ifxalchemy_test",
    )

    loaded = support.load_non_ansi_test_environment(required=True)

    assert loaded == env_file
    assert support.make_url(
        support.non_ansi_test_dburi()
    ).host == "shellhost"


def test_official_environment_does_not_replace_non_ansi_profile(
    monkeypatch,
    tmp_path,
):
    env_file = tmp_path / ".env.official-suites"
    env_file.write_text(
        "INFORMIX_SQLALCHEMY_ANSI_URL="
        "informix+pyodbc://u:p@ansihost/ifxalchemy_test_ansi\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(support, "OFFICIAL_ENV_FILE", env_file)
    monkeypatch.setenv(
        "INFORMIX_SQLALCHEMY_NON_ANSI_URL",
        "informix+pyodbc://u:p@normalhost/ifxalchemy_test",
    )

    support.load_official_suite_environment(required=True)

    assert support.make_url(
        support.non_ansi_test_dburi()
    ).host == "normalhost"
    assert support.make_url(
        support.ansi_test_dburi()
    ).host == "ansihost"
