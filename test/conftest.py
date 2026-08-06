from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from tools.certification import (
    collect_runtime_provenance,
    junit_properties,
    normalise_dburi,
    render_safe_url,
    write_provenance,
)
from tools.official_suite_support import (
    ensure_non_ansi_test_database,
    load_non_ansi_test_environment,
    non_ansi_database_name,
    non_ansi_test_dburi,
)


def _unique_name(prefix: str = "sa_") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _build_informix_url() -> str:
    """Return the dedicated non-ANSI URL for project integration tests."""
    load_non_ansi_test_environment(required=False)
    return non_ansi_test_dburi()


def _smoke_check_informix_url(url: str) -> None:
    expected_database = non_ansi_database_name()
    engine = create_engine(url, pool_pre_ping=True)

    try:
        with engine.connect() as connection:
            database_name = connection.exec_driver_sql(
                "SELECT DBINFO('dbname') FROM systables WHERE tabid = 1"
            ).scalar_one()
            first_table = connection.exec_driver_sql(
                "SELECT FIRST 1 tabname FROM systables ORDER BY tabname"
            ).scalar_one()
    except Exception as exc:
        rendered_url = make_url(url).render_as_string(hide_password=True)
        raise pytest.UsageError(
            "Informix smoke check failed before running tests.\n"
            f"URL: {rendered_url}\n"
            "Check the configured user, password, host, service, server, "
            f"database={expected_database}.\n"
            f"Original error: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        engine.dispose()

    if str(database_name).strip().casefold() != expected_database.casefold():
        rendered_url = make_url(url).render_as_string(hide_password=True)
        raise pytest.UsageError(
            "Informix smoke check connected to an unexpected database.\n"
            f"URL: {rendered_url}\n"
            f"Connected database: {database_name!r}\n"
            f"Expected database: {expected_database!r}"
        )

    if not str(first_table).strip():
        rendered_url = make_url(url).render_as_string(hide_password=True)
        raise pytest.UsageError(
            "Informix smoke check succeeded, but systables returned no rows.\n"
            f"URL: {rendered_url}"
        )


_INFORMIX_FIXTURES = {
    "conn",
    "db_builder",
    "engine",
    "pinned_connection_session",
}

_OFFICIAL_SUITE_FILES = {
    "test_out_parameters.py",
    "test_suite.py",
    "test_suite_alembic.py",
}


def pytest_addoption(parser):
    parser.addoption(
        "--run-informix",
        action="store_true",
        default=False,
        help="run tests that require a live Informix server",
    )


def _is_official_suite_run(config) -> bool:
    if config.pluginmanager.hasplugin(
        "sqlalchemy.testing.plugin.pytestplugin"
    ):
        return True

    try:
        return bool(config.getoption("dburi"))
    except (AttributeError, ValueError):
        return False


def pytest_ignore_collect(collection_path, config):
    if collection_path.name in _OFFICIAL_SUITE_FILES:
        return not _is_official_suite_run(config)
    return False


def _is_live_informix_run(config) -> bool:
    try:
        explicitly_enabled = bool(config.getoption("--run-informix"))
    except (AttributeError, ValueError):
        explicitly_enabled = False
    return explicitly_enabled or _is_official_suite_run(config)


def pytest_collection_modifyitems(config, items):
    run_live = _is_live_informix_run(config)
    skip_live = pytest.mark.skip(
        reason="requires live Informix; rerun with --run-informix",
    )
    for item in items:
        if _INFORMIX_FIXTURES.intersection(getattr(item, "fixturenames", ())):
            item.add_marker(pytest.mark.requires_informix)
        if item.get_closest_marker("requires_informix") and not run_live:
            item.add_marker(skip_live)


@pytest.fixture(scope="session", autouse=True)
def certification_provenance(request, record_testsuite_property):
    """Attach exact runtime provenance to every live JUnit test report."""
    if not _is_live_informix_run(request.config):
        yield None
        return

    if _is_official_suite_run(request.config):
        try:
            raw_url = request.config.getoption("dburi")
        except (AttributeError, ValueError):
            raw_url = None
        try:
            url = normalise_dburi(raw_url)
        except (TypeError, ValueError) as exc:
            raise pytest.UsageError(str(exc)) from exc
    else:
        url = _build_informix_url()
        ensure_non_ansi_test_database(url)

    if not url:
        raise pytest.UsageError(
            "A live Informix run requires a SQLAlchemy --dburi or configured "
            "INFORMIX_SQLALCHEMY_NON_ANSI_URL."
        )

    provenance_engine = None
    try:
        provenance_engine = create_engine(url, pool_pre_ping=True)
        with provenance_engine.connect() as connection:
            report = collect_runtime_provenance(
                url=url,
                connection=connection,
            )
    except Exception as exc:
        raise pytest.UsageError(
            "Could not collect mandatory Informix certification provenance.\n"
            f"URL: {render_safe_url(url)}\n"
            f"Original error: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if provenance_engine is not None:
            provenance_engine.dispose()

    output = Path(
        os.getenv(
            "IFXALCHEMY_CERTIFICATION_JSON",
            "artifacts/certification/provenance.json",
        )
    )
    write_provenance(output, report)
    for name, value in junit_properties(report).items():
        record_testsuite_property(name, value)

    yield report


@pytest.fixture(scope="session")
def informix_url() -> str:
    return _build_informix_url()


@pytest.fixture(scope="session")
def engine(informix_url: str):
    ensure_non_ansi_test_database(informix_url)
    _smoke_check_informix_url(informix_url)
    eng = create_engine(informix_url, pool_pre_ping=True)
    try:
        yield eng
    finally:
        eng.dispose()


@pytest.fixture
def conn(engine):
    with engine.connect() as connection:
        yield connection


@pytest.fixture
def pinned_connection_session(engine):
    """Pin Session and Connection to the same physical connection."""
    with engine.connect() as connection:
        with Session(bind=connection, expire_on_commit=False) as session:
            yield connection, session


@pytest.fixture
def name_factory():
    return _unique_name


@pytest.fixture
def qident():
    return _quote_ident


@pytest.fixture
def db_builder(engine):
    """Execute DDL and clean it up in reverse order."""
    created_groups: list[list[str]] = []

    def _build(create_sqls, drop_sqls):
        create_list = (
            [create_sqls]
            if isinstance(create_sqls, str)
            else list(create_sqls)
        )
        drop_list = (
            [drop_sqls]
            if isinstance(drop_sqls, str)
            else list(drop_sqls)
        )

        with engine.connect() as connection:
            for statement in create_list:
                connection.exec_driver_sql(statement)
            connection.commit()

        created_groups.append(drop_list)

    yield _build

    with engine.connect() as connection:
        for drop_list in reversed(created_groups):
            for statement in drop_list:
                try:
                    connection.exec_driver_sql(statement)
                    connection.commit()
                except Exception:
                    connection.rollback()
