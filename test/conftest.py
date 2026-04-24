from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

DEFAULT_INFORMIX_SQLALCHEMY_URL = (
    "informix+pyodbc://informix:in4mix@127.0.0.1/prueba4db"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&protocol=onsoctcp"
    "&server=informix"
    "&service=9088"
    "&DELIMIDENT=Y"
)


def _unique_name(prefix: str = "sa_") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _build_informix_url() -> str:
    url = os.getenv("INFORMIX_SQLALCHEMY_URL") or DEFAULT_INFORMIX_SQLALCHEMY_URL
    if "delimident=" not in url.lower():
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}DELIMIDENT=Y"
    return url


def _smoke_check_informix_url(url: str) -> None:
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
            "Expected Docker defaults: "
            "user=informix, password=in4mix, host=127.0.0.1, "
            "service=9088, server=informix, database=prueba4db.\n"
            f"Original error: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        engine.dispose()

    if str(database_name).strip() != "prueba4db":
        rendered_url = make_url(url).render_as_string(hide_password=True)
        raise pytest.UsageError(
            "Informix smoke check connected to an unexpected database.\n"
            f"URL: {rendered_url}\n"
            f"Connected database: {database_name!r}\n"
            "Expected database: 'prueba4db'"
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


def _is_sqlalchemy_suite_run(config) -> bool:
    if config.pluginmanager.hasplugin("sqlalchemy.testing.plugin.pytestplugin"):
        return True
    try:
        return bool(config.getoption("dburi"))
    except (AttributeError, ValueError):
        return False


def pytest_ignore_collect(collection_path, config):
    if collection_path.name in {"test_out_parameters.py", "test_suite.py"}:
        return not _is_sqlalchemy_suite_run(config)
    return False


def pytest_collection_modifyitems(config, items):
    for item in items:
        if _INFORMIX_FIXTURES.intersection(getattr(item, "fixturenames", ())):
            item.add_marker(pytest.mark.requires_informix)


@pytest.fixture(scope="session")
def informix_url() -> str:
    return _build_informix_url()


@pytest.fixture(scope="session")
def engine(informix_url: str):
    _smoke_check_informix_url(informix_url)
    eng = create_engine(
        informix_url,
        pool_pre_ping=True,
    )
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
    """
    Fija Session + Connection a la misma conexión física.
    Es clave para TEMP TABLES en Informix.
    """
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
    """
    build(create_sqls, drop_sqls)

    - create_sqls: str o iterable[str]
    - drop_sqls: str o iterable[str]

    Hace CREATE con commit explícito y limpia al final en orden inverso.
    """
    created_groups: list[list[str]] = []

    def _build(create_sqls, drop_sqls):
        create_list = [create_sqls] if isinstance(create_sqls, str) else list(create_sqls)
        drop_list = [drop_sqls] if isinstance(drop_sqls, str) else list(drop_sqls)

        with engine.connect() as connection:
            for stmt in create_list:
                connection.exec_driver_sql(stmt)
            connection.commit()

        created_groups.append(drop_list)

    yield _build

    with engine.connect() as connection:
        for drop_list in reversed(created_groups):
            for stmt in drop_list:
                try:
                    connection.exec_driver_sql(stmt)
                    connection.commit()
                except Exception:
                    connection.rollback()
