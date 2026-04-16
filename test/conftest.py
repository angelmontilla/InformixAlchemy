from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

DEFAULT_INFORMIX_SQLALCHEMY_URL = (
    "informix+pyodbc://ctl:magogo@192.168.11.64/faempre999"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&protocol=onsoctcp"
    "&server=pru_famadesa_s9"
    "&service=9088"
    "&DELIMIDENT=Y"
)


def _unique_name(prefix: str = "sa_") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@pytest.fixture(scope="session")
def informix_url() -> str:
    url = os.getenv("INFORMIX_SQLALCHEMY_URL") or DEFAULT_INFORMIX_SQLALCHEMY_URL
    if "delimident=" not in url.lower():
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}DELIMIDENT=Y"
    return url


@pytest.fixture(scope="session")
def engine(informix_url: str):
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
