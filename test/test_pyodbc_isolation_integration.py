from __future__ import annotations

import pytest
from sqlalchemy import create_engine


pytestmark = [
    pytest.mark.requires_informix,
]


@pytest.mark.parametrize(
    "level",
    [
        "READ UNCOMMITTED",
        "DIRTY READ",
        "READ COMMITTED",
        "COMMITTED READ",
        "REPEATABLE READ",
        "SERIALIZABLE",
        "UNCOMMITTED READ",
        "UR",
        "CURSOR STABILITY",
        "CS",
        "READ STABILITY",
        "RS",
        "RR",
    ],
)
def test_connection_execution_option_sets_and_reports_isolation(engine, level):
    """Exercise SQLAlchemy's public per-Connection isolation API."""

    with engine.connect() as connection:
        connection.execution_options(isolation_level=level)

        assert connection.get_isolation_level() == level
        assert connection.exec_driver_sql(
            "SELECT FIRST 1 tabname FROM systables ORDER BY tabname"
        ).scalar_one()
        connection.rollback()


def test_pool_checkout_restores_default_isolation_level(engine):
    default = engine.dialect.default_isolation_level

    with engine.connect() as connection:
        connection.execution_options(isolation_level="RR")
        assert connection.get_isolation_level() == "RR"

    with engine.connect() as connection:
        assert connection.get_isolation_level() == default


def test_engine_execution_option_is_reapplied_after_pool_reset(engine):
    isolated_engine = engine.execution_options(isolation_level="RS")

    with isolated_engine.connect() as connection:
        assert connection.get_isolation_level() == "RS"

    with isolated_engine.connect() as connection:
        assert connection.get_isolation_level() == "RS"

    with engine.connect() as connection:
        assert (
            connection.get_isolation_level()
            == engine.dialect.default_isolation_level
        )


def test_create_engine_isolation_level_parameter(informix_url):
    isolated_engine = create_engine(
        informix_url,
        isolation_level="CURSOR STABILITY",
        pool_pre_ping=True,
    )
    try:
        with isolated_engine.connect() as connection:
            assert connection.get_isolation_level() == "CURSOR STABILITY"
    finally:
        isolated_engine.dispose()


def test_create_engine_setting_is_restored_after_connection_override(
    informix_url,
):
    """Regression for SQLAlchemy IsolationLevelTest reset semantics."""

    isolated_engine = create_engine(
        informix_url,
        isolation_level="CURSOR STABILITY",
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
    )
    try:
        with isolated_engine.connect() as connection:
            assert (
                connection.get_isolation_level()
                == "CURSOR STABILITY"
            )

        with isolated_engine.connect() as connection:
            connection.execution_options(
                isolation_level="READ COMMITTED"
            )
            assert connection.get_isolation_level() == "READ COMMITTED"

        # The same pooled DBAPI connection must return to the Engine-wide
        # baseline after the per-Connection option is reset on check-in.
        with isolated_engine.connect() as connection:
            assert (
                connection.get_isolation_level()
                == "CURSOR STABILITY"
            )
    finally:
        isolated_engine.dispose()

