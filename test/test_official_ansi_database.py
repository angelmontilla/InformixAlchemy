from __future__ import annotations

import pytest

from tools.official_suite_support import (
    ansi_database_name,
    ansi_test_dburi,
    ensure_required_test_databases,
    load_non_ansi_test_environment,
    load_official_suite_environment,
    non_ansi_database_name,
    non_ansi_test_dburi,
    verify_test_database,
)


@pytest.mark.requires_informix
@pytest.mark.sqlalchemy_suite
def test_required_test_databases_exist_with_expected_modes():
    """Provision and certify the two isolated Docker test databases.

    * ``ifxalchemy_test`` is transactional and non-ANSI. It is used by the
      package's normal integration tests.
    * ``ifxalchemy_test_ansi`` is transactional and ANSI. It is used by the
      official SQLAlchemy and Alembic suites so schema/owner tests are enabled.

    Missing databases are created. Existing databases are never dropped or
    converted; a mode mismatch fails explicitly.
    """
    load_non_ansi_test_environment(required=False)
    load_official_suite_environment(required=False)

    provisioned = ensure_required_test_databases()

    non_ansi = provisioned["non_ansi"]
    ansi = provisioned["ansi"]

    assert non_ansi["database"].casefold() == (
        non_ansi_database_name().casefold()
    )
    assert non_ansi["is_ansi_database"] is False
    assert non_ansi["is_logging"] is True

    assert ansi["database"].casefold() == ansi_database_name().casefold()
    assert ansi["is_ansi_database"] is True
    assert ansi["is_logging"] is True

    non_ansi_verified = verify_test_database(
        non_ansi_test_dburi(),
        expected_database=non_ansi_database_name(),
        expected_ansi=False,
    )
    ansi_verified = verify_test_database(
        ansi_test_dburi(),
        expected_database=ansi_database_name(),
        expected_ansi=True,
    )

    assert non_ansi_verified["is_ansi_database"] is False
    assert ansi_verified["is_ansi_database"] is True
