from __future__ import annotations

import pytest

from tools.official_suite_support import (
    ensure_official_suite_ansi_database,
    load_official_suite_environment,
    official_suite_dburi,
    verify_official_suite_database,
)


@pytest.mark.requires_informix
@pytest.mark.sqlalchemy_suite
def test_official_suite_database_exists_and_is_ansi():
    """Provision the disposable Docker database and certify ANSI mode.

    The operation is idempotent: it creates the configured database only when
    it is absent. If a database with the same name already exists in non-ANSI
    mode, the test fails without dropping or converting it.
    """
    load_official_suite_environment()
    dburi = official_suite_dburi()

    provisioned = ensure_official_suite_ansi_database(
        dburi
    )

    assert provisioned["is_ansi_database"] is True

    verified = verify_official_suite_database(
        dburi,
        require_empty=False,
    )

    assert verified["database"].casefold() == (
        provisioned["database"].casefold()
    )
    assert verified["is_ansi_database"] is True
