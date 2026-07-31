from __future__ import annotations

import pytest

from IfxAlchemy.base import IfxDialect


pytestmark = pytest.mark.alembic_suite


def test_alembic_migration_context_selects_informix_impl() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext

    from IfxAlchemy.alembic import InformixImpl

    context = MigrationContext.configure(dialect=IfxDialect())

    assert isinstance(context.impl, InformixImpl)
    assert context.impl.__dialect__ == "informix"
    assert context.impl.transactional_ddl is False


def test_alembic_impl_is_registered_by_package_import() -> None:
    pytest.importorskip("alembic")
    from alembic.ddl.impl import DefaultImpl

    dialect = IfxDialect()

    assert DefaultImpl.get_by_dialect(dialect).__name__ == "InformixImpl"


def _functional_indexes_for_comparison():
    from sqlalchemy import Column, Index, MetaData, String, Table, func, text

    metadata = MetaData()
    table = Table(
        "functional_people",
        metadata,
        Column("name", String(128)),
    )
    declared = Index(
        "ix_normalized_name",
        func.normalized_name(table.c.name).desc(),
        unique=True,
        informix_functional=True,
    )
    reflected = Index(
        "ix_normalized_name",
        text("NORMALIZED_NAME ( name ) DESC"),
        unique=True,
        _table=table,
        informix_procedure="normalized_name",
        informix_access_method="btree",
        informix_opclass="btree_ops",
    )
    return declared, reflected


def test_alembic_compares_equivalent_functional_indexes_without_warning(
    caplog,
) -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext

    declared, reflected = _functional_indexes_for_comparison()
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_equal
    assert "approximate signature" not in caplog.text


def test_alembic_detects_changed_functional_index_expression() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Index, text

    declared, reflected = _functional_indexes_for_comparison()
    changed = Index(
        reflected.name,
        text("different_normalizer(name) DESC"),
        unique=True,
        _table=reflected.table,
        informix_procedure="different_normalizer",
    )
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, changed)

    assert result.is_different
    assert "expression" in result.message
