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


def _ordinary_advanced_indexes_for_comparison():
    from sqlalchemy import Column, Index, Integer, MetaData, String, Table

    from IfxAlchemy.fragmentation import _ReflectedFragmentExpression

    metadata = MetaData()
    table = Table(
        "orders",
        metadata,
        Column("id", Integer),
        Column("status", String(20)),
    )
    declared = Index(
        "ix_open_orders",
        table.c.id,
        informix_where=table.c.status == "OPEN",
        informix_dbspace="idxspace",
        informix_compressed=True,
        informix_visible=False,
        informix_mode="DISABLED",
    )
    reflected = Index(
        "ix_open_orders",
        table.c.id,
        _table=table,
        informix_where=_ReflectedFragmentExpression("status = 'OPEN'"),
        informix_dbspace="idxspace",
        informix_compressed=True,
        informix_visible=False,
        informix_mode="DISABLED",
    )
    return declared, reflected


def test_alembic_compares_persistent_advanced_index_options() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext

    declared, reflected = _ordinary_advanced_indexes_for_comparison()
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_equal


def test_alembic_detects_changed_advanced_index_option() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Index

    declared, reflected = _ordinary_advanced_indexes_for_comparison()
    changed = Index(
        reflected.name,
        *reflected.expressions,
        _table=reflected.table,
        informix_where=reflected.dialect_options["informix"]["where"],
        informix_dbspace="other_space",
        informix_compressed=True,
        informix_visible=False,
        informix_mode="DISABLED",
    )
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, changed)

    assert result.is_different
    assert "dbspace" in result.message


def test_alembic_compares_forest_of_trees_metadata() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Index, Integer, MetaData, Table

    metadata = MetaData()
    table = Table(
        "events",
        metadata,
        Column("tenant_id", Integer),
        Column("event_id", Integer),
    )
    declared = Index(
        "ix_events_fot",
        table.c.tenant_id,
        table.c.event_id,
        informix_hash_on=(table.c.tenant_id,),
        informix_buckets=32,
    )
    reflected = Index(
        "ix_events_fot",
        table.c.tenant_id,
        table.c.event_id,
        informix_hash_on=("tenant_id",),
        informix_buckets=16,
    )
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_different
    assert "buckets" in result.message


def test_alembic_ignores_nonpersistent_online_and_fillfactor() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Index, Integer, MetaData, Table

    metadata = MetaData()
    table = Table("events", metadata, Column("id", Integer))
    declared = Index(
        "ix_events",
        table.c.id,
        informix_online=True,
        informix_fillfactor=80,
    )
    reflected = Index("ix_events", table.c.id)
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_equal


def test_alembic_compares_structured_and_reflected_access_method_parameters() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Index, MetaData, String, Table

    from IfxAlchemy.indexes import _ReflectedAccessMethodParameters

    metadata = MetaData()
    table = Table("documents", metadata, Column("body", String(200)))
    declared = Index(
        "ix_documents_bts",
        table.c.body,
        informix_using="bts",
        informix_amparam={"delete": "deferred", "max_docs": 2500},
    )
    reflected = Index(
        "ix_documents_bts",
        table.c.body,
        informix_using="bts",
        informix_amparam=_ReflectedAccessMethodParameters(
            "delete='deferred', max_docs=2500"
        ),
    )
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_equal


def test_alembic_detects_changed_access_method_parameters() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Index, MetaData, String, Table

    from IfxAlchemy.indexes import _ReflectedAccessMethodParameters

    metadata = MetaData()
    table = Table("documents", metadata, Column("body", String(200)))
    declared = Index(
        "ix_documents_bts",
        table.c.body,
        informix_using="bts",
        informix_amparam={"max_docs": 2500},
    )
    reflected = Index(
        "ix_documents_bts",
        table.c.body,
        informix_using="bts",
        informix_amparam=_ReflectedAccessMethodParameters("max_docs=1000"),
    )
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_different
    assert "access-method parameters" in result.message


def test_alembic_ignores_effective_dbspace_when_metadata_uses_default() -> None:
    """A reflected physical dbspace is not an explicit metadata requirement."""
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Index, Integer, MetaData, Table

    metadata = MetaData()
    table = Table("events", metadata, Column("id", Integer))
    declared = Index("ix_events", table.c.id)
    reflected = Index(
        "ix_events",
        table.c.id,
        informix_dbspace="rootdbs",
    )
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_equal


def test_alembic_detects_explicit_dbspace_change() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Index, Integer, MetaData, Table

    metadata = MetaData()
    table = Table("events", metadata, Column("id", Integer))
    declared = Index(
        "ix_events",
        table.c.id,
        informix_dbspace="index_space",
    )
    reflected = Index(
        "ix_events",
        table.c.id,
        informix_dbspace="rootdbs",
    )
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_different
    assert "dbspace" in result.message


def test_alembic_normalizes_outer_parentheses_in_partial_predicates() -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Index, Integer, MetaData, String, Table

    from IfxAlchemy.fragmentation import _ReflectedFragmentExpression

    metadata = MetaData()
    table = Table(
        "orders",
        metadata,
        Column("id", Integer),
        Column("status", String(20)),
    )
    declared = Index(
        "ix_open_orders",
        table.c.id,
        informix_where=table.c.status == "OPEN",
        informix_dbspace="rootdbs",
    )
    reflected = Index(
        "ix_open_orders",
        table.c.id,
        informix_where=_ReflectedFragmentExpression(
            "(((status = 'OPEN')))"
        ),
        informix_dbspace="rootdbs",
    )
    context = MigrationContext.configure(dialect=IfxDialect())

    result = context.impl.compare_indexes(declared, reflected)

    assert result.is_equal


def test_alembic_native_advanced_index_set_has_no_false_differences() -> None:
    """Model the persistent metadata returned by the native integration test."""
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Index, Integer, MetaData, String, Table

    from IfxAlchemy.fragmentation import _ReflectedFragmentExpression

    metadata = MetaData()
    table = Table(
        "sa_idx_adv",
        metadata,
        Column("id", Integer),
        Column("tenant_id", Integer),
        Column("status", String(12)),
        Column("partial_key", Integer),
        Column("compressed_key", Integer),
        Column("online_key", Integer),
    )

    declared = (
        Index(
            "ix_partial",
            table.c.partial_key,
            informix_where=table.c.status == "OPEN",
            informix_dbspace="rootdbs",
            informix_fillfactor=80,
        ),
        Index(
            "ix_compressed",
            table.c.compressed_key,
            informix_dbspace="rootdbs",
            informix_compressed=True,
        ),
        Index(
            "ix_online",
            table.c.online_key,
            informix_online=True,
        ),
        Index(
            "ix_fot",
            table.c.tenant_id,
            table.c.id,
            informix_hash_on=(table.c.tenant_id,),
            informix_buckets=8,
        ),
        Index("ix_mode", table.c.status),
    )
    reflected = (
        Index(
            "ix_partial",
            table.c.partial_key,
            informix_where=_ReflectedFragmentExpression(
                "(status = 'OPEN')"
            ),
            informix_dbspace="rootdbs",
            informix_mode="ENABLED",
        ),
        Index(
            "ix_compressed",
            table.c.compressed_key,
            informix_dbspace="rootdbs",
            informix_compressed=True,
            informix_mode="ENABLED",
        ),
        Index(
            "ix_online",
            table.c.online_key,
            informix_dbspace="rootdbs",
            informix_mode="ENABLED",
        ),
        Index(
            "ix_fot",
            table.c.tenant_id,
            table.c.id,
            informix_dbspace="rootdbs",
            informix_hash_on=("tenant_id",),
            informix_buckets=8,
            informix_mode="ENABLED",
        ),
        Index(
            "ix_mode",
            table.c.status,
            informix_dbspace="rootdbs",
            informix_mode="ENABLED",
        ),
    )

    context = MigrationContext.configure(dialect=IfxDialect())

    results = [
        context.impl.compare_indexes(metadata_index, reflected_index)
        for metadata_index, reflected_index in zip(declared, reflected)
    ]

    assert all(result.is_equal for result in results), [
        result.message for result in results if not result.is_equal
    ]
