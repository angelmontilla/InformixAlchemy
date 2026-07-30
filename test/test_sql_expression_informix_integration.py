from __future__ import annotations

import pytest
from sqlalchemy import (
    Boolean,
    bindparam,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    except_,
    intersect,
    literal,
    literal_column,
    select,
    union,
)


pytestmark = pytest.mark.requires_informix


@pytest.fixture
def expression_tables(conn, name_factory):
    metadata = MetaData()
    source = Table(
        name_factory("sa_expr_src_"),
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(20), nullable=False),
        Column("parent_id", Integer),
        Column("enabled", Boolean),
    )
    target = Table(
        name_factory("sa_expr_dst_"),
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(20), nullable=False),
        Column("parent_id", Integer),
    )

    metadata.create_all(conn)
    conn.execute(
        source.insert(),
        [
            {"id": 1, "data": "d1", "parent_id": None, "enabled": True},
            {"id": 2, "data": "d2", "parent_id": 1, "enabled": False},
            {"id": 3, "data": "d3", "parent_id": 1, "enabled": True},
            {"id": 4, "data": "d4", "parent_id": 3, "enabled": False},
            {"id": 5, "data": "d5", "parent_id": 3, "enabled": True},
        ],
    )
    conn.execute(
        target.insert(),
        [
            {"id": 2, "data": "d2", "parent_id": 1},
            {"id": 4, "data": "d4", "parent_id": 3},
            {"id": 6, "data": "d6", "parent_id": 5},
        ],
    )
    conn.commit()

    try:
        yield source, target
    finally:
        conn.rollback()
        metadata.drop_all(conn, checkfirst=True)
        conn.commit()


def test_intersect_and_except_round_trip(conn, expression_tables):
    source, target = expression_tables

    common = conn.execute(
        intersect(select(source.c.id), select(target.c.id)).order_by("id")
    ).scalars().all()
    source_only = conn.execute(
        except_(select(source.c.id), select(target.c.id)).order_by("id")
    ).scalars().all()

    assert common == [2, 4]
    assert source_only == [1, 3, 5]


def test_nonrecursive_and_recursive_ctes_round_trip(conn, expression_tables):
    source, _ = expression_tables

    filtered = select(source).where(source.c.id >= 3).cte("filtered_rows")
    assert conn.execute(
        select(filtered.c.id).order_by(filtered.c.id)
    ).scalars().all() == [3, 4, 5]

    hierarchy = select(source.c.id, source.c.parent_id).where(
        source.c.id == 1
    ).cte("hierarchy", recursive=True)
    child = source.alias("child")
    hierarchy = hierarchy.union_all(
        select(child.c.id, child.c.parent_id).where(
            child.c.parent_id == hierarchy.c.id
        )
    )

    assert conn.execute(
        select(hierarchy.c.id).order_by(hierarchy.c.id)
    ).scalars().all() == [1, 2, 3, 4, 5]


def test_cte_update_and_delete_round_trip(conn, expression_tables):
    """Exercise the same ordinary binds used by SQLAlchemy's CTETest."""
    source, target = expression_tables
    affected = (
        select(source)
        .where(source.c.data.in_(["d2", "d4"]))
        .cte("affected_rows")
    )

    conn.execute(
        target.update()
        .values(parent_id=99)
        .where(target.c.data == affected.c.data)
    )

    assert conn.execute(
        select(target.c.id, target.c.parent_id).order_by(target.c.id)
    ).all() == [(2, 99), (4, 99), (6, 5)]

    deleted = (
        select(source)
        .where(source.c.data.in_(["d4"]))
        .cte("deleted_rows")
    )

    conn.execute(
        target.delete().where(target.c.data == deleted.c.data)
    )

    assert conn.execute(
        select(target.c.id).order_by(target.c.id)
    ).scalars().all() == [2, 6]



def test_cte_insert_from_select_round_trip(conn, expression_tables):
    source, target = expression_tables
    conn.execute(target.delete())

    selected = select(source).where(source.c.id.in_([2, 3, 4])).cte(
        "selected_rows"
    )
    conn.execute(
        target.insert().from_select(
            ["id", "data", "parent_id"],
            select(selected.c.id, selected.c.data, selected.c.parent_id),
        )
    )

    assert conn.execute(
        select(target).order_by(target.c.id)
    ).all() == [
        (2, "d2", 1),
        (3, "d3", 1),
        (4, "d4", 3),
    ]


def test_cte_delete_scalar_subquery_round_trip(conn, expression_tables):
    source, target = expression_tables
    selected = select(source).where(source.c.id.in_([2, 4])).cte(
        "selected_rows"
    )

    conn.execute(
        target.delete().where(
            target.c.data
            == select(selected.c.data)
            .where(selected.c.id == target.c.id)
            .scalar_subquery()
        )
    )

    assert conn.execute(
        select(target.c.id).order_by(target.c.id)
    ).scalars().all() == [6]

def test_boolean_projection_round_trip(conn, expression_tables):
    source, _ = expression_tables

    rows = conn.execute(
        select(source.c.id, (source.c.id >= 3).label("is_high")).order_by(
            source.c.id
        )
    ).all()

    assert rows == [
        (1, False),
        (2, False),
        (3, True),
        (4, True),
        (5, True),
    ]


def test_ordered_limited_union_branches_round_trip(conn, expression_tables):
    source, _ = expression_tables
    first = (
        select(source.c.id)
        .where(source.c.id <= 2)
        .order_by(source.c.id.desc())
        .limit(1)
    )
    second = (
        select(source.c.id)
        .where(source.c.id >= 4)
        .order_by(source.c.id)
        .limit(1)
    )

    assert conn.execute(
        union(first, second).order_by("id")
    ).scalars().all() == [2, 4]

    ordered_first = (
        select(source.c.id)
        .where(source.c.id == 2)
        .order_by(source.c.id)
    )
    ordered_second = (
        select(source.c.id)
        .where(source.c.id == 4)
        .order_by(source.c.id)
    )
    assert conn.execute(
        union(ordered_first, ordered_second).order_by("id")
    ).scalars().all() == [2, 4]


def test_order_by_projected_label_expression_round_trip(
    conn,
    expression_tables,
):
    source, _ = expression_tables
    label = source.c.data.label("foo")

    rows = conn.execute(
        select(label).order_by(label + literal("_suffix"))
    ).scalars().all()

    assert rows == ["d1", "d2", "d3", "d4", "d5"]


def test_fetch_variants_round_trip(conn, expression_tables):
    source, _ = expression_tables

    assert conn.execute(
        select(source.c.id).order_by(source.c.id).fetch(2)
    ).scalars().all() == [1, 2]

    assert set(
        conn.execute(select(source.c.id).fetch(10)).scalars().all()
    ) == {1, 2, 3, 4, 5}

    two = literal_column("1") + literal_column("1")
    assert conn.execute(
        select(source.c.id)
        .order_by(source.c.id)
        .fetch(two)
        .offset(two)
    ).scalars().all() == [3, 4]


def test_native_skip_first_variants_round_trip(conn, expression_tables):
    source, _ = expression_tables

    assert conn.execute(
        select(source.c.id).order_by(source.c.id).offset(2)
    ).scalars().all() == [3, 4, 5]

    assert conn.execute(
        select(source.c.id).order_by(source.c.id).limit(2).offset(2)
    ).scalars().all() == [3, 4]

    bounded = (
        select(source.c.id)
        .order_by(source.c.id)
        .limit(bindparam("limit_count"))
        .offset(bindparam("offset_count"))
    )
    assert conn.execute(
        bounded,
        {"limit_count": 2, "offset_count": 1},
    ).scalars().all() == [2, 3]

    combined = union(
        select(source.c.id).where(source.c.id <= 3),
        select(source.c.id).where(source.c.id >= 3),
    ).order_by("id").limit(2).offset(1)
    assert conn.execute(combined).scalars().all() == [2, 3]
