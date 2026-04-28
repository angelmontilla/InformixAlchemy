from __future__ import annotations

from importlib.metadata import metadata, requires


def test_distribution_declares_sqlalchemy_2045_to_20x_range():
    reqs = requires("IfxAlchemy") or []

    assert any(
        req.startswith("SQLAlchemy")
        and ">=2.0.45" in req
        and "<2.2" in req
        for req in reqs
    )


def test_distribution_name_is_ifxalchemy():
    meta = metadata("IfxAlchemy")

    assert meta["Name"] == "IfxAlchemy"
