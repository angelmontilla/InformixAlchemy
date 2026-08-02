# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Angel Montilla
"""Typed advanced-index DDL for the Informix SQLAlchemy dialect.

The objects in this module deliberately accept identifiers and structured
SQLAlchemy objects only.  They never accept arbitrary SQL fragments.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import exc
from sqlalchemy import schema as sa_schema
from sqlalchemy.sql.ddl import ExecutableDDLElement

from .ddl import _validate_identifier_part

_IDENTIFIER_MAX_LENGTH = 128
_INDEX_MODES = {
    "ENABLED",
    "DISABLED",
    "FILTERING",
    "FILTERING WITH ERROR",
    "FILTERING WITHOUT ERROR",
}

class _ReflectedAccessMethodParameters(str):
    """Trusted access-method parameter text read from ``SYSINDICES``.

    Public declarations must use a mapping so every identifier and literal can
    be validated.  Reflection needs a lossless marker because Informix stores
    the access-method parameter list as catalog text rather than structured
    rows.  Only the reflector creates this private marker.
    """

    _informix_reflected_amparam = True



def _identifier(value: Any, field_name: str) -> str:
    return _validate_identifier_part(value, field_name, _IDENTIFIER_MAX_LENGTH)


def _index_name(index_or_name: Any) -> tuple[str, str | None]:
    if isinstance(index_or_name, sa_schema.Index):
        if index_or_name.name is None:
            raise exc.ArgumentError("Informix index-state DDL requires a named Index")
        schema = getattr(getattr(index_or_name, "table", None), "schema", None)
        return _identifier(index_or_name.name, "index name"), schema
    return _identifier(index_or_name, "index name"), None


@dataclass(frozen=True)
class SetIndexMode(ExecutableDDLElement):
    """Change an index mode through native ``SET INDEXES`` DDL."""

    __visit_name__ = "set_index_mode"
    index: Any
    mode: str

    inherit_cache = True

    def __post_init__(self) -> None:
        _index_name(self.index)
        if not isinstance(self.mode, str):
            raise exc.ArgumentError("index mode must be a string")
        normalized = " ".join(self.mode.strip().upper().split())
        if normalized not in _INDEX_MODES:
            raise exc.ArgumentError(
                "index mode must be ENABLED, DISABLED, FILTERING, "
                "FILTERING WITH ERROR, or FILTERING WITHOUT ERROR"
            )
        object.__setattr__(self, "mode", normalized)


class EnableIndex(SetIndexMode):
    __visit_name__ = "set_index_mode"

    def __init__(self, index: Any):
        super().__init__(index=index, mode="ENABLED")


class DisableIndex(SetIndexMode):
    __visit_name__ = "set_index_mode"

    def __init__(self, index: Any):
        super().__init__(index=index, mode="DISABLED")


@dataclass(frozen=True)
class SetIndexVisibility(ExecutableDDLElement):
    """Make a user-created index visible or invisible to the optimizer."""

    __visit_name__ = "set_index_visibility"
    index: Any
    visible: bool

    inherit_cache = True

    def __post_init__(self) -> None:
        _index_name(self.index)
        if not isinstance(self.visible, bool):
            raise exc.ArgumentError("visible must be a boolean")


@dataclass(frozen=True)
class AlterIndexCluster(ExecutableDDLElement):
    """Set or clear the clustering attribute of an existing index."""

    __visit_name__ = "alter_index_cluster"
    index: Any
    clustered: bool = True

    inherit_cache = True

    def __post_init__(self) -> None:
        _index_name(self.index)
        if not isinstance(self.clustered, bool):
            raise exc.ArgumentError("clustered must be a boolean")


__all__ = (
    "AlterIndexCluster",
    "DisableIndex",
    "EnableIndex",
    "SetIndexMode",
    "SetIndexVisibility",
)
