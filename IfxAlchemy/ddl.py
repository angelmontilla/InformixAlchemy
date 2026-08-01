# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Angel Montilla
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Informix-specific executable DDL constructs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import exc
from sqlalchemy import schema as sa_schema
from sqlalchemy.sql.ddl import ExecutableDDLElement


class SetTableLockMode(ExecutableDDLElement):
    """Set the native Informix lock mode of an existing table.

    Informix supports only ``PAGE`` and ``ROW`` in the writable
    ``ALTER TABLE ... LOCK MODE (...)`` syntax.  The catalog-only
    ``PAGE_AND_ROW`` state returned by reflection is therefore intentionally
    not accepted by this construct.

    Example::

        connection.execute(SetTableLockMode(customers, "ROW"))
    """

    __visit_name__ = "set_table_lock_mode"

    def __init__(self, table: sa_schema.Table, lock_mode: Any) -> None:
        if not isinstance(table, sa_schema.Table):
            raise exc.ArgumentError(
                "SetTableLockMode table must be a sqlalchemy.schema.Table"
            )

        self.table = table
        self.element = table
        self.lock_mode = lock_mode


class ModifyTableExtents(ExecutableDDLElement):
    """Modify native Informix extent sizes for an existing table.

    At least one of ``first_extent`` or ``next_extent`` must be supplied.
    Values are compiled only after strict positive-integer validation, so the
    construct cannot be used as an arbitrary SQL escape hatch.

    Informix supports these native forms::

        ALTER TABLE table_name MODIFY EXTENT SIZE 128
        ALTER TABLE table_name MODIFY NEXT SIZE 64
        ALTER TABLE table_name MODIFY EXTENT SIZE 128 NEXT SIZE 64

    Extent sizes are expressed in kilobytes.

    Example::

        connection.execute(
            ModifyTableExtents(
                movements,
                first_extent=128,
                next_extent=64,
            )
        )
    """

    __visit_name__ = "modify_table_extents"

    def __init__(
        self,
        table: sa_schema.Table,
        *,
        first_extent: Any = None,
        next_extent: Any = None,
    ) -> None:
        if not isinstance(table, sa_schema.Table):
            raise exc.ArgumentError(
                "ModifyTableExtents table must be a "
                "sqlalchemy.schema.Table"
            )

        if first_extent is None and next_extent is None:
            raise exc.ArgumentError(
                "ModifyTableExtents requires first_extent and/or "
                "next_extent"
            )

        self.table = table
        self.element = table
        self.first_extent = first_extent
        self.next_extent = next_extent



_IDENTIFIER_MAX_LENGTH = 128
_OWNER_MAX_LENGTH = 32
_TARGET_KINDS = frozenset({"table", "view", "sequence"})


def _validate_identifier_part(value: Any, field_name: str, max_length: int):
    """Validate one Informix identifier component without accepting raw SQL.

    Qualification characters are rejected in ordinary strings.  Applications
    that need a literal dot, colon, or at-sign inside a quoted identifier can
    use :class:`sqlalchemy.sql.elements.quoted_name` with ``quote=True``.
    """
    if not isinstance(value, str):
        raise exc.ArgumentError(f"{field_name} must be a string identifier")

    if not value:
        raise exc.ArgumentError(f"{field_name} must not be empty")

    if len(value) > max_length:
        raise exc.ArgumentError(
            f"{field_name} exceeds the Informix limit of {max_length} characters"
        )

    if any(ord(char) < 32 for char in value):
        raise exc.ArgumentError(f"{field_name} contains control characters")

    if getattr(value, "quote", None) is not True and any(
        token in value for token in (".", ":", "@")
    ):
        raise exc.ArgumentError(
            f"{field_name} must be supplied as a structured identifier; "
            "do not embed '.', ':', or '@' qualifiers in a plain string"
        )

    return value


@dataclass(frozen=True)
class SynonymName:
    """Structured name of a synonym in the current Informix database."""

    name: str
    owner: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_identifier_part(self.name, "synonym name", _IDENTIFIER_MAX_LENGTH)
        if self.owner is not None:
            _validate_identifier_part(self.owner, "synonym owner", _OWNER_MAX_LENGTH)


@dataclass(frozen=True)
class SynonymTarget:
    """Structured local or remote target of an Informix synonym.

    ``database`` and ``server`` model the native Informix form
    ``database@server:owner.object``.  A server therefore requires a database.
    Remote sequence targets are rejected because Informix supports sequence
    synonyms only inside the current database.
    """

    name: str
    owner: Optional[str] = None
    database: Optional[str] = None
    server: Optional[str] = None
    kind: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_identifier_part(self.name, "target name", _IDENTIFIER_MAX_LENGTH)
        if self.owner is not None:
            _validate_identifier_part(self.owner, "target owner", _OWNER_MAX_LENGTH)
        if self.database is not None:
            _validate_identifier_part(
                self.database,
                "target database",
                _IDENTIFIER_MAX_LENGTH,
            )
        if self.server is not None:
            _validate_identifier_part(
                self.server,
                "target server",
                _IDENTIFIER_MAX_LENGTH,
            )
        if self.server is not None and self.database is None:
            raise exc.ArgumentError(
                "target server requires an explicit target database"
            )

        if self.kind is not None and not isinstance(self.kind, str):
            raise exc.ArgumentError(
                "target kind must be 'table', 'view', 'sequence', or None"
            )
        normalized_kind = self.kind.lower() if isinstance(self.kind, str) else None
        if normalized_kind is not None and normalized_kind not in _TARGET_KINDS:
            raise exc.ArgumentError(
                "target kind must be 'table', 'view', 'sequence', or None"
            )
        if normalized_kind == "sequence" and (
            self.database is not None or self.server is not None
        ):
            raise exc.ArgumentError(
                "Informix does not support synonyms for sequence objects "
                "outside the current database"
            )
        object.__setattr__(self, "kind", normalized_kind)


def _coerce_synonym_name(value: Any) -> SynonymName:
    if isinstance(value, SynonymName):
        return value
    if isinstance(value, str):
        return SynonymName(value)
    raise exc.ArgumentError(
        "synonym name must be a string or IfxAlchemy.SynonymName"
    )


def _coerce_synonym_target(value: Any) -> SynonymTarget:
    if isinstance(value, SynonymTarget):
        return value
    if isinstance(value, sa_schema.Sequence):
        return SynonymTarget(
            value.name,
            owner=value.schema,
            kind="sequence",
        )
    if isinstance(value, sa_schema.Table):
        return SynonymTarget(
            value.name,
            owner=value.schema,
            kind="table",
        )
    if isinstance(value, str):
        return SynonymTarget(value)
    raise exc.ArgumentError(
        "synonym target must be a string, Table, Sequence, or "
        "IfxAlchemy.SynonymTarget"
    )


def _validate_optional_bool(value: Any, field_name: str) -> Optional[bool]:
    if value is not None and not isinstance(value, bool):
        raise exc.ArgumentError(f"{field_name} must be True, False, or None")
    return value


class CreateSynonym(ExecutableDDLElement):
    """Create a native Informix synonym.

    ``public`` has three states: ``True`` renders ``PUBLIC``, ``False``
    renders ``PRIVATE``, and ``None`` omits the modifier.  Informix interprets
    the omitted modifier as PUBLIC in a non-ANSI database and as PRIVATE in a
    MODE ANSI database.  ANSI databases reject both explicit modifiers.
    """

    __visit_name__ = "create_synonym"

    def __init__(
        self,
        name: Any,
        target: Any,
        *,
        public: Optional[bool] = None,
        if_not_exists: bool = False,
    ) -> None:
        self.name = _coerce_synonym_name(name)
        self.target = _coerce_synonym_target(target)
        self.public = _validate_optional_bool(public, "public")
        if not isinstance(if_not_exists, bool):
            raise exc.ArgumentError("if_not_exists must be a boolean")
        self.if_not_exists = if_not_exists
        self.element = self.name


class DropSynonym(ExecutableDDLElement):
    """Drop a native Informix synonym from the current database.

    Informix DROP SYNONYM has no PUBLIC/PRIVATE keyword.  ``public`` is kept
    as an optional semantic hint for callers and validation, but does not alter
    the emitted SQL; ownership is expressed structurally by ``SynonymName``.
    """

    __visit_name__ = "drop_synonym"

    def __init__(
        self,
        name: Any,
        *,
        public: Optional[bool] = None,
        if_exists: bool = False,
    ) -> None:
        self.name = _coerce_synonym_name(name)
        self.target = None
        self.public = _validate_optional_bool(public, "public")
        if not isinstance(if_exists, bool):
            raise exc.ArgumentError("if_exists must be a boolean")
        self.if_exists = if_exists
        self.element = self.name
