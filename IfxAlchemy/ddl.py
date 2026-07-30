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

from typing import Any

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

