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
"""Informix-specific SQLAlchemy DML constructs.

The first supported MERGE surface is intentionally small and explicit.  It
supports a target table or updatable view, a table-like source (including an
aliased SELECT or named ``Values`` construct), one ON condition, and these
native Informix action combinations:

* matched UPDATE;
* matched DELETE;
* not-matched INSERT;
* UPDATE + INSERT;
* DELETE + INSERT.

All values are represented as SQLAlchemy expressions or bound parameters.
The construct never accepts raw SQL fragments for identifiers or user values.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import exc
from sqlalchemy import sql
from sqlalchemy.sql import coercions, roles, selectable
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.util import ClauseAdapter


class InformixMerge(Executable, ClauseElement):
    """Represent one native Informix ``MERGE`` statement.

    Construct instances through :func:`merge` and add one or two actions with
    the generative methods.  UPDATE and DELETE are mutually exclusive because
    Informix permits at most one ``WHEN MATCHED`` action.

    A plain ``Select`` source is converted to a named subquery.  References to
    its selected columns in the ON clause and action values are adapted to the
    generated subquery automatically.  Callers can also pass an explicitly
    named ``Select.subquery()`` when they need control over the alias.
    """

    __visit_name__ = "informix_merge"
    inherit_cache = False
    _execution_options = Executable._execution_options.union(
        {"preserve_rowcount": True}
    )

    def __init__(self, target: Any, source: Any, onclause: Any) -> None:
        self.target = coercions.expect(
            roles.DMLTableRole,
            target,
            argname="target",
        )
        target_base = (
            self.target.element
            if isinstance(self.target, selectable.Alias)
            else self.target
        )
        if not isinstance(target_base, selectable.TableClause):
            raise exc.ArgumentError(
                "Informix MERGE target must be a table, view, synonym, "
                "or an alias of one; joins and derived tables are not valid "
                "targets"
            )

        self._source_adapter: ClauseAdapter | None = None
        if isinstance(source, (sql.elements.TextClause, selectable.TextualSelect)):
            raise exc.ArgumentError(
                "Informix MERGE source must be a structured table, SELECT, "
                "or collection-derived FromClause; textual SQL is not accepted"
            )
        if isinstance(source, selectable.CTE):
            raise exc.ArgumentError(
                "InformixMerge does not yet support a CTE as its direct "
                "source; pass the underlying SELECT or a named subquery"
            )
        if isinstance(source, selectable.Join):
            raise exc.ArgumentError(
                "Informix MERGE join sources must be wrapped in a SELECT "
                "subquery"
            )
        if isinstance(source, selectable.SelectBase):
            source_alias_name = "merge_source"
            target_alias_name = self._declared_alias_name(self.target)
            if self._same_identifier(target_alias_name, source_alias_name):
                source_alias_name = "merge_source_source"
            source_from = source.subquery(source_alias_name)
            self._source_adapter = ClauseAdapter(source_from)
            source = source_from
        else:
            source = coercions.expect(
                roles.FromClauseRole,
                source,
                argname="source",
            )

        self.source = source

        target_alias_name = self._declared_alias_name(self.target)
        source_alias_name = self._declared_alias_name(self.source)
        if (
            target_alias_name is not None
            and source_alias_name is not None
            and self._same_identifier(target_alias_name, source_alias_name)
        ):
            raise exc.ArgumentError(
                "Informix MERGE source and target aliases must be different"
            )

        self.onclause = self._adapt_source_expression(
            coercions.expect(
                roles.OnClauseRole,
                onclause,
                argname="onclause",
            )
        )

        self._matched_update: tuple[tuple[Any, ClauseElement], ...] | None = None
        self._matched_delete = False
        self._not_matched_insert: tuple[
            tuple[Any, ClauseElement], ...
        ] | None = None

    @staticmethod
    def _declared_alias_name(from_clause: Any):
        if isinstance(from_clause, selectable.AliasedReturnsRows):
            return from_clause.name
        if (
            isinstance(from_clause, selectable.Values)
            and from_clause.named_with_column
        ):
            return from_clause.name
        return None

    @staticmethod
    def _same_identifier(left: Any, right: Any) -> bool:
        if left is None or right is None:
            return False
        if getattr(left, "quote", None) is True or getattr(
            right, "quote", None
        ) is True:
            return str(left) == str(right)
        return str(left).casefold() == str(right).casefold()

    def _adapt_source_expression(self, expression: ClauseElement) -> ClauseElement:
        if self._source_adapter is None:
            return expression
        return self._source_adapter.traverse(expression)

    def _target_column(self, key: Any):
        if isinstance(key, str):
            column = self.target.c.get(key)
            if column is None:
                raise exc.ArgumentError(
                    f"MERGE target has no column named {key!r}"
                )
            return column

        corresponding_column = getattr(
            self.target,
            "corresponding_column",
            None,
        )
        if corresponding_column is not None:
            column = corresponding_column(key, require_embedded=False)
            if column is not None:
                return column

        if any(key is candidate for candidate in self.target.c):
            return key

        raise exc.ArgumentError(
            "MERGE assignment keys must be target column names or columns "
            "that correspond to the target"
        )

    def _coerce_action_values(
        self,
        values: Mapping[Any, Any],
        *,
        action_name: str,
    ) -> tuple[tuple[Any, ClauseElement], ...]:
        if not isinstance(values, Mapping):
            raise exc.ArgumentError(
                f"{action_name} values must be a mapping"
            )
        if not values:
            raise exc.ArgumentError(
                f"{action_name} requires at least one target column"
            )

        assignments: list[tuple[Any, ClauseElement]] = []
        seen_columns: set[Any] = set()

        for key, value in values.items():
            column = self._target_column(key)
            if column in seen_columns:
                raise exc.ArgumentError(
                    f"{action_name} assigns target column {column.key!r} "
                    "more than once"
                )
            seen_columns.add(column)

            if isinstance(value, ClauseElement):
                expression = coercions.expect(
                    roles.ExpressionElementRole,
                    value,
                    argname=f"value for {column.key}",
                )
            else:
                expression = sql.bindparam(
                    None,
                    value,
                    type_=column.type,
                    unique=True,
                )

            assignments.append(
                (
                    column,
                    self._adapt_source_expression(expression),
                )
            )

        return tuple(assignments)

    def when_matched_update(
        self,
        *,
        values: Mapping[Any, Any],
    ) -> "InformixMerge":
        """Add ``WHEN MATCHED THEN UPDATE SET ...``."""

        if self._matched_delete or self._matched_update is not None:
            raise exc.ArgumentError(
                "Informix MERGE accepts only one WHEN MATCHED action; "
                "UPDATE and DELETE are mutually exclusive"
            )

        cloned = self._clone()
        cloned._matched_update = self._coerce_action_values(
            values,
            action_name="when_matched_update",
        )
        return cloned

    def when_matched_delete(self) -> "InformixMerge":
        """Add ``WHEN MATCHED THEN DELETE``."""

        if self._matched_delete or self._matched_update is not None:
            raise exc.ArgumentError(
                "Informix MERGE accepts only one WHEN MATCHED action; "
                "UPDATE and DELETE are mutually exclusive"
            )

        cloned = self._clone()
        cloned._matched_delete = True
        return cloned

    def when_not_matched_insert(
        self,
        *,
        values: Mapping[Any, Any],
    ) -> "InformixMerge":
        """Add ``WHEN NOT MATCHED THEN INSERT (...) VALUES (...)``."""

        if self._not_matched_insert is not None:
            raise exc.ArgumentError(
                "Informix MERGE accepts only one WHEN NOT MATCHED action"
            )

        cloned = self._clone()
        cloned._not_matched_insert = self._coerce_action_values(
            values,
            action_name="when_not_matched_insert",
        )
        return cloned


def merge(target: Any, source: Any, onclause: Any) -> InformixMerge:
    """Create an :class:`InformixMerge` statement.

    Example::

        source = incoming.alias("s")
        target = customers.alias("t")

        stmt = (
            merge(target, source, target.c.id == source.c.id)
            .when_matched_update(values={target.c.name: source.c.name})
            .when_not_matched_insert(
                values={
                    target.c.id: source.c.id,
                    target.c.name: source.c.name,
                }
            )
        )
    """

    return InformixMerge(target, source, onclause)
