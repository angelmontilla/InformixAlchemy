# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Angel Montilla
"""Alembic integration for the Informix SQLAlchemy dialect.

Alembic chooses its database-specific DDL implementation from the dialect
name. Defining a :class:`~alembic.ddl.impl.DefaultImpl` subclass with
``__dialect__ = "informix"`` registers the backend and allows
``MigrationContext`` and autogenerate to work with connections produced by
this package.

The module is imported conditionally from :mod:`IfxAlchemy` so Alembic remains
an optional development/runtime dependency. Importing the SQLAlchemy dialect
therefore continues to work when Alembic is not installed.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from alembic.ddl.impl import ComparisonResult, DefaultImpl
from sqlalchemy import exc as sa_exc


class InformixImpl(DefaultImpl):
    """Alembic DDL implementation for IBM Informix 14.10 and newer.

    Informix DDL is not declared transactional here. This matches Alembic's
    conservative default and avoids promising rollback semantics that vary by
    statement and server configuration.

    Informix functional indexes are reflected from ``SYSINDICES`` as SQL text
    because SQLAlchemy cannot reconstruct an arbitrary ``FunctionElement``
    from catalog metadata. Alembic's generic implementation warns for such
    expression indexes and compares only an approximate column signature.
    ``compare_indexes()`` therefore performs a dialect-specific comparison of
    the normalized rendered expressions.
    """

    __dialect__ = "informix"
    transactional_ddl = False

    @staticmethod
    def _informix_index_options(index: Any) -> dict[str, Any]:
        """Return a plain dictionary of Informix index options."""
        try:
            return dict(index.dialect_options["informix"])
        except (AttributeError, KeyError, TypeError):
            return {}

    @classmethod
    def _is_functional_index(cls, index: Any) -> bool:
        """Identify declared and reflected Informix functional indexes."""
        options = cls._informix_index_options(index)
        return bool(options.get("functional") or options.get("procedure"))

    @staticmethod
    def _normalize_expression_sql(value: str) -> str:
        """Canonicalize SQL without changing quoted identifier contents.

        Informix stores expression text independently from SQLAlchemy's
        expression tree. The comparison is case-insensitive for unquoted SQL
        and removes insignificant whitespace around punctuation. Text inside
        quoted identifiers and literals is preserved byte-for-byte.
        """
        text = str(value).strip()
        if not text:
            return ""

        normalized: list[str] = []
        quote: str | None = None
        pending_space = False
        suppress_space_after = False
        index = 0

        while index < len(text):
            character = text[index]

            if quote is not None:
                normalized.append(character)
                if character == quote:
                    # SQL escapes quotes by doubling them. Preserve both and
                    # remain inside the quoted token.
                    if index + 1 < len(text) and text[index + 1] == quote:
                        normalized.append(text[index + 1])
                        index += 2
                        continue
                    quote = None
                index += 1
                continue

            if character in {'"', "'"}:
                if pending_space and normalized and not suppress_space_after:
                    normalized.append(" ")
                pending_space = False
                suppress_space_after = False
                quote = character
                normalized.append(character)
                index += 1
                continue

            if character.isspace():
                pending_space = True
                index += 1
                continue

            if character in "(,.":
                while normalized and normalized[-1] == " ":
                    normalized.pop()
                normalized.append(character)
                pending_space = False
                suppress_space_after = True
                index += 1
                continue

            if character == ")":
                while normalized and normalized[-1] == " ":
                    normalized.pop()
                normalized.append(character)
                pending_space = False
                suppress_space_after = False
                index += 1
                continue

            if pending_space and normalized and not suppress_space_after:
                normalized.append(" ")
            pending_space = False
            suppress_space_after = False
            normalized.append(character.casefold())
            index += 1

        return "".join(normalized).strip()

    def _render_index_expressions(self, index: Any) -> tuple[str, ...] | None:
        """Render index expressions using Informix's compiler.

        ``include_table=False`` is essential: a declared function expression
        normally renders ``function(table.column)`` while reflection stores
        ``function(column)``. Informix treats both as the same index key.
        """
        rendered: list[str] = []

        try:
            expressions: Iterable[Any] = index.expressions
            for expression in expressions:
                compiled = expression.compile(
                    dialect=self.dialect,
                    compile_kwargs={
                        "include_table": False,
                        "literal_binds": True,
                    },
                )
                rendered.append(
                    self._normalize_expression_sql(str(compiled))
                )
        except (AttributeError, TypeError, ValueError, sa_exc.CompileError):
            return None

        return tuple(rendered)

    @staticmethod
    def _normalized_option(value: Any) -> tuple[str, ...] | None:
        """Normalize optional catalog identifiers for semantic comparison."""
        if value is None:
            return None

        values = value if isinstance(value, (tuple, list)) else (value,)
        normalized = tuple(str(item).strip().casefold() for item in values)
        return normalized or None

    def compare_indexes(
        self,
        metadata_index: Any,
        reflected_index: Any,
    ) -> ComparisonResult:
        """Compare Informix functional indexes without approximate signatures.

        Ordinary indexes continue through Alembic's generic implementation.
        Functional indexes compare uniqueness, rendered key expressions and
        any access-method/operator-class option explicitly present in target
        metadata. If an expression cannot be rendered safely, comparison is
        skipped rather than generating a destructive drop/create pair.
        """
        metadata_functional = self._is_functional_index(metadata_index)
        reflected_functional = self._is_functional_index(reflected_index)

        if not metadata_functional and not reflected_functional:
            return super().compare_indexes(metadata_index, reflected_index)

        if metadata_functional != reflected_functional:
            return ComparisonResult.Different(
                "one index is functional and the other is not"
            )

        differences: list[str] = []
        unique_difference = self._compare_index_unique(
            metadata_index,
            reflected_index,
        )
        if unique_difference:
            differences.append(unique_difference)

        metadata_expressions = self._render_index_expressions(metadata_index)
        reflected_expressions = self._render_index_expressions(reflected_index)

        if metadata_expressions is None or reflected_expressions is None:
            return ComparisonResult.Skip(
                "Informix functional-index expressions could not be rendered"
            )

        if metadata_expressions != reflected_expressions:
            differences.append(
                "expression "
                f"{reflected_expressions!r} to {metadata_expressions!r}"
            )

        metadata_options = self._informix_index_options(metadata_index)
        reflected_options = self._informix_index_options(reflected_index)
        for option_name in ("access_method", "opclass"):
            metadata_value = self._normalized_option(
                metadata_options.get(option_name)
            )
            if metadata_value is None:
                # Informix supplies defaults such as btree during reflection;
                # an unspecified target option must not cause false diffs.
                continue

            reflected_value = self._normalized_option(
                reflected_options.get(option_name)
            )
            if metadata_value != reflected_value:
                differences.append(
                    f"Informix {option_name} "
                    f"{reflected_value!r} to {metadata_value!r}"
                )

        if differences:
            return ComparisonResult.Different(differences)
        return ComparisonResult.Equal()


__all__ = ("InformixImpl",)
