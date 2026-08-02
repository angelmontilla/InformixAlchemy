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
from sqlalchemy.sql import elements as sql_elements

from .fragmentation import _ReflectedFragmentExpression


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

        normalized_sql = "".join(normalized).strip()
        return InformixImpl._strip_redundant_outer_parentheses(normalized_sql)

    @staticmethod
    def _strip_redundant_outer_parentheses(value: str) -> str:
        """Remove parentheses that wrap the complete SQL expression.

        ``SYSFRAGMENTS`` may persist an expression predicate either as
        ``status = 'OPEN'`` or ``(status = 'OPEN')`` depending on the server
        release and the DDL form used to create the fragment.  Those forms are
        semantically identical and must not trigger an Alembic drop/create
        cycle.

        Parentheses are removed only when the first opening parenthesis is
        matched by the final character.  Quoted identifiers and string
        literals are tracked so parentheses inside them are ignored.
        """
        text = str(value).strip()

        while len(text) >= 2 and text[0] == "(" and text[-1] == ")":
            depth = 0
            quote: str | None = None
            wraps_complete_expression = True
            index = 0

            while index < len(text):
                character = text[index]

                if quote is not None:
                    if character == quote:
                        if index + 1 < len(text) and text[index + 1] == quote:
                            index += 2
                            continue
                        quote = None
                    index += 1
                    continue

                if character in {'"', "'"}:
                    quote = character
                elif character == "(":
                    depth += 1
                elif character == ")":
                    depth -= 1
                    if depth < 0:
                        wraps_complete_expression = False
                        break
                    if depth == 0 and index != len(text) - 1:
                        wraps_complete_expression = False
                        break

                index += 1

            if quote is not None or depth != 0 or not wraps_complete_expression:
                break

            text = text[1:-1].strip()

        return text

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

    @staticmethod
    def _first_option(options: dict[str, Any], *names: str) -> Any:
        for name in names:
            value = options.get(name)
            if value is not None:
                return value
        return None

    def _render_index_predicate(
        self,
        value: Any,
    ) -> str | None:
        if value is None:
            return None
        if isinstance(value, _ReflectedFragmentExpression):
            return self._normalize_expression_sql(value.sql)
        if isinstance(value, sql_elements.TextClause):
            return None
        try:
            compiled = value.compile(
                dialect=self.dialect,
                compile_kwargs={
                    "include_table": False,
                    "literal_binds": True,
                },
            )
        except (AttributeError, TypeError, ValueError, sa_exc.CompileError):
            return None
        return self._normalize_expression_sql(str(compiled))

    @staticmethod
    def _normalized_index_columns(value: Any) -> tuple[str, ...] | None:
        if value is None:
            return None
        values = value if isinstance(value, (tuple, list)) else (value,)
        normalized = []
        for item in values:
            name = getattr(item, "name", item)
            normalized.append(str(name).strip().casefold())
        return tuple(normalized) or None

    def _render_access_method_parameters(self, value: Any) -> str | None:
        if value is None:
            return None
        try:
            ddl_compiler = self.dialect.ddl_compiler(self.dialect, None)
            rendered = ddl_compiler._index_access_method_parameters(
                {"amparam": value},
                "reflected_access_method",
            )
        except (AttributeError, TypeError, ValueError, sa_exc.CompileError):
            return None
        return self._normalize_expression_sql(rendered)

    def _render_fragmentation_option(
        self,
        index: Any,
        value: Any,
    ) -> str | None:
        if value is None:
            return None
        try:
            ddl_compiler = self.dialect.ddl_compiler(self.dialect, None)
            clauses = ddl_compiler._fragment_storage_clauses(
                index,
                {"dbspace": None, "fragment_by": value},
            )
        except (AttributeError, TypeError, ValueError, sa_exc.CompileError):
            return None
        return self._normalize_expression_sql(" ".join(clauses))

    def _advanced_index_differences(
        self,
        metadata_index: Any,
        reflected_index: Any,
    ) -> list[str] | None:
        """Compare persistent Informix index attributes.

        ``ONLINE`` is a creation-time locking strategy, and ``FILLFACTOR`` is
        not exposed as a durable, round-trippable SYSINDICES attribute.  They
        are intentionally excluded so Alembic does not emit perpetual
        drop/create operations for metadata the server cannot reflect.
        """
        metadata = self._informix_index_options(metadata_index)
        reflected = self._informix_index_options(reflected_index)
        differences: list[str] = []

        metadata_access = self._first_option(
            metadata, "using", "access_method"
        ) or "btree"
        reflected_access = self._first_option(
            reflected, "using", "access_method"
        ) or "btree"
        if self._normalized_option(metadata_access) != self._normalized_option(
            reflected_access
        ):
            differences.append(
                "Informix access method "
                f"{reflected_access!r} to {metadata_access!r}"
            )

        metadata_opclass = metadata.get("opclass")
        if metadata_opclass is not None:
            reflected_opclass = reflected.get("opclass")
            if self._normalized_option(metadata_opclass) != self._normalized_option(
                reflected_opclass
            ):
                differences.append(
                    "Informix opclass "
                    f"{reflected_opclass!r} to {metadata_opclass!r}"
                )

        metadata_where = metadata.get("where")
        reflected_where = reflected.get("where")
        if metadata_where is not None or reflected_where is not None:
            rendered_metadata = self._render_index_predicate(metadata_where)
            rendered_reflected = self._render_index_predicate(reflected_where)
            if rendered_metadata is None or rendered_reflected is None:
                return None
            if rendered_metadata != rendered_reflected:
                differences.append(
                    "Informix partial predicate "
                    f"{rendered_reflected!r} to {rendered_metadata!r}"
                )

        metadata_fragmentation = metadata.get("fragment_by")
        reflected_fragmentation = reflected.get("fragment_by")
        if metadata_fragmentation is not None or reflected_fragmentation is not None:
            rendered_metadata = self._render_fragmentation_option(
                metadata_index, metadata_fragmentation
            )
            rendered_reflected = self._render_fragmentation_option(
                reflected_index, reflected_fragmentation
            )
            if rendered_metadata is None or rendered_reflected is None:
                return None
            if rendered_metadata != rendered_reflected:
                differences.append(
                    "Informix fragmentation "
                    f"{rendered_reflected!r} to {rendered_metadata!r}"
                )

        metadata_dbspace = metadata.get("dbspace")
        reflected_dbspace = reflected.get("dbspace")
        # Informix records the effective physical dbspace for ordinary indexes
        # even when metadata left storage placement unspecified.  An omitted
        # ``informix_dbspace`` means "use the server default", not "the index
        # must reflect without a dbspace".  Compare placement only when target
        # metadata explicitly requests one.
        if metadata_dbspace is not None:
            if self._normalized_option(metadata_dbspace) != self._normalized_option(
                reflected_dbspace
            ):
                differences.append(
                    "Informix dbspace "
                    f"{reflected_dbspace!r} to {metadata_dbspace!r}"
                )

        metadata_hash = metadata.get("hash_on")
        reflected_hash = reflected.get("hash_on")
        if metadata_hash is not None or reflected_hash is not None:
            if self._normalized_index_columns(
                metadata_hash
            ) != self._normalized_index_columns(reflected_hash):
                differences.append(
                    "Informix HASH ON columns "
                    f"{reflected_hash!r} to {metadata_hash!r}"
                )

        metadata_buckets = metadata.get("buckets")
        reflected_buckets = reflected.get("buckets")
        if (
            metadata_buckets is not None or reflected_buckets is not None
        ) and metadata_buckets != reflected_buckets:
            differences.append(
                "Informix FOT buckets "
                f"{reflected_buckets!r} to {metadata_buckets!r}"
            )

        metadata_compressed = bool(metadata.get("compressed"))
        reflected_compressed = bool(reflected.get("compressed"))
        if metadata_compressed != reflected_compressed:
            differences.append(
                "Informix compression "
                f"{reflected_compressed!r} to {metadata_compressed!r}"
            )

        metadata_visible = metadata.get("visible")
        reflected_visible = reflected.get("visible")
        metadata_visible = True if metadata_visible is None else bool(metadata_visible)
        reflected_visible = True if reflected_visible is None else bool(reflected_visible)
        if metadata_visible != reflected_visible:
            differences.append(
                "Informix visibility "
                f"{reflected_visible!r} to {metadata_visible!r}"
            )

        metadata_mode = metadata.get("mode") or "ENABLED"
        reflected_mode = reflected.get("mode") or "ENABLED"
        normalized_metadata_mode = " ".join(
            str(metadata_mode).strip().upper().split()
        )
        normalized_reflected_mode = " ".join(
            str(reflected_mode).strip().upper().split()
        )
        if normalized_metadata_mode != normalized_reflected_mode:
            differences.append(
                "Informix index mode "
                f"{normalized_reflected_mode!r} to "
                f"{normalized_metadata_mode!r}"
            )

        metadata_amparam = metadata.get("amparam")
        reflected_amparam = reflected.get("amparam")
        if metadata_amparam is not None or reflected_amparam is not None:
            rendered_metadata_amparam = self._render_access_method_parameters(
                metadata_amparam
            )
            rendered_reflected_amparam = self._render_access_method_parameters(
                reflected_amparam
            )
            if (
                rendered_metadata_amparam is None
                or rendered_reflected_amparam is None
            ):
                return None
            if rendered_metadata_amparam != rendered_reflected_amparam:
                differences.append(
                    "Informix access-method parameters "
                    f"{reflected_amparam!r} to {metadata_amparam!r}"
                )

        return differences

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
            generic_result = super().compare_indexes(
                metadata_index, reflected_index
            )
            if not generic_result.is_equal:
                return generic_result
            advanced = self._advanced_index_differences(
                metadata_index, reflected_index
            )
            if advanced is None:
                return ComparisonResult.Skip(
                    "Informix advanced-index options could not be rendered"
                )
            if advanced:
                return ComparisonResult.Different(advanced)
            return ComparisonResult.Equal()

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

        advanced = self._advanced_index_differences(
            metadata_index, reflected_index
        )
        if advanced is None:
            return ComparisonResult.Skip(
                "Informix advanced-index options could not be rendered"
            )
        differences.extend(advanced)

        if differences:
            return ComparisonResult.Different(differences)
        return ComparisonResult.Equal()


__all__ = ("InformixImpl",)
