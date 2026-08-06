# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Angel Montilla
"""Typed Informix optimizer directives and session controls.

This module deliberately exposes no raw-text directive escape hatch.  Informix
optimizer directives are SQL comments, so accepting arbitrary strings would
create a comment-termination and SQL-injection surface.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

from sqlalchemy import exc
from sqlalchemy import schema as sa_schema
from sqlalchemy.engine import characteristics
from sqlalchemy.sql import selectable


INFORMIX_OPTIMIZER_DIRECTIVES = "informix_optimizer_directives"
INFORMIX_OPTIMIZATION = "informix_optimization"
INFORMIX_PDQPRIORITY = "informix_pdqpriority"
INFORMIX_STATEMENT_CACHE = "informix_statement_cache"
INFORMIX_EXPLAIN = "informix_explain"

_SESSION_DEFAULTS = {
    INFORMIX_OPTIMIZATION: "ALL_ROWS",
    # -1 restores the server/environment PDQ default for pooled connections.
    INFORMIX_PDQPRIORITY: -1,
    INFORMIX_STATEMENT_CACHE: False,
    INFORMIX_EXPLAIN: "OFF",
}

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_DML_KEYWORDS = frozenset({"SELECT", "UPDATE", "DELETE"})


def _validate_identifier(value: Any, *, role: str) -> str:
    if not isinstance(value, str) or not value:
        raise exc.ArgumentError(f"Informix {role} must be a non-empty string")
    if not _SAFE_IDENTIFIER_RE.fullmatch(value):
        raise exc.ArgumentError(
            f"Unsafe Informix {role} {value!r}; only unquoted identifier "
            "characters are accepted by optimizer directive objects"
        )
    return value


def _table_name_for_cache(table: Any) -> tuple[Any, ...]:
    name = getattr(table, "name", None)
    schema = getattr(table, "schema", None)
    original = getattr(table, "original", None)
    original_name = getattr(original, "name", None)
    return (
        type(table).__name__,
        str(schema or ""),
        str(name or ""),
        str(original_name or ""),
    )


def _validate_table_reference(table: Any) -> Any:
    if isinstance(table, sa_schema.Table):
        _validate_identifier(table.name, role="table name")
        return table

    if isinstance(table, selectable.Alias):
        alias_name = getattr(table, "name", None)
        if (
            not isinstance(alias_name, str)
            or alias_name.startswith("%(")
        ):
            raise exc.ArgumentError(
                "Informix optimizer directives require aliases with an "
                "explicit stable name"
            )
        _validate_identifier(alias_name, role="table alias")
        original = getattr(table, "original", None)
        if not isinstance(original, sa_schema.Table):
            raise exc.ArgumentError(
                "Informix INDEX directives only accept a Table or an alias "
                "whose original object is a Table"
            )
        return table

    raise exc.ArgumentError(
        "Informix INDEX directives require a SQLAlchemy Table or explicit "
        "Table alias, not arbitrary SQL text"
    )


def _index_name(index: Any) -> str:
    if isinstance(index, sa_schema.Index):
        if index.name is None:
            raise exc.ArgumentError(
                "Informix optimizer directives require a named Index"
            )
        return _validate_identifier(index.name, role="index name")
    return _validate_identifier(index, role="index name")


def _base_table(table: Any) -> sa_schema.Table:
    table = _validate_table_reference(table)
    if isinstance(table, selectable.Alias):
        return table.original
    return table


def _validate_index_for_table(table: Any, index: Any) -> str:
    name = _index_name(index)
    if isinstance(index, sa_schema.Index):
        if index.table is not _base_table(table):
            raise exc.ArgumentError(
                f"Informix optimizer index {name!r} does not belong to "
                f"table {_base_table(table).name!r}"
            )
    return name


class OptimizerDirective:
    """Base class for typed, immutable Informix directives."""

    __slots__ = ()

    @property
    def cache_key(self) -> tuple[Any, ...]:
        raise NotImplementedError

    def render(self, compiler) -> str:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class FirstRows(OptimizerDirective):
    @property
    def cache_key(self) -> tuple[str]:
        return ("FIRST_ROWS",)

    def render(self, compiler) -> str:
        return "FIRST_ROWS"


@dataclass(frozen=True, slots=True)
class AllRows(OptimizerDirective):
    @property
    def cache_key(self) -> tuple[str]:
        return ("ALL_ROWS",)

    def render(self, compiler) -> str:
        return "ALL_ROWS"


@dataclass(frozen=True, slots=True)
class JoinOrder(OptimizerDirective):
    @property
    def cache_key(self) -> tuple[str]:
        return ("ORDERED",)

    def render(self, compiler) -> str:
        return "ORDERED"


@dataclass(frozen=True, slots=True)
class UseIndex(OptimizerDirective):
    table: Any
    index: Any

    def __post_init__(self) -> None:
        _validate_table_reference(self.table)
        _validate_index_for_table(self.table, self.index)

    @property
    def cache_key(self) -> tuple[Any, ...]:
        return (
            "INDEX",
            _table_name_for_cache(self.table),
            _validate_index_for_table(self.table, self.index),
        )

    def render(self, compiler) -> str:
        return "INDEX(%s %s)" % (
            render_directive_table(self.table, compiler),
            compiler.preparer.quote(
                _validate_index_for_table(self.table, self.index)
            ),
        )


@dataclass(frozen=True, slots=True)
class AvoidIndex(OptimizerDirective):
    table: Any
    index: Any

    def __post_init__(self) -> None:
        _validate_table_reference(self.table)
        _validate_index_for_table(self.table, self.index)

    @property
    def cache_key(self) -> tuple[Any, ...]:
        return (
            "AVOID_INDEX",
            _table_name_for_cache(self.table),
            _validate_index_for_table(self.table, self.index),
        )

    def render(self, compiler) -> str:
        return "AVOID_INDEX(%s %s)" % (
            render_directive_table(self.table, compiler),
            compiler.preparer.quote(
                _validate_index_for_table(self.table, self.index)
            ),
        )


def render_directive_table(table: Any, compiler) -> str:
    table = _validate_table_reference(table)
    if isinstance(table, selectable.Alias):
        return compiler.preparer.format_alias(table)
    return compiler.preparer.format_table(table, use_schema=False)


def normalize_optimizer_directives(value: Any) -> tuple[OptimizerDirective, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes, OptimizerDirective)):
        raise exc.ArgumentError(
            "informix_optimizer_directives must be a sequence of typed "
            "directive objects"
        )
    try:
        directives = tuple(value)
    except TypeError as error:
        raise exc.ArgumentError(
            "informix_optimizer_directives must be an iterable of typed "
            "directive objects"
        ) from error

    goal_count = 0
    seen: set[tuple[Any, ...]] = set()
    for directive in directives:
        if not isinstance(directive, OptimizerDirective):
            raise exc.ArgumentError(
                "Informix optimizer directives do not accept arbitrary text; "
                f"received {type(directive).__name__}"
            )
        if isinstance(directive, (FirstRows, AllRows)):
            goal_count += 1
        if directive.cache_key in seen:
            raise exc.ArgumentError(
                f"Duplicate Informix optimizer directive: {directive!r}"
            )
        seen.add(directive.cache_key)

    if goal_count > 1:
        raise exc.ArgumentError(
            "FIRST_ROWS and ALL_ROWS are mutually exclusive"
        )
    return directives


def optimizer_directives_cache_key(value: Any) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        directive.cache_key
        for directive in normalize_optimizer_directives(value)
    )


def render_optimizer_comment(value: Any, compiler) -> str:
    directives = normalize_optimizer_directives(value)
    if not directives:
        return ""
    return "{+" + ", ".join(d.render(compiler) for d in directives) + "}"


def _scan_top_level_keyword(
    statement: str,
    keyword: str,
) -> tuple[int, int] | None:
    """Locate a root DML keyword, ignoring strings, comments, and CTE bodies."""
    target = keyword.upper()
    if target not in _DML_KEYWORDS:
        raise ValueError(f"Unsupported DML keyword: {keyword!r}")

    index = 0
    length = len(statement)
    depth = 0
    quote: str | None = None
    while index < length:
        char = statement[index]

        if quote is not None:
            if char == quote:
                if index + 1 < length and statement[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            elif char == "\\" and index + 1 < length:
                index += 2
                continue
            index += 1
            continue

        if statement.startswith("--", index):
            newline = statement.find("\n", index + 2)
            index = length if newline < 0 else newline + 1
            continue
        if statement.startswith("/*", index):
            end = statement.find("*/", index + 2)
            if end < 0:
                raise exc.CompileError("Unterminated SQL comment")
            index = end + 2
            continue
        if char in ("'", '"'):
            quote = char
            index += 1
            continue
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            index += 1
            continue
        if depth == 0 and (char.isalpha() or char == "_"):
            start = index
            index += 1
            while index < length and (
                statement[index].isalnum() or statement[index] in "_$"
            ):
                index += 1
            if statement[start:index].upper() == target:
                return start, index
            continue
        index += 1
    return None


def insert_optimizer_comment(statement: str, keyword: str, comment: str) -> str:
    if not comment:
        return statement
    located = _scan_top_level_keyword(statement, keyword)
    if located is None:
        raise exc.CompileError(
            f"Could not locate top-level {keyword} for Informix optimizer "
            "directives"
        )
    _, end = located
    return statement[:end] + " " + comment + statement[end:]


def normalize_session_option(name: str, value: Any) -> Any:
    if name == INFORMIX_OPTIMIZATION:
        if not isinstance(value, str):
            raise exc.ArgumentError(
                "informix_optimization must be FIRST_ROWS or ALL_ROWS"
            )
        normalized = value.strip().upper().replace("-", "_").replace(" ", "_")
        if normalized not in {"FIRST_ROWS", "ALL_ROWS"}:
            raise exc.ArgumentError(
                "informix_optimization must be FIRST_ROWS or ALL_ROWS"
            )
        return normalized

    if name == INFORMIX_PDQPRIORITY:
        if isinstance(value, bool) or not isinstance(value, int):
            raise exc.ArgumentError(
                "informix_pdqpriority must be an integer from -1 through 100"
            )
        if value < -1 or value > 100:
            raise exc.ArgumentError(
                "informix_pdqpriority must be from -1 through 100"
            )
        return value

    if name == INFORMIX_STATEMENT_CACHE:
        if not isinstance(value, bool):
            raise exc.ArgumentError(
                "informix_statement_cache must be True or False"
            )
        return value

    if name == INFORMIX_EXPLAIN:
        if isinstance(value, bool):
            return "ON" if value else "OFF"
        if isinstance(value, str):
            normalized = (
                value.strip()
                .upper()
                .replace("-", "_")
                .replace(" ", "_")
            )
            if normalized in {"ON", "OFF", "AVOID_EXECUTE"}:
                return normalized
        raise exc.ArgumentError(
            "informix_explain must be True, False, 'ON', 'OFF', or "
            "'AVOID_EXECUTE'"
        )

    raise exc.ArgumentError(f"Unknown Informix session option: {name!r}")


def session_option_sql(name: str, value: Any) -> str:
    normalized = normalize_session_option(name, value)
    if name == INFORMIX_OPTIMIZATION:
        return f"SET OPTIMIZATION {normalized}"
    if name == INFORMIX_PDQPRIORITY:
        return f"SET PDQPRIORITY {normalized}"
    if name == INFORMIX_STATEMENT_CACHE:
        return "SET STATEMENT CACHE " + ("ON" if normalized else "OFF")
    if name == INFORMIX_EXPLAIN:
        if normalized == "AVOID_EXECUTE":
            return "SET EXPLAIN ON AVOID_EXECUTE"
        return f"SET EXPLAIN {normalized}"
    raise AssertionError(name)


def default_session_option(name: str) -> Any:
    return _SESSION_DEFAULTS[name]


class InformixSessionCharacteristic(characteristics.ConnectionCharacteristic):
    """SQLAlchemy connection characteristic backed by an Informix SET command."""

    __slots__ = ("name",)
    transactional = True

    def __init__(self, name: str):
        self.name = name

    def reset_characteristic(self, dialect, dbapi_conn) -> None:
        dialect.reset_informix_session_option(dbapi_conn, self.name)

    def set_characteristic(self, dialect, dbapi_conn, value: Any) -> None:
        dialect.set_informix_session_option(dbapi_conn, self.name, value)

    def get_characteristic(self, dialect, dbapi_conn) -> Any:
        return dialect.get_informix_session_option(dbapi_conn, self.name)


INFORMIX_CONNECTION_CHARACTERISTICS: Mapping[str, InformixSessionCharacteristic] = {
    name: InformixSessionCharacteristic(name)
    for name in _SESSION_DEFAULTS
}


__all__ = (
    "AllRows",
    "AvoidIndex",
    "FirstRows",
    "INFORMIX_CONNECTION_CHARACTERISTICS",
    "INFORMIX_EXPLAIN",
    "INFORMIX_OPTIMIZATION",
    "INFORMIX_OPTIMIZER_DIRECTIVES",
    "INFORMIX_PDQPRIORITY",
    "INFORMIX_STATEMENT_CACHE",
    "JoinOrder",
    "OptimizerDirective",
    "UseIndex",
    "default_session_option",
    "insert_optimizer_comment",
    "normalize_optimizer_directives",
    "normalize_session_option",
    "optimizer_directives_cache_key",
    "render_optimizer_comment",
    "session_option_sql",
)
