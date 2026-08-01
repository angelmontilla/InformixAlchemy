# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Angel Montilla
"""Typed Informix fragmentation models and ALTER FRAGMENT DDL.

The public API deliberately avoids free-form SQL strings.  Fragment names and
storage spaces are validated identifiers, while conditions and values are
SQLAlchemy expression objects or Python literals compiled by SQLAlchemy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Tuple

from sqlalchemy import exc
from sqlalchemy import schema as sa_schema
from sqlalchemy.sql import elements as sql_elements
from sqlalchemy.sql.ddl import ExecutableDDLElement

from .ddl import _validate_identifier_part

_IDENTIFIER_MAX_LENGTH = 128
_UNSET = object()


def _identifier(value: Any, field_name: str) -> str:
    return _validate_identifier_part(value, field_name, _IDENTIFIER_MAX_LENGTH)


def _identifier_tuple(values: Iterable[Any], field_name: str) -> Tuple[str, ...]:
    result = tuple(_identifier(value, field_name) for value in values)
    if not result:
        raise exc.ArgumentError(f"{field_name} requires at least one identifier")
    if len(set(result)) != len(result):
        raise exc.ArgumentError(f"{field_name} must not contain duplicates")
    return result


def _tuple(values: Iterable[Any], field_name: str) -> tuple[Any, ...]:
    try:
        result = tuple(values)
    except TypeError as err:
        raise exc.ArgumentError(f"{field_name} must be an iterable") from err
    return result


def _validate_fragment_names(fragments: tuple[Any, ...]) -> None:
    names = [
        fragment.name
        for fragment in fragments
        if getattr(fragment, "name", None) is not None
    ]
    if len(set(names)) != len(names):
        raise exc.ArgumentError("fragment names must be unique")


def _validate_unique_list_values(fragments: tuple[Any, ...]) -> None:
    """Reject duplicate Python literals across LIST fragments.

    SQLAlchemy expressions are validated by the compiler and are intentionally
    excluded here because their equality operator builds SQL expressions.
    Informix itself remains the authority for database-specific literal types.
    """
    seen: set[tuple[type[Any], Any]] = set()
    for fragment in fragments:
        for value in fragment.values:
            if _is_sql_expression(value):
                continue
            marker = (type(value), value)
            try:
                duplicate = marker in seen
            except TypeError:
                continue
            if duplicate:
                raise exc.ArgumentError(
                    "LIST fragment values must be unique across fragments"
                )
            seen.add(marker)


@dataclass(frozen=True)
class _ReflectedFragmentExpression:
    """Trusted catalog expression used only by reflection round-trips."""

    sql: str
    udr_dependencies: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise exc.ArgumentError("reflected fragment expression must be non-empty")
        object.__setattr__(self, "sql", self.sql.strip())
        object.__setattr__(self, "udr_dependencies", tuple(self.udr_dependencies))


def _is_sql_expression(value: Any) -> bool:
    return isinstance(value, (sql_elements.ClauseElement, _ReflectedFragmentExpression))


def _validate_expression(value: Any, field_name: str) -> Any:
    if not _is_sql_expression(value):
        raise exc.ArgumentError(
            f"{field_name} must be a SQLAlchemy expression, not raw SQL text"
        )
    if isinstance(value, _ReflectedFragmentExpression):
        return value
    if isinstance(value, sql_elements.TextClause):
        raise exc.ArgumentError(
            f"{field_name} must be structured SQLAlchemy expressions; text() is not accepted"
        )
    return value


def _validate_optional_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise exc.ArgumentError(f"{field_name} must be a boolean")
    return value


@dataclass(frozen=True)
class Fragment:
    """One named or unnamed fragment in a typed distribution strategy.

    Only fields meaningful for the enclosing strategy may be populated:

    * ``expression`` for expression/range strategies;
    * ``values`` or ``is_null`` for list strategies;
    * ``upper_bound`` or ``is_null`` for range-interval strategies;
    * ``remainder`` for expression/list strategies;
    * ``dbspace`` for the physical location.
    """

    name: Optional[str] = None
    expression: Any = None
    dbspace: Optional[str] = None
    values: tuple[Any, ...] = field(default_factory=tuple)
    upper_bound: Any = field(default=_UNSET, repr=False)
    is_null: bool = False
    remainder: bool = False
    _catalog_selector: Optional[_ReflectedFragmentExpression] = field(
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        if self.name is not None:
            _identifier(self.name, "fragment name")
        if self.dbspace is not None:
            _identifier(self.dbspace, "fragment dbspace")
        if self.expression is not None:
            _validate_expression(self.expression, "fragment expression")
        if self._catalog_selector is not None and not isinstance(
            self._catalog_selector,
            _ReflectedFragmentExpression,
        ):
            raise exc.ArgumentError(
                "_catalog_selector is reserved for reflected catalog metadata"
            )

        object.__setattr__(self, "values", _tuple(self.values, "fragment values"))
        _validate_optional_bool(self.is_null, "fragment is_null")
        _validate_optional_bool(self.remainder, "fragment remainder")

        selectors = sum(
            (
                self.expression is not None,
                bool(self.values),
                self.upper_bound is not _UNSET,
                self.is_null,
                self.remainder,
                self._catalog_selector is not None,
            )
        )
        if selectors > 1:
            raise exc.ArgumentError(
                "Fragment accepts only one selector: expression, values, "
                "upper_bound, is_null, or remainder"
            )

    @property
    def has_upper_bound(self) -> bool:
        return self.upper_bound is not _UNSET


@dataclass(frozen=True)
class RoundRobinFragmentation:
    """Round-robin table fragmentation across dbspaces or named partitions."""

    dbspaces: tuple[str, ...] = field(default_factory=tuple)
    fragments: tuple[Fragment, ...] = field(default_factory=tuple)
    partition_by: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "dbspaces",
            tuple(_identifier(value, "round-robin dbspace") for value in self.dbspaces),
        )
        if len(set(self.dbspaces)) != len(self.dbspaces):
            raise exc.ArgumentError("round-robin dbspaces must not contain duplicates")
        object.__setattr__(self, "fragments", _tuple(self.fragments, "fragments"))
        _validate_fragment_names(self.fragments)
        _validate_optional_bool(self.partition_by, "partition_by")

        if bool(self.dbspaces) == bool(self.fragments):
            raise exc.ArgumentError(
                "RoundRobinFragmentation requires either dbspaces or fragments"
            )
        if self.dbspaces and len(self.dbspaces) < 2:
            raise exc.ArgumentError(
                "RoundRobinFragmentation requires at least two dbspaces"
            )
        if self.fragments:
            if len(self.fragments) < 2:
                raise exc.ArgumentError(
                    "RoundRobinFragmentation requires at least two fragments"
                )
            for fragment in self.fragments:
                if not isinstance(fragment, Fragment):
                    raise exc.ArgumentError("fragments must contain Fragment objects")
                if fragment.dbspace is None:
                    raise exc.ArgumentError("round-robin fragments require dbspace")
                if any(
                    (
                        fragment.expression is not None,
                        bool(fragment.values),
                        fragment.has_upper_bound,
                        fragment.is_null,
                        fragment.remainder,
                        fragment._catalog_selector is not None,
                    )
                ):
                    raise exc.ArgumentError(
                        "round-robin fragments cannot define expressions or values"
                    )


@dataclass(frozen=True)
class ExpressionFragmentation:
    """Native ``FRAGMENT BY EXPRESSION`` strategy."""

    fragments: tuple[Fragment, ...]
    partition_by: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "fragments", _tuple(self.fragments, "fragments"))
        _validate_optional_bool(self.partition_by, "partition_by")
        _validate_expression_fragments(self.fragments, "ExpressionFragmentation")


@dataclass(frozen=True)
class RangeFragmentation:
    """Typed fixed-range strategy compiled through ``BY EXPRESSION``.

    Informix 14.10 has no separate fixed ``FRAGMENT BY RANGE`` grammar.  Its
    native RANGE form is range-interval fragmentation.  Fixed ranges are
    therefore represented safely as ordered Boolean expressions.
    """

    fragments: tuple[Fragment, ...]
    partition_by: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "fragments", _tuple(self.fragments, "fragments"))
        _validate_optional_bool(self.partition_by, "partition_by")
        _validate_expression_fragments(self.fragments, "RangeFragmentation")


def _validate_expression_fragments(fragments: tuple[Fragment, ...], owner: str) -> None:
    _validate_fragment_names(fragments)
    if len(fragments) < 2:
        raise exc.ArgumentError(f"{owner} requires at least two fragments")
    remainder_seen = False
    for position, fragment in enumerate(fragments):
        if not isinstance(fragment, Fragment):
            raise exc.ArgumentError("fragments must contain Fragment objects")
        if fragment.dbspace is None:
            raise exc.ArgumentError(f"{owner} fragments require dbspace")
        if fragment._catalog_selector is not None:
            pass
        elif fragment.remainder:
            if position != len(fragments) - 1:
                raise exc.ArgumentError("REMAINDER must be the last fragment")
            remainder_seen = True
        elif fragment.is_null:
            pass
        elif fragment.expression is None:
            raise exc.ArgumentError(
                f"{owner} fragments require expression, is_null=True, "
                "or remainder=True"
            )
        if any((fragment.values, fragment.has_upper_bound)):
            raise exc.ArgumentError(
                f"{owner} fragments cannot define list/range-interval selectors"
            )
    if sum(1 for fragment in fragments if fragment.remainder) > 1 or (
        remainder_seen and not fragments[-1].remainder
    ):
        raise exc.ArgumentError("only one final REMAINDER fragment is allowed")


@dataclass(frozen=True)
class ListFragmentation:
    """Native ``FRAGMENT BY LIST(key)`` strategy."""

    key: Any
    fragments: tuple[Fragment, ...]
    partition_by: bool = False

    def __post_init__(self) -> None:
        _validate_expression(self.key, "list fragmentation key")
        object.__setattr__(self, "fragments", _tuple(self.fragments, "fragments"))
        _validate_fragment_names(self.fragments)
        _validate_unique_list_values(self.fragments)
        _validate_optional_bool(self.partition_by, "partition_by")
        if len(self.fragments) < 2:
            raise exc.ArgumentError("ListFragmentation requires at least two fragments")

        remainder_count = 0
        null_count = 0
        for position, fragment in enumerate(self.fragments):
            if not isinstance(fragment, Fragment):
                raise exc.ArgumentError("fragments must contain Fragment objects")
            if fragment.name is None or fragment.dbspace is None:
                raise exc.ArgumentError(
                    "list fragments require both name and dbspace"
                )
            if fragment._catalog_selector is not None:
                pass
            elif fragment.remainder:
                remainder_count += 1
                if position != len(self.fragments) - 1:
                    raise exc.ArgumentError("REMAINDER must be the last fragment")
            elif fragment.is_null:
                null_count += 1
            elif not fragment.values:
                raise exc.ArgumentError(
                    "list fragments require values, is_null=True, or remainder=True"
                )
            if fragment.expression is not None or fragment.has_upper_bound:
                raise exc.ArgumentError(
                    "list fragments cannot define expression or upper_bound"
                )
        if remainder_count > 1 or null_count > 1:
            raise exc.ArgumentError(
                "ListFragmentation permits at most one NULL and one REMAINDER fragment"
            )


@dataclass(frozen=True)
class RangeIntervalFragmentation:
    """Native Informix ``FRAGMENT BY RANGE ... INTERVAL`` strategy."""

    key: Any
    interval: Any
    fragments: tuple[Fragment, ...]
    store_in: tuple[str, ...] = field(default_factory=tuple)
    partition_by: bool = False

    def __post_init__(self) -> None:
        _validate_expression(self.key, "range-interval fragmentation key")
        if self.interval is None:
            raise exc.ArgumentError("RangeIntervalFragmentation requires interval")
        object.__setattr__(self, "fragments", _tuple(self.fragments, "fragments"))
        _validate_fragment_names(self.fragments)
        object.__setattr__(
            self,
            "store_in",
            tuple(_identifier(value, "interval dbspace") for value in self.store_in),
        )
        _validate_optional_bool(self.partition_by, "partition_by")
        if not self.fragments:
            raise exc.ArgumentError(
                "RangeIntervalFragmentation requires at least one range fragment"
            )
        if self.store_in and len(set(self.store_in)) != len(self.store_in):
            raise exc.ArgumentError("store_in must not contain duplicate dbspaces")

        null_count = 0
        for fragment in self.fragments:
            if not isinstance(fragment, Fragment):
                raise exc.ArgumentError("fragments must contain Fragment objects")
            if fragment.name is None or fragment.dbspace is None:
                raise exc.ArgumentError(
                    "range-interval fragments require both name and dbspace"
                )
            if fragment._catalog_selector is not None:
                pass
            elif fragment.is_null:
                null_count += 1
            elif not fragment.has_upper_bound:
                raise exc.ArgumentError(
                    "range-interval fragments require upper_bound or is_null=True"
                )
            if any((fragment.expression is not None, fragment.values, fragment.remainder)):
                raise exc.ArgumentError(
                    "range-interval fragments cannot define expression, values, or remainder"
                )
        if null_count > 1:
            raise exc.ArgumentError(
                "RangeIntervalFragmentation permits at most one NULL fragment"
            )


@dataclass(frozen=True)
class AttachedIndexFragmentation:
    """Reflection marker for an index attached to its table's strategy."""

    strategy: str = "table"


Fragmentation = (
    RoundRobinFragmentation
    | ExpressionFragmentation
    | RangeFragmentation
    | ListFragmentation
    | RangeIntervalFragmentation
    | AttachedIndexFragmentation
)


def _validate_subject(subject: Any, *, allow_index: bool = True) -> Any:
    valid = (sa_schema.Table, sa_schema.Index) if allow_index else (sa_schema.Table,)
    if not isinstance(subject, valid):
        expected = "Table or Index" if allow_index else "Table"
        raise exc.ArgumentError(f"fragment subject must be a SQLAlchemy {expected}")
    return subject


def _validate_online(value: Any) -> bool:
    if not isinstance(value, bool):
        raise exc.ArgumentError("online must be a boolean")
    return value


class _AlterFragmentBase(ExecutableDDLElement):
    inherit_cache = False

    def __init__(self, subject: Any, *, online: bool = False) -> None:
        self.subject = _validate_subject(subject)
        self.element = subject
        self.online = _validate_online(online)


class InitFragmentation(_AlterFragmentBase):
    """Apply or replace a table/index fragmentation strategy."""

    __visit_name__ = "init_fragmentation"

    def __init__(
        self,
        subject: Any,
        fragment_by: Optional[Fragmentation] = None,
        *,
        dbspace: Optional[str] = None,
        fragment_name: Optional[str] = None,
        online: bool = False,
    ) -> None:
        super().__init__(subject, online=online)
        if fragment_by is None and dbspace is None:
            raise exc.ArgumentError(
                "InitFragmentation requires fragment_by or dbspace"
            )
        if fragment_by is not None and dbspace is not None:
            raise exc.ArgumentError(
                "InitFragmentation accepts fragment_by or dbspace, not both"
            )
        if dbspace is not None:
            _identifier(dbspace, "dbspace")
        if fragment_name is not None:
            _identifier(fragment_name, "fragment name")
            if dbspace is None:
                raise exc.ArgumentError("fragment_name requires dbspace")
        self.fragment_by = fragment_by
        self.dbspace = dbspace
        self.fragment_name = fragment_name


class AddFragment(_AlterFragmentBase):
    """Add one typed fragment or interval storage dbspaces."""

    __visit_name__ = "add_fragment"

    def __init__(
        self,
        subject: Any,
        fragment: Optional[Fragment] = None,
        *,
        before: Optional[str] = None,
        after: Optional[str] = None,
        interval_dbspaces: Iterable[str] = (),
        online: bool = False,
    ) -> None:
        super().__init__(subject, online=online)
        self.interval_dbspaces = tuple(
            _identifier(value, "interval dbspace") for value in interval_dbspaces
        )
        if (fragment is None) == (not self.interval_dbspaces):
            raise exc.ArgumentError(
                "AddFragment requires exactly one fragment or interval_dbspaces"
            )
        if fragment is not None and not isinstance(fragment, Fragment):
            raise exc.ArgumentError("fragment must be a Fragment")
        if before is not None:
            _identifier(before, "before fragment")
        if after is not None:
            _identifier(after, "after fragment")
        if before is not None and after is not None:
            raise exc.ArgumentError("before and after are mutually exclusive")
        if self.interval_dbspaces and (before is not None or after is not None):
            raise exc.ArgumentError(
                "before/after cannot be combined with interval_dbspaces"
            )
        self.fragment = fragment
        self.before = before
        self.after = after


class DropFragment(_AlterFragmentBase):
    """Drop one fragment or interval storage dbspaces."""

    __visit_name__ = "drop_fragment"

    def __init__(
        self,
        subject: Any,
        fragment_name: Optional[str] = None,
        *,
        partition: bool = False,
        interval_dbspaces: Iterable[str] = (),
        online: bool = False,
    ) -> None:
        super().__init__(subject, online=online)
        self.interval_dbspaces = tuple(
            _identifier(value, "interval dbspace") for value in interval_dbspaces
        )
        if (fragment_name is None) == (not self.interval_dbspaces):
            raise exc.ArgumentError(
                "DropFragment requires exactly one fragment_name or interval_dbspaces"
            )
        if fragment_name is not None:
            _identifier(fragment_name, "fragment name")
        self.fragment_name = fragment_name
        self.partition = _validate_optional_bool(partition, "partition")


class ModifyFragment(_AlterFragmentBase):
    """Replace one existing expression, list, or range fragment definition."""

    __visit_name__ = "modify_fragment"

    def __init__(
        self,
        subject: Any,
        old_name: str,
        fragment: Fragment,
        *,
        old_partition: bool = False,
        online: bool = False,
    ) -> None:
        super().__init__(subject, online=online)
        self.old_name = _identifier(old_name, "old fragment name")
        if not isinstance(fragment, Fragment):
            raise exc.ArgumentError("fragment must be a Fragment")
        if fragment.dbspace is None:
            raise exc.ArgumentError("modified fragment requires dbspace")
        if not any(
            (
                fragment.expression is not None,
                fragment.values,
                fragment.has_upper_bound,
                fragment.is_null,
                fragment.remainder,
                fragment._catalog_selector is not None,
            )
        ):
            raise exc.ArgumentError(
                "modified fragment requires expression, values, upper_bound, "
                "is_null, or remainder"
            )
        self.fragment = fragment
        self.old_partition = _validate_optional_bool(
            old_partition, "old_partition"
        )


class AttachFragment(_AlterFragmentBase):
    """Attach a nonfragmented table to a surviving table."""

    __visit_name__ = "attach_fragment"

    def __init__(
        self,
        surviving_table: sa_schema.Table,
        consumed_table: sa_schema.Table,
        *,
        fragment: Optional[Fragment] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        online: bool = False,
    ) -> None:
        if not isinstance(surviving_table, sa_schema.Table):
            raise exc.ArgumentError("surviving_table must be a Table")
        if not isinstance(consumed_table, sa_schema.Table):
            raise exc.ArgumentError("consumed_table must be a Table")
        super().__init__(surviving_table, online=online)
        if fragment is not None and not isinstance(fragment, Fragment):
            raise exc.ArgumentError("fragment must be a Fragment")
        if fragment is not None and fragment.dbspace is not None:
            raise exc.ArgumentError(
                "ATTACH does not accept a fragment dbspace; Informix uses "
                "the consumed table's existing storage location"
            )
        if before is not None:
            _identifier(before, "before fragment")
        if after is not None:
            _identifier(after, "after fragment")
        if before is not None and after is not None:
            raise exc.ArgumentError("before and after are mutually exclusive")
        if (before is not None or after is not None) and fragment is None:
            raise exc.ArgumentError(
                "ATTACH before/after positioning requires an AS fragment definition"
            )
        if surviving_table is consumed_table:
            raise exc.ArgumentError(
                "surviving_table and consumed_table must be different tables"
            )
        self.surviving_table = surviving_table
        self.consumed_table = consumed_table
        self.fragment = fragment
        self.before = before
        self.after = after


class DetachFragment(_AlterFragmentBase):
    """Detach one table fragment into a new nonfragmented table."""

    __visit_name__ = "detach_fragment"

    def __init__(
        self,
        table: sa_schema.Table,
        fragment_name: str,
        new_table: sa_schema.Table,
        *,
        partition: bool = True,
        online: bool = False,
    ) -> None:
        if not isinstance(table, sa_schema.Table):
            raise exc.ArgumentError("table must be a Table")
        if not isinstance(new_table, sa_schema.Table):
            raise exc.ArgumentError("new_table must be a Table")
        super().__init__(table, online=online)
        self.fragment_name = _identifier(fragment_name, "fragment name")
        self.new_table = new_table
        self.partition = _validate_optional_bool(partition, "partition")


# Concise aliases matching the native ALTER FRAGMENT operation names.
InitFragment = InitFragmentation
