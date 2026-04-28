# SPDX-License-Identifier: Apache-2.0
"""SQLAlchemy compatibility helpers for IfxAlchemy.

All direct access to SQLAlchemy private Select internals must live here.
The compiler must import helpers from this module instead of touching
private attributes inline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlalchemy
from sqlalchemy import exc

try:
    from packaging.version import Version
except ImportError:  # pragma: no cover - exercised only without packaging

    class Version:
        def __init__(self, value):
            self._parts = self._parse(value)

        @staticmethod
        def _parse(value):
            head = str(value).split("+", 1)[0]
            head = head.replace("b", ".")
            parts = []
            for token in head.split("."):
                try:
                    parts.append(int(token))
                except ValueError:
                    break
            return tuple(parts)

        def __ge__(self, other):
            return self._parts >= other._parts


SQLALCHEMY_VERSION = Version(sqlalchemy.__version__)
SQLALCHEMY_IS_21_PLUS = SQLALCHEMY_VERSION >= Version("2.1.0b1")


@dataclass(frozen=True)
class IfxSelectLimitState:
    limit_clause: Any
    limit_value: Any
    offset_clause: Any
    offset_value: Any
    fetch_clause: Any
    fetch_options: dict[str, Any]
    distinct: Any
    order_by_clauses: tuple[Any, ...]


def _missing_private_api(name: str) -> exc.CompileError:
    return exc.CompileError(
        "The Informix dialect requires SQLAlchemy Select private API "
        f"{name!r}, which is not available in SQLAlchemy "
        f"{sqlalchemy.__version__}. "
        "Update IfxAlchemy.sqla_compat for this SQLAlchemy release."
    )


def get_fetch_clause(select):
    return getattr(select, "_fetch_clause", None)


def get_fetch_clause_options(select):
    return getattr(select, "_fetch_clause_options", None) or {
        "percent": False,
        "with_ties": False,
    }


def get_limit_clause(select):
    return getattr(select, "_limit_clause", None)


def get_offset_clause(select):
    return getattr(select, "_offset_clause", None)


def get_distinct(select):
    return getattr(select, "_distinct", False)


def get_select_for_update_arg(select):
    """Return the private Select._for_update_arg value.

    This is intentionally centralized because SQLAlchemy does not expose
    a stable public accessor for this compiler-level state.
    """

    return getattr(select, "_for_update_arg", None)


def get_select_for_update(select):
    """Return compiler-level FOR UPDATE state for a Select."""

    return get_select_for_update_arg(select)


def get_offset_value(select):
    return getattr(select, "_offset", None)


def get_limit_value(select):
    return getattr(select, "_limit", None)


def clone_select(select):
    generate = getattr(select, "_generate", None)
    if generate is None:
        raise _missing_private_api("_generate")
    return generate()


def simple_int_clause(select, clause):
    if clause is None:
        return False

    meth = getattr(select, "_simple_int_clause", None)
    if meth is None:
        raise _missing_private_api("_simple_int_clause")

    return meth(clause)


def offset_or_limit_clause_asint(select, clause, attrname):
    meth = getattr(select, "_offset_or_limit_clause_asint", None)
    if meth is None:
        raise _missing_private_api("_offset_or_limit_clause_asint")

    return meth(clause, attrname)


def get_order_by_clauses(select):
    clauses = getattr(select, "_order_by_clauses", None)
    if clauses is None:
        raise _missing_private_api("_order_by_clauses")

    return tuple(clauses)


def get_table_autoincrement_column(table):
    """Return SQLAlchemy's selected autoincrement column for a Table."""

    return getattr(table, "_autoincrement_column", None)


def get_table_sorted_constraints(table):
    """Return SQLAlchemy's internally sorted constraints for DDL emission."""

    return getattr(table, "_sorted_constraints", ())


def identifier_requires_quotes(preparer, value):
    """Return whether an identifier requires quoting.

    SQLAlchemy exposes this as an internal preparer rule; keep the access
    in one place so minor-version changes fail in the compatibility layer.
    """

    return preparer._requires_quotes(value)


def get_dml_compile_state(compiled):
    """Return the DML compile state associated with a compiled statement."""

    return getattr(compiled, "dml_compile_state", None)


def compiled_returns_rows(compiled):
    """Return whether a compiled DML statement has effective RETURNING."""

    return bool(getattr(compiled, "effective_returning", None))


def get_statement_returning(statement):
    """Return SQLAlchemy's private DML returning collection, if present."""

    return getattr(statement, "_returning", None)


def get_limit_state(select) -> IfxSelectLimitState:
    return IfxSelectLimitState(
        limit_clause=get_limit_clause(select),
        limit_value=get_limit_value(select),
        offset_clause=get_offset_clause(select),
        offset_value=get_offset_value(select),
        fetch_clause=get_fetch_clause(select),
        fetch_options=get_fetch_clause_options(select),
        distinct=get_distinct(select),
        order_by_clauses=get_order_by_clauses(select),
    )
