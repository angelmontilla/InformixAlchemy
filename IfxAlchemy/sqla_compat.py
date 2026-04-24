# SPDX-License-Identifier: Apache-2.0
"""SQLAlchemy compatibility helpers for IfxAlchemy.

All direct access to SQLAlchemy private Select internals must live here.
The compiler must import helpers from this module instead of touching
private attributes inline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import exc


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
        f"{name!r}, which is not available in this SQLAlchemy version. "
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
