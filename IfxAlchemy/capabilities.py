# SPDX-License-Identifier: Apache-2.0
"""Auditable capability contract for Informix and SQLAlchemy.

The dialect flags in :mod:`IfxAlchemy.base` describe implementation behavior,
while SQLAlchemy's ``SuiteRequirements`` decide which conformance tests run.
This module keeps both views explicit so unsupported functionality cannot be
mistaken for untested functionality.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class Capability:
    server: str
    dialect: str
    suite: str
    status: str
    rationale: str


CAPABILITY_MATRIX = MappingProxyType(
    {
        "savepoints": Capability(
            server="yes",
            dialect="native SQL compilation",
            suite="open",
            status="supported; live certification required",
            rationale=(
                "SAVEPOINT, ROLLBACK TO SAVEPOINT and RELEASE SAVEPOINT are "
                "compiled explicitly and covered by integration tests."
            ),
        ),
        "autocommit": Capability(
            server="driver mode",
            dialect="pyodbc isolation level",
            suite="open",
            status="supported; driver matrix required",
            rationale=(
                "AUTOCOMMIT is implemented as a DBAPI connection mode and "
                "restores the configured native isolation level."
            ),
        ),
        "foreign_key_constraint_name_reflection": Capability(
            server="yes",
            dialect="catalog reflection",
            suite="open",
            status="supported",
            rationale="SYSREFERENCES/SYSCONSTRAINTS preserve FK names.",
        ),
        "emulated_lastrowid": Capability(
            server="DBINFO",
            dialect="execution-context query",
            suite="open",
            status="supported",
            rationale=(
                "The execution context obtains generated SERIAL values from "
                "DBINFO immediately after singleton INSERT."
            ),
        ),
        "nullsordering": Capability(
            server="yes",
            dialect="generic SQLAlchemy rendering",
            suite="open",
            status="supported",
            rationale="Informix accepts ORDER BY ... NULLS FIRST/LAST.",
        ),
        "native_interval": Capability(
            server="yes",
            dialect="native INTERVAL type",
            suite="dialect tests",
            status="supported; matrix certification required",
            rationale=(
                "Qualifiers, leading precision and fractional precision are "
                "preserved by DDL, reflection, values and Alembic comparison."
            ),
        ),
        "datetime_interval": Capability(
            server="yes",
            dialect="native type only",
            suite="closed",
            status="generic SQLAlchemy contract not claimed",
            rationale=(
                "The suite requirement targets generic sqlalchemy.Interval; "
                "the dialect exposes IfxAlchemy.INTERVAL to avoid lossy mapping."
            ),
        ),
        "datetime_literals": Capability(
            server="partial",
            dialect="bind-first policy",
            suite="closed",
            status="not certified",
            rationale="Generic date/time literal rendering is not guaranteed.",
        ),
        "json_type": Capability(
            server="native JSON/BSON",
            dialect="Informix-specific comparator",
            suite="closed",
            status="generic JSON contract not claimed",
            rationale=(
                "Native document storage exists, but SQLAlchemy's complete "
                "generic JSON indexing/casting contract is not implemented."
            ),
        ),
        "regexp_match": Capability(
            server="function-dependent",
            dialect="not implemented",
            suite="closed",
            status="unsupported",
            rationale="No portable SQLAlchemy regexp-match operator mapping.",
        ),
        "regexp_replace": Capability(
            server="function-dependent",
            dialect="not implemented",
            suite="closed",
            status="unsupported",
            rationale="No portable SQLAlchemy regexp-replace operator mapping.",
        ),
        "tuple_in": Capability(
            server="configuration/version dependent",
            dialect="not certified",
            suite="closed",
            status="unsupported contract",
            rationale="Row-value IN semantics are intentionally not advertised.",
        ),
        "fetch_percent": Capability(
            server="no mapped syntax",
            dialect="explicit compile error",
            suite="closed",
            status="unsupported",
            rationale="FETCH ... PERCENT is rejected rather than miscompiled.",
        ),
        "fetch_ties": Capability(
            server="no mapped syntax",
            dialect="explicit compile error",
            suite="closed",
            status="unsupported",
            rationale="FETCH ... WITH TIES is rejected rather than miscompiled.",
        ),
        "identity": Capability(
            server="SERIAL/sequence facilities",
            dialect="SQLAlchemy Identity emulation",
            suite="partial",
            status="emulated, not native-identity equivalent",
            rationale=(
                "Identity semantics use SERIAL or managed sequences and must "
                "not be confused with every native GENERATED IDENTITY feature."
            ),
        ),
    }
)


def capability_rows() -> tuple[tuple[str, Capability], ...]:
    """Return deterministic rows for documentation and validation."""
    return tuple(sorted(CAPABILITY_MATRIX.items()))
