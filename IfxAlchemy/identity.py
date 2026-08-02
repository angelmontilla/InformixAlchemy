# SPDX-License-Identifier: Apache-2.0
"""Private sequence emulation for SQLAlchemy ``Identity`` columns.

Informix 14.10/15.0 has native ``SERIAL`` types and independent sequence
objects, but it does not expose SQLAlchemy's full ``Identity`` contract as a
single column clause.  The dialect therefore keeps legacy implicit
``autoincrement=True`` columns on SERIAL/SERIAL8 while normalizing an explicit
``Identity`` object to:

* a normal INTEGER/BIGINT column;
* one deterministic private sequence;
* a Python-side Sequence default that SQLAlchemy pre-executes on INSERT;
* before-create / after-drop lifecycle hooks; and
* catalog-based reflection of the Identity options.

The sequence name is derived from table and column names, so reflection can
find it without a side metadata table.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
import hashlib
import re
from typing import Any

from sqlalchemy import event
from sqlalchemy import schema as sa_schema

IDENTITY_SEQUENCE_INFO_KEY = "_ifx_identity_sequences"
IDENTITY_SEQUENCE_COLUMN_INFO_KEY = "_ifx_identity_sequence"
IDENTITY_SEQUENCE_MANAGED_ATTR = "_ifx_identity_managed"
INFORMIX_IDENTIFIER_MAX_LENGTH = 128

__all__ = [
    "IDENTITY_SEQUENCE_COLUMN_INFO_KEY",
    "IDENTITY_SEQUENCE_INFO_KEY",
    "identity_requires_sequence",
    "identity_sequence_for_column",
    "identity_sequence_name",
    "identity_uses_native_serial",
    "install_identity_event_listeners",
    "iter_ifx_identity_sequences",
    "register_ifx_identity_sequence",
    "register_ifx_identity_sequences_for_table",
    "_drop_ifx_identity_sequences",
    "_prepare_ifx_identity_sequences",
]

_IDENTITY_NAME_TOKEN_RE = re.compile(r"[^0-9A-Za-z_$]+")


def _column_identity(column: sa_schema.Column[Any]) -> Any | None:
    return getattr(column, "identity", None)


def identity_uses_native_serial(column: sa_schema.Column[Any]) -> bool:
    """Return whether an ``Identity`` needs no private sequence.

    The compatibility contract retained by the dialect treats the two exact
    default forms, ``Identity()`` and ``Identity(start=1, increment=1)``, as
    the native/default path.  Any partially specified or extended contract is
    materialized as a private Informix sequence so no option is silently lost.

    This predicate deliberately does not classify a column without
    ``Identity`` as an identity column.
    """
    identity = _column_identity(column)
    if identity is None:
        return False

    default_shape = (
        (identity.start is None and identity.increment is None)
        or (identity.start == 1 and identity.increment == 1)
    )
    if not default_shape:
        return False

    return (
        not bool(identity.always)
        and identity.minvalue is None
        and identity.maxvalue is None
        and identity.cache is None
        and identity.nominvalue in (None, False)
        and identity.nomaxvalue in (None, False)
        and identity.cycle in (None, False)
        and identity.order in (None, False)
        and getattr(identity, "on_null", None) in (None, False)
    )


def identity_requires_sequence(column: sa_schema.Column[Any]) -> bool:
    return (
        _column_identity(column) is not None
        and not identity_uses_native_serial(column)
    )


def _identity_name_token(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    token = _IDENTITY_NAME_TOKEN_RE.sub("_", str(value)).strip("_").lower()
    return token or fallback


def identity_sequence_name(
    table_or_column: sa_schema.Column[Any] | Any,
    column_name: Any | None = None,
    schema: Any | None = None,
) -> str:
    """Return the deterministic private sequence name.

    The helper accepts either a SQLAlchemy ``Column`` or the explicit
    ``(table_name, column_name, schema)`` form used by reflection.  ``schema``
    is deliberately not included in the readable part because the sequence is
    already owner-qualified; it participates in the digest for overlong names.
    """
    if isinstance(table_or_column, sa_schema.Column):
        column = table_or_column
        table = getattr(column, "table", None)
        table_name = getattr(table, "name", None)
        column_name = getattr(column, "name", None)
        schema = getattr(table, "schema", None)
    else:
        table_name = table_or_column

    table_token = _identity_name_token(table_name, "table")
    column_token = _identity_name_token(column_name, "column")
    candidate = f"{table_token}_{column_token}_identity_seq"

    # Keep readable names for ordinary lower-case Informix identifiers.  For
    # quoted, mixed-case, whitespace-bearing, or otherwise special names use a
    # canonical hash.  Reflection can then reproduce the same identifier after
    # catalog case folding and quote normalization.
    simple_identifier = re.compile(r"^[a-z_][a-z0-9_$]*$")
    table_text = "" if table_name is None else str(table_name)
    column_text = "" if column_name is None else str(column_name)
    if (
        simple_identifier.fullmatch(table_text)
        and simple_identifier.fullmatch(column_text)
        and len(candidate) <= INFORMIX_IDENTIFIER_MAX_LENGTH
    ):
        return candidate

    canonical_name = "|".join(
        str(part).strip().casefold()
        for part in (schema, table_name, column_name)
        if part not in (None, "")
    )
    digest = hashlib.sha256(canonical_name.encode("utf-8")).hexdigest()[:24]
    return f"ifx_id_{digest}_identity_seq"


def _normalized_sequence_options(identity: Any) -> dict[str, Any]:
    start = 1 if identity.start is None else int(identity.start)
    increment = 1 if identity.increment is None else int(identity.increment)
    if increment == 0:
        raise ValueError("Informix Identity increment cannot be zero")

    minvalue = identity.minvalue
    maxvalue = identity.maxvalue

    # Informix requires MINVALUE < START and MAXVALUE > START.  SQLAlchemy's
    # generic Identity accepts equality, so normalize only the invalid bound
    # while preserving the caller's direction and all other values.
    if minvalue is not None:
        minvalue = int(minvalue)
        if minvalue >= start:
            minvalue = start - 1
    if maxvalue is not None:
        maxvalue = int(maxvalue)
        if maxvalue <= start:
            maxvalue = start + 1

    cache = identity.cache
    if cache is not None:
        cache = int(cache)
        if cache < 2:
            # Informix CACHE requires at least two values.  ``None`` leaves the
            # server default in place and is preferable to emitting invalid SQL.
            cache = None

    return {
        "start": start,
        "increment": increment,
        "minvalue": minvalue,
        "maxvalue": maxvalue,
        "nominvalue": identity.nominvalue,
        "nomaxvalue": identity.nomaxvalue,
        "cycle": bool(identity.cycle),
        "cache": cache,
        "order": identity.order,
    }


def _attach_sequence_default(
    column: sa_schema.Column[Any], sequence: sa_schema.Sequence
) -> None:
    """Attach *sequence* as a client-side default without metadata DDL.

    Registering the sequence in ``MetaData._sequences`` would make SQLAlchemy
    emit it once through generic metadata traversal and a second time through
    the private lifecycle hook.  Assigning the default directly gives the DML
    compiler the required pre-executed default while the lifecycle hook remains
    the single DDL owner.
    """
    existing = getattr(column, "default", None)
    if existing is not None and existing is not sequence:
        raise ValueError(
            f"Identity column {column!s} already has an incompatible default"
        )
    column.default = sequence
    sequence.column = column


def _cached_identity_sequence(
    column: sa_schema.Column[Any],
) -> sa_schema.Sequence | None:
    cached = column.info.get(IDENTITY_SEQUENCE_COLUMN_INFO_KEY)
    if cached is None:
        return None
    if not isinstance(cached, sa_schema.Sequence):
        raise TypeError(
            "Cached Informix identity sequence must be a Sequence"
        )
    return cached


def _materialize_ifx_identity_sequence(
    column: sa_schema.Column[Any],
) -> sa_schema.Sequence | None:
    """Materialize the implementation sequence for any explicit Identity.

    SQLAlchemy represents ``Identity`` as a server-side default, but Informix
    needs a client-side sequence default so INSERT can pre-execute NEXTVAL.
    The public compatibility helper intentionally reports no sequence for the
    default ``Identity()`` shape until DDL/DML compilation needs one.  This
    private helper performs that late materialization without changing the
    public capability predicates.
    """
    if not isinstance(column, sa_schema.Column):
        raise TypeError(
            "_materialize_ifx_identity_sequence() requires a SQLAlchemy Column"
        )

    identity = _column_identity(column)
    if identity is None:
        return None

    cached = _cached_identity_sequence(column)
    if cached is not None:
        _attach_sequence_default(column, cached)
        table = getattr(column, "table", None)
        if table is not None:
            return register_ifx_identity_sequence(table, cached)
        return cached

    table = getattr(column, "table", None)
    options = _normalized_sequence_options(identity)
    sequence = sa_schema.Sequence(
        identity_sequence_name(column),
        schema=getattr(table, "schema", None),
        data_type=column.type,
        **options,
    )
    setattr(sequence, IDENTITY_SEQUENCE_MANAGED_ATTR, True)
    column.info[IDENTITY_SEQUENCE_COLUMN_INFO_KEY] = sequence
    _attach_sequence_default(column, sequence)

    if table is not None:
        return register_ifx_identity_sequence(table, sequence)
    return sequence


def identity_sequence_for_column(
    column: sa_schema.Column[Any],
) -> sa_schema.Sequence | None:
    """Return the public private-sequence contract for an Identity column.

    The default ``Identity()`` and ``Identity(start=1, increment=1)`` shapes
    remain classified as the dialect's native/default path and therefore
    return ``None`` without registry side effects.  DDL and DML compilation
    use :func:`_materialize_ifx_identity_sequence` internally to attach the
    client-side default Informix needs while preserving this public contract.
    """
    if not isinstance(column, sa_schema.Column):
        raise TypeError(
            "identity_sequence_for_column() requires a SQLAlchemy Column"
        )

    identity = _column_identity(column)
    if identity is None or identity_uses_native_serial(column):
        return None

    return _materialize_ifx_identity_sequence(column)


def _materialize_ifx_identity_sequences_for_table(
    table: sa_schema.Table,
) -> tuple[sa_schema.Sequence, ...]:
    for column in table.columns:
        _materialize_ifx_identity_sequence(column)
    return iter_ifx_identity_sequences(table)


def register_ifx_identity_sequences_for_table(
    table: sa_schema.Table,
) -> tuple[sa_schema.Sequence, ...]:
    for column in table.columns:
        identity_sequence_for_column(column)
    return iter_ifx_identity_sequences(table)


def _sequence_key(sequence: sa_schema.Sequence) -> tuple[str | None, str]:
    schema = None if sequence.schema is None else str(sequence.schema)
    return schema, str(sequence.name)


def _coerce_identity_sequence(
    table: sa_schema.Table,
    value: sa_schema.Sequence | str | Mapping[str, Any],
) -> sa_schema.Sequence:
    if isinstance(value, sa_schema.Sequence):
        return value
    if isinstance(value, str):
        return sa_schema.Sequence(value, schema=table.schema)
    if isinstance(value, Mapping):
        name = value.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                "An Informix identity sequence mapping requires a non-empty "
                "string 'name'"
            )
        return sa_schema.Sequence(
            name,
            schema=value.get("schema", table.schema),
        )
    raise TypeError(
        "Informix identity sequence entries must be Sequence, str, or mapping; "
        f"received {type(value).__name__}"
    )


def register_ifx_identity_sequence(
    table: sa_schema.Table,
    sequence: sa_schema.Sequence | str | Mapping[str, Any],
) -> sa_schema.Sequence:
    normalized = _coerce_identity_sequence(table, sequence)
    registry = table.info.setdefault(IDENTITY_SEQUENCE_INFO_KEY, [])
    existing = {
        _sequence_key(_coerce_identity_sequence(table, item))
        for item in registry
    }
    if _sequence_key(normalized) not in existing:
        registry.append(normalized)
    return normalized


def iter_ifx_identity_sequences(
    table: sa_schema.Table,
) -> tuple[sa_schema.Sequence, ...]:
    raw: Iterable[Any] = table.info.get(IDENTITY_SEQUENCE_INFO_KEY, ())
    result: list[sa_schema.Sequence] = []
    seen: set[tuple[str | None, str]] = set()
    for item in raw:
        sequence = _coerce_identity_sequence(table, item)
        key = _sequence_key(sequence)
        if key in seen:
            continue
        seen.add(key)
        result.append(sequence)
    return tuple(result)


def _is_informix_connection(connection: Any) -> bool:
    dialect = getattr(connection, "dialect", None)
    name = getattr(dialect, "name", None)
    # Small recording doubles used by the unit tests intentionally expose no
    # dialect.  Treat only an explicit non-Informix name as a reason to skip.
    return name in (None, "informix")


def _prepare_ifx_identity_sequences(
    table: sa_schema.Table,
    connection: Any,
    **kw: Any,
) -> None:
    """Recreate private sequences immediately before their owning table."""
    del kw
    if not _is_informix_connection(connection):
        return

    sequences = _materialize_ifx_identity_sequences_for_table(table)
    for sequence in sequences:
        if getattr(sequence, IDENTITY_SEQUENCE_MANAGED_ATTR, False):
            # Auto-generated private Identity sequences must match the current
            # Identity options exactly, even after an interrupted prior DDL.
            connection.execute(sa_schema.DropSequence(sequence, if_exists=True))
            connection.execute(sa_schema.CreateSequence(sequence))
        else:
            # Explicitly registered sequences are user-owned declarations.
            # Never destroy them during table preparation.
            connection.execute(
                sa_schema.CreateSequence(sequence, if_not_exists=True)
            )


def _drop_ifx_identity_sequences(
    table: sa_schema.Table,
    connection: Any,
    **kw: Any,
) -> None:
    del kw
    if not _is_informix_connection(connection):
        return

    sequences = _materialize_ifx_identity_sequences_for_table(table)
    for sequence in reversed(sequences):
        connection.execute(sa_schema.DropSequence(sequence, if_exists=True))


def install_identity_event_listeners() -> None:
    if not event.contains(
        sa_schema.Table,
        "before_create",
        _prepare_ifx_identity_sequences,
    ):
        event.listen(
            sa_schema.Table,
            "before_create",
            _prepare_ifx_identity_sequences,
            propagate=True,
        )

    if not event.contains(
        sa_schema.Table,
        "after_drop",
        _drop_ifx_identity_sequences,
    ):
        event.listen(
            sa_schema.Table,
            "after_drop",
            _drop_ifx_identity_sequences,
            propagate=True,
        )
