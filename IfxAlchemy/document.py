# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Angel Montilla
"""Informix native JSON and BSON document types.

The IBM Informix ODBC driver has exposed JSON/BSON differently across client
SDK versions.  The types in this module therefore keep the wire transport
explicit and configurable instead of treating BSON as an unconditional
``LargeBinary`` alias.
"""
from __future__ import annotations

import json as _stdlib_json
from typing import Any, Callable

from sqlalchemy import cast, func, literal, type_coerce
from sqlalchemy import exc
from sqlalchemy import types as sa_types
from sqlalchemy.sql import elements


Serializer = Callable[[Any], Any]
Deserializer = Callable[[Any], Any]


INFORMIX_DOCUMENT_MAX_BYTES = 32 * 1024


def _validate_text_document(serialized: Any, *, type_name: str) -> str:
    """Validate Informix native JSON/BSON textual input.

    Informix 14.10+ accepts JSON *documents*, whose top-level value is an
    object delimited by braces.  Scalar values (including JSON ``null``) and
    top-level arrays are not valid opaque-type input.  Arrays remain fully
    supported as values nested inside a document.

    The server documents a 32 KiB maximum document size.  Validate the UTF-8
    external representation here so invalid values fail as SQLAlchemy bind
    errors instead of opaque ``json_in`` UDR failures during executemany.
    """
    if not isinstance(serialized, str):
        raise TypeError(
            f"Informix {type_name} textual transport requires the configured "
            "serializer/encoder to return str"
        )

    stripped = serialized.strip()
    if not stripped.startswith("{") or not stripped.endswith("}"):
        raise ValueError(
            f"Informix {type_name} requires a top-level JSON document object; "
            "top-level scalars, arrays, and JSON null are not supported. "
            "Use SQL NULL via sqlalchemy.null() or none_as_null=True, and "
            "store arrays/null as fields inside an object."
        )

    encoded_size = len(serialized.encode("utf-8"))
    if encoded_size > INFORMIX_DOCUMENT_MAX_BYTES:
        raise ValueError(
            f"Informix {type_name} document is {encoded_size} bytes; the "
            f"native maximum is {INFORMIX_DOCUMENT_MAX_BYTES} bytes"
        )

    return serialized


def _coerce_driver_bytes(value: Any) -> Any:
    """Normalize mutable DBAPI binary wrappers without guessing their format."""
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    return value


def _coerce_json_text(value: Any) -> Any:
    value = _coerce_driver_bytes(value)
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _default_json_deserializer(value: Any) -> Any:
    return _stdlib_json.loads(_coerce_json_text(value))


def _document_literal_processor(
    dialect,
    serializer: Serializer,
    *,
    type_name: str,
):
    """Build a safe quoted textual literal processor.

    Informix performs the native cast through ``bind_expression``.  This
    processor only quotes the serialized JSON text and deliberately rejects
    binary payloads, for which inline SQL literals are driver/version
    dependent and therefore unsafe.
    """
    string_processor = sa_types.String().literal_processor(dialect)
    if string_processor is None:
        return None

    def process(value: Any) -> str:
        try:
            serialized = _validate_text_document(
                serializer(value),
                type_name=type_name,
            )
        except (TypeError, ValueError) as error:
            raise exc.CompileError(str(error)) from error
        return string_processor(serialized)

    return process


def _null_aware_literal_processor(
    dialect,
    serializer: Serializer,
    *,
    type_name: str,
    none_as_null: bool,
    null_sentinel: Any,
):
    textual_processor = _document_literal_processor(
        dialect,
        serializer,
        type_name=type_name,
    )
    if textual_processor is None:
        return None

    def process(value: Any) -> str:
        if value is null_sentinel:
            return textual_processor(None)
        if isinstance(value, elements.Null) or (
            value is None and none_as_null
        ):
            return "NULL"
        return textual_processor(value)

    return process


class JSON(sa_types.JSON):
    """Native Informix JSON opaque type.

    ``None`` follows SQLAlchemy's ``none_as_null`` contract.  Informix
    native JSON stores top-level document objects only, so whole-column JSON
    ``null`` (``JSON.NULL`` or ``None`` with ``none_as_null=False``) cannot be
    represented and raises a bind error.  Use :func:`sqlalchemy.null` or
    ``JSON(none_as_null=True)`` for SQL ``NULL``.  JSON null values remain
    supported inside document fields.
    """

    __visit_name__ = "JSON"
    cache_ok = True

    class Comparator(sa_types.TypeEngine.Comparator):
        """Only expose operations verified for native Informix JSON."""

        def as_bson(self):
            return cast(self.expr, BSON())

    comparator_factory = Comparator

    def bind_expression(self, bindvalue):
        # Informix validates and stores JSON through an explicit native cast.
        return cast(bindvalue, self)

    def bind_processor(self, dialect):
        serializer = getattr(dialect, "_json_serializer", None) or _stdlib_json.dumps

        def process(value: Any) -> Any:
            if value is self.NULL:
                value = None
            elif isinstance(value, elements.Null) or (
                value is None and self.none_as_null
            ):
                return None
            return _validate_text_document(
                serializer(value),
                type_name="JSON",
            )

        return process

    def result_processor(self, dialect, coltype):
        deserializer = (
            getattr(dialect, "_json_deserializer", None)
            or _default_json_deserializer
        )

        def process(value: Any) -> Any:
            if value is None:
                return None
            decoded = deserializer(_coerce_json_text(value))
            return self.NULL if decoded is None else decoded

        return process

    def literal_processor(self, dialect):
        serializer = getattr(dialect, "_json_serializer", None) or _stdlib_json.dumps
        return _null_aware_literal_processor(
            dialect,
            serializer,
            type_name="JSON",
            none_as_null=self.none_as_null,
            null_sentinel=self.NULL,
        )

    def get_dbapi_type(self, dbapi):
        # Informix opaque-type input casts consume the external character
        # representation through LVARCHAR.  With pyodbc, SQL_LONGVARCHAR is
        # mapped by the IBM driver to Informix TEXT, and TEXT cannot be cast
        # to JSON (-9634).  SQL_VARCHAR is the ODBC descriptor for Informix
        # LVARCHAR; the dialect supplies column-size 0 via setinputsizes so
        # pyodbc keeps this descriptor for large document parameters.
        return getattr(
            dbapi,
            "SQL_VARCHAR",
            getattr(dbapi, "SQL_LONGVARCHAR", getattr(dbapi, "STRING", None)),
        )


class BSON(sa_types.JSON):
    """Native Informix BSON opaque type.

    Parameters
    ----------
    none_as_null:
        Same SQL NULL versus document-null behavior as :class:`JSON`.
    transport:
        ``"json"`` (default) serializes a top-level document object to JSON
        text and emits ``CAST(CAST(? AS JSON) AS BSON)``.  Whole-column BSON
        null and top-level arrays/scalars are not representable by Informix
        native BSON; nested nulls and arrays are supported.  ``"binary"``
        passes the configured encoder output as bytes and emits a direct BSON
        cast.  This explicit
        setting avoids assuming that every Informix ODBC/CSDK combination
        exposes BSON as ``LargeBinary``.
    """

    __visit_name__ = "BSON"
    cache_ok = True

    class Comparator(sa_types.TypeEngine.Comparator):
        """Verified Informix BSON operators and processing functions."""

        def get(self, field: Any, new_field: Any | None = None):
            return bson_get(self.expr, field, new_field)

        def update(self, update_document: Any):
            return bson_update(self.expr, update_document)

        def size(self, field: Any | None = None):
            return bson_size(self.expr, field)

        def as_json(self):
            return cast(self.expr, JSON())

    comparator_factory = Comparator

    def __init__(self, none_as_null: bool = False, transport: str = "json"):
        super().__init__(none_as_null=none_as_null)
        normalized_transport = str(transport).strip().lower()
        if normalized_transport not in {"json", "binary"}:
            raise exc.ArgumentError(
                "Informix BSON transport must be 'json' or 'binary'"
            )
        self.transport = normalized_transport

    def bind_expression(self, bindvalue):
        if self.transport == "binary":
            return cast(bindvalue, self)
        return cast(cast(bindvalue, JSON(none_as_null=self.none_as_null)), self)

    def column_expression(self, colexpr):
        """Expose BSON as readable JSON unless binary transport is explicit.

        Informix requires a BSON-to-JSON cast to display BSON through SQL.
        More importantly, different CSDK/ODBC versions can expose a raw BSON
        column as binary bytes or as text.  The default ``json`` transport
        therefore asks the server for JSON and avoids guessing the driver's
        raw BSON representation.  Applications that have diagnosed and
        configured a binary codec can opt into ``transport='binary'`` and
        receive the uncast DBAPI value.
        """
        if self.transport == "binary":
            return colexpr
        json_expression = cast(
            colexpr,
            JSON(none_as_null=self.none_as_null),
        )
        # Keep the BSON type on the client side so ``bson_decoder`` and the
        # BSON SQL-NULL/document-null contract still process the JSON text
        # returned by the server-side cast.
        return type_coerce(json_expression, self)

    def bind_processor(self, dialect):
        encoder = getattr(dialect, "_bson_encoder", None)
        if encoder is None:
            if self.transport == "binary":
                def missing_binary_encoder(value):
                    if value is None or isinstance(value, elements.Null):
                        return None
                    if value is self.NULL:
                        raise TypeError(
                            "BSON(transport='binary') requires bson_encoder "
                            "to encode BSON.NULL/document null"
                        )
                    value = _coerce_driver_bytes(value)
                    if isinstance(value, bytes):
                        return value
                    raise TypeError(
                        "BSON(transport='binary') requires bytes-like values "
                        "or a dialect bson_encoder"
                    )

                encoder = missing_binary_encoder
            else:
                encoder = (
                    getattr(dialect, "_json_serializer", None)
                    or _stdlib_json.dumps
                )

        def process(value: Any) -> Any:
            if value is self.NULL:
                raise ValueError(
                    "Informix BSON cannot represent a whole-column document "
                    "null; use sqlalchemy.null() or BSON(none_as_null=True), "
                    "or store null inside a document field"
                )
            if isinstance(value, elements.Null) or (
                value is None and self.none_as_null
            ):
                return None
            if value is None:
                raise ValueError(
                    "Informix BSON cannot represent a whole-column document "
                    "null; use sqlalchemy.null() or BSON(none_as_null=True), "
                    "or store null inside a document field"
                )
            encoded = encoder(value)
            encoded = _coerce_driver_bytes(encoded)
            if self.transport == "binary" and not isinstance(encoded, bytes):
                raise TypeError(
                    "Informix BSON binary transport encoder must return bytes"
                )
            if self.transport == "json":
                return _validate_text_document(
                    encoded,
                    type_name="BSON",
                )
            return encoded

        return process

    def result_processor(self, dialect, coltype):
        decoder = getattr(dialect, "_bson_decoder", None)
        if decoder is None and self.transport == "json":
            decoder = (
                getattr(dialect, "_json_deserializer", None)
                or _default_json_deserializer
            )

        def process(value: Any) -> Any:
            if value is None:
                return None
            value = (
                _coerce_json_text(value)
                if self.transport == "json"
                else _coerce_driver_bytes(value)
            )
            if decoder is None:
                return value
            decoded = decoder(value)
            return self.NULL if decoded is None else decoded

        return process

    def literal_processor(self, dialect):
        if self.transport == "binary":
            return None
        encoder = (
            getattr(dialect, "_bson_encoder", None)
            or getattr(dialect, "_json_serializer", None)
            or _stdlib_json.dumps
        )
        return _null_aware_literal_processor(
            dialect,
            encoder,
            type_name="BSON",
            none_as_null=self.none_as_null,
            null_sentinel=self.NULL,
        )

    def get_dbapi_type(self, dbapi):
        if self.transport == "binary":
            # Keep the parameter in the variable-binary family.  Binding it
            # as SQL_LONGVARBINARY can make the Informix driver expose a BLOB,
            # for which a native BSON input cast is not guaranteed.
            return getattr(
                dbapi,
                "SQL_VARBINARY",
                getattr(
                    dbapi,
                    "SQL_LONGVARBINARY",
                    getattr(dbapi, "BINARY", None),
                ),
            )

        # The default BSON transport is JSON text and therefore follows the
        # same LVARCHAR input contract as JSON before the JSON-to-BSON cast.
        return getattr(
            dbapi,
            "SQL_VARCHAR",
            getattr(dbapi, "SQL_LONGVARCHAR", getattr(dbapi, "STRING", None)),
        )


class _BSONUpdateDocument(JSON):
    """Textual update document accepted by native ``BSON_UPDATE``.

    Informix provides overloaded ``BSON_UPDATE`` routines.  Casting a bound
    update document to the opaque JSON type makes overload resolution
    ambiguous on supported 14.10/15.x servers (SQL error -9700).  The native
    SQL contract and documented examples pass the update document as its
    external LVARCHAR representation instead.

    Reuse :class:`JSON` serialization, validation, literal rendering and ODBC
    ``SQL_VARCHAR`` typing, but deliberately suppress the normal JSON bind
    cast so SQLAlchemy emits ``BSON_UPDATE(column, ?)``.
    """

    cache_ok = True

    def bind_expression(self, bindvalue):
        return bindvalue


def _coerce_bson_update_argument(value: Any):
    if hasattr(value, "_compiler_dispatch"):
        return value
    return literal(value, type_=_BSONUpdateDocument())


def bson_get(bson_expression: Any, field: Any, new_field: Any | None = None):
    """Return Informix ``BSON_GET`` for a field or renamed field.

    Informix returns a BSON *field-value document*, not the scalar field
    value.  For a multilevel path such as ``"customer.name"``, the default
    returned document is ``{"name": ...}``, using the leaf field name.  Pass
    ``new_field`` when a deterministic alternative key is required.
    """
    args = [bson_expression, field]
    if new_field is not None:
        args.append(new_field)
    return func.BSON_GET(*args, type_=BSON())


def bson_update(bson_expression: Any, update_document: Any):
    """Return native Informix ``BSON_UPDATE``.

    Python update mappings are serialized as validated JSON text and bound as
    the external LVARCHAR representation.  They are intentionally *not* cast
    to JSON: Informix 14.10+ exposes multiple BSON_UPDATE overloads and an
    opaque JSON argument can produce ambiguous-routine error ``-9700``.
    """
    return func.BSON_UPDATE(
        bson_expression,
        _coerce_bson_update_argument(update_document),
        type_=BSON(),
    )


def bson_size(bson_expression: Any, field: Any | None = None):
    """Return the native Informix BSON storage size in bytes.

    Informix exposes ``BSON_SIZE`` with a mandatory second argument.  An
    empty field name requests the size of the complete BSON document; a
    non-empty name requests the size of that field-value pair.  Emitting the
    tempting one-argument form cannot be resolved by Informix 14.10/15.x and
    fails with SQL error ``-674``.
    """
    native_field = "" if field is None else field
    return func.BSON_SIZE(
        bson_expression,
        native_field,
        type_=sa_types.Integer(),
    )


def gen_bson(
    row_expression: Any,
    keep_nulls: bool | int = False,
    skip_id: bool | int = False,
):
    """Return Informix ``genBSON(row, keep_nulls, skip_id)``."""
    keep_nulls_value = int(bool(keep_nulls))
    skip_id_value = int(bool(skip_id))
    return func.genBSON(
        row_expression,
        keep_nulls_value,
        skip_id_value,
        type_=BSON(),
    )


__all__ = (
    "BSON",
    "JSON",
    "bson_get",
    "bson_size",
    "bson_update",
    "gen_bson",
)
