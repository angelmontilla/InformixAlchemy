# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Angel Montilla
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
"""Declarative support for native Informix complex data types.

The IBM/HCL Informix ODBC driver exposes LIST, SET, MULTISET and ROW values
through their documented external text representation.  This module provides
immutable SQLAlchemy types, a recursive parser/serializer for that external
representation, and executable DDL constructs for named ROW and DISTINCT
user-defined types.
"""

from __future__ import annotations

import datetime as _datetime
import decimal as _decimal
import re
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, Sequence

from sqlalchemy import exc
from sqlalchemy import literal, types as sa_types
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import cast
from sqlalchemy.sql.ddl import ExecutableDDLElement


_COMPLEX_TYPE_CODES = {
    19: "SET",
    20: "MULTISET",
    21: "LIST",
    22: "ROW",
}
_COMPLEX_TYPE_NAMES = frozenset(_COMPLEX_TYPE_CODES.values())
_IDENTIFIER_RE = re.compile(r"^[^\x00-\x1f]+$")


def _type_cache_key(type_: sa_types.TypeEngine[Any]) -> Any:
    key = getattr(type_, "_static_cache_key", None)
    if key is not None:
        return key
    return (type(type_), repr(type_))


def _coerce_type(type_: Any) -> sa_types.TypeEngine[Any]:
    try:
        coerced = sa_types.to_instance(type_)
    except Exception as error:  # pragma: no cover - defensive SQLAlchemy API guard
        raise exc.ArgumentError(f"Invalid SQLAlchemy type {type_!r}") from error
    if not isinstance(coerced, sa_types.TypeEngine):
        raise exc.ArgumentError(f"Invalid SQLAlchemy type {type_!r}")
    return coerced


def _validate_identifier(value: Any, label: str, max_length: int = 128) -> str:
    if not isinstance(value, str):
        raise exc.ArgumentError(f"{label} must be a string identifier")
    if not value:
        raise exc.ArgumentError(f"{label} must not be empty")
    if len(value) > max_length:
        raise exc.ArgumentError(
            f"{label} exceeds the Informix limit of {max_length} characters"
        )
    if not _IDENTIFIER_RE.match(value):
        raise exc.ArgumentError(f"{label} contains control characters")
    if getattr(value, "quote", None) is not True and any(
        token in value for token in (".", ":", "@")
    ):
        raise exc.ArgumentError(
            f"{label} must be supplied as a structured identifier"
        )
    return value


def _qualified_type_name(preparer: Any, name: str, owner: str | None) -> str:
    rendered = preparer.quote(name)
    if owner:
        rendered = f"{preparer.quote(owner)}.{rendered}"
    return rendered


def _informix_type_token(type_: sa_types.TypeEngine[Any]) -> str:
    return str(
        getattr(type_, "__visit_name__", type(type_).__name__)
    ).upper()


def _validate_collection_element_type(
    element_type: sa_types.TypeEngine[Any],
) -> None:
    token = _informix_type_token(element_type)
    if token in {
        "SERIAL",
        "SERIAL8",
        "BIGSERIAL",
        "TEXT",
        "BYTE",
        "LARGE_BINARY",
        "LARGEBINARY",
    }:
        raise exc.ArgumentError(
            "Informix collection elements cannot use SERIAL, SERIAL8, "
            "BIGSERIAL, TEXT, or BYTE source types"
        )


def _validate_distinct_source_type(
    source_type: sa_types.TypeEngine[Any],
) -> None:
    if _informix_type_token(source_type) in {
        "SERIAL",
        "SERIAL8",
        "BIGSERIAL",
    }:
        raise exc.ArgumentError(
            "Informix DISTINCT types cannot use SERIAL, SERIAL8, or "
            "BIGSERIAL as their source type"
        )


@dataclass(frozen=True, slots=True)
class RowField:
    """One field of a native Informix ROW type."""

    name: str
    type_: sa_types.TypeEngine[Any]
    nullable: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _validate_identifier(self.name, "ROW field"))
        object.__setattr__(self, "type_", _coerce_type(self.type_))
        if not isinstance(self.nullable, bool):
            raise exc.ArgumentError("ROW field nullable must be a boolean")

    def __hash__(self) -> int:
        return hash((self.name, _type_cache_key(self.type_), self.nullable))


@dataclass(frozen=True, slots=True)
class RowValue:
    """Immutable parsed ROW value with positional and name-based access."""

    fields: tuple[str, ...]
    values: tuple[Any, ...]
    type_name: str | None = None

    def __post_init__(self) -> None:
        if len(self.fields) != len(self.values):
            raise ValueError("ROW field and value counts differ")

    def __iter__(self) -> Iterator[Any]:
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def __getitem__(self, key: int | str) -> Any:
        if isinstance(key, int):
            return self.values[key]
        try:
            index = self.fields.index(key)
        except ValueError as error:
            raise KeyError(key) from error
        return self.values[index]

    def as_dict(self) -> dict[str, Any]:
        return dict(zip(self.fields, self.values))


@dataclass(frozen=True, slots=True)
class _ConstructorValue:
    name: str
    values: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class _QuotedValue:
    value: str


class _ComplexValueParser:
    """Recursive-descent parser for Informix complex text representations."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.length = len(text)
        self.position = 0

    def parse(self) -> Any:
        self._skip_space()
        value = self._parse_value(stop_chars=frozenset())
        self._skip_space()
        if self.position != self.length:
            raise ValueError(
                "Unexpected trailing data in Informix complex value at "
                f"character {self.position + 1}"
            )
        return value

    def _skip_space(self) -> None:
        while self.position < self.length and self.text[self.position].isspace():
            self.position += 1

    def _parse_value(self, stop_chars: frozenset[str]) -> Any:
        self._skip_space()
        if self.position >= self.length:
            return ""

        current = self.text[self.position]
        if current in {"'", '"'}:
            return _QuotedValue(self._parse_quoted())

        identifier_position = self.position
        identifier = self._parse_identifier()
        if identifier:
            self._skip_space()
            if self.position < self.length:
                opener = self.text[self.position]
                upper_identifier = identifier.upper()
                if opener == "{" and upper_identifier in {
                    "LIST",
                    "SET",
                    "MULTISET",
                }:
                    return self._parse_constructor(upper_identifier, "{", "}")
                if opener == "(":
                    # ROW is used for anonymous rows.  Named rows can be
                    # returned by the driver as type_name(...).
                    return self._parse_constructor(identifier, "(", ")")

        self.position = identifier_position
        return self._parse_bare(stop_chars)

    def _parse_identifier(self) -> str:
        start = self.position
        while self.position < self.length:
            character = self.text[self.position]
            if character.isalnum() or character in {"_", "$", "."}:
                self.position += 1
                continue
            break
        return self.text[start:self.position]

    def _parse_constructor(self, name: str, opener: str, closer: str) -> Any:
        if self.text[self.position] != opener:
            raise AssertionError("parser constructor opener mismatch")
        self.position += 1
        values: list[Any] = []
        self._skip_space()
        if self.position < self.length and self.text[self.position] == closer:
            self.position += 1
            return _ConstructorValue(name, tuple())

        while True:
            values.append(self._parse_value(frozenset({",", closer})))
            self._skip_space()
            if self.position >= self.length:
                raise ValueError(
                    f"Unterminated {name} constructor in Informix complex value"
                )
            character = self.text[self.position]
            if character == closer:
                self.position += 1
                break
            if character != ",":
                raise ValueError(
                    "Expected ',' or closing delimiter in Informix complex "
                    f"value at character {self.position + 1}"
                )
            self.position += 1
        return _ConstructorValue(name, tuple(values))

    def _parse_quoted(self) -> str:
        quote = self.text[self.position]
        self.position += 1
        result: list[str] = []
        while self.position < self.length:
            character = self.text[self.position]
            if character == quote:
                if (
                    self.position + 1 < self.length
                    and self.text[self.position + 1] == quote
                ):
                    result.append(quote)
                    self.position += 2
                    continue
                self.position += 1
                return "".join(result)
            if character == "\\" and self.position + 1 < self.length:
                # Informix examples use backslash escaping in client source;
                # accepting it here makes the parser robust across ODBC
                # driver representations without making split-based guesses.
                self.position += 1
                result.append(self.text[self.position])
                self.position += 1
                continue
            result.append(character)
            self.position += 1
        raise ValueError("Unterminated quoted string in Informix complex value")

    def _parse_bare(self, stop_chars: frozenset[str]) -> str | None:
        start = self.position
        while self.position < self.length:
            character = self.text[self.position]
            if character in stop_chars:
                break
            self.position += 1
        token = self.text[start:self.position].strip()
        if token.upper() == "NULL":
            return None
        return token


def parse_complex_value(text: Any) -> Any:
    """Parse an Informix LIST/SET/MULTISET/ROW external representation.

    This public low-level parser returns constructor nodes and scalar tokens.
    SQLAlchemy type result processors additionally coerce those tokens through
    their declared element/field types.
    """

    if isinstance(text, memoryview):
        text = text.tobytes()
    if isinstance(text, bytearray):
        text = bytes(text)
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    if not isinstance(text, str):
        raise TypeError(
            "Informix complex values must be str or UTF-8 bytes, not "
            f"{type(text).__name__}"
        )
    return _ComplexValueParser(text.strip()).parse()


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _render_scalar(value: Any, type_: sa_types.TypeEngine[Any], dialect: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(type_, DISTINCT):
        return _render_scalar(value, type_.source_type, dialect)
    if isinstance(type_, _ComplexTypeBase):
        return type_._serialize(value, dialect)

    processor = type_.literal_processor(dialect)
    if processor is not None:
        try:
            rendered = processor(value)
        except Exception:
            rendered = None
        if rendered is not None:
            return str(rendered)

    if isinstance(value, str):
        return _quote_string(value)
    if isinstance(value, bool):
        return "'t'" if value else "'f'"
    if isinstance(value, (_datetime.date, _datetime.time, _datetime.datetime)):
        rendered = (
            value.isoformat(sep=" ")
            if isinstance(value, _datetime.datetime)
            else value.isoformat()
        )
        return _quote_string(rendered)
    if isinstance(value, _decimal.Decimal):
        return format(value, "f")
    if isinstance(value, (int, float)):
        return repr(value)
    raise TypeError(
        f"Cannot serialize {type(value).__name__} as Informix {type_!r}"
    )


def _coerce_scalar(
    value: Any,
    type_: sa_types.TypeEngine[Any],
    dialect: Any,
) -> Any:
    if value is None:
        return None
    if isinstance(type_, DISTINCT):
        return _coerce_scalar(value, type_.source_type, dialect)
    if isinstance(type_, _ComplexTypeBase):
        return type_._coerce_parsed(value, dialect)
    if isinstance(value, _QuotedValue):
        value = value.value
    if not isinstance(value, str):
        return value

    text = value.strip()
    if isinstance(type_, sa_types.Boolean):
        normalized = text.casefold()
        if normalized in {"t", "true", "1"}:
            return True
        if normalized in {"f", "false", "0"}:
            return False
    if isinstance(
        type_,
        (sa_types.SmallInteger, sa_types.Integer, sa_types.BigInteger),
    ):
        return int(text)
    if isinstance(type_, (sa_types.Numeric, sa_types.DECIMAL)):
        if getattr(type_, "asdecimal", True):
            return _decimal.Decimal(text)
        return float(text)
    if isinstance(type_, (sa_types.Float, sa_types.REAL)):
        return float(text)
    if isinstance(type_, sa_types.DateTime):
        return _datetime.datetime.fromisoformat(text)
    if isinstance(type_, sa_types.Date):
        return _datetime.date.fromisoformat(text)
    if isinstance(type_, sa_types.Time):
        return _datetime.time.fromisoformat(text)
    if isinstance(type_, sa_types.LargeBinary):
        return text.encode("utf-8")

    processor = type_.result_processor(dialect, None)
    if processor is not None:
        try:
            return processor(text)
        except (TypeError, ValueError):
            pass
    return text


def _hashable_collection_element(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_hashable_collection_element(item) for item in value)
    if isinstance(value, set):
        return frozenset(_hashable_collection_element(item) for item in value)
    if isinstance(value, dict):
        return tuple(
            sorted(
                (key, _hashable_collection_element(item))
                for key, item in value.items()
            )
        )
    if isinstance(value, RowValue):
        return RowValue(
            value.fields,
            tuple(_hashable_collection_element(item) for item in value.values),
            value.type_name,
        )
    return value


class _ComplexTypeBase(sa_types.TypeEngine[Any]):
    cache_ok = True
    hashable = False

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_public_frozen", False) and not name.startswith("_"):
            raise AttributeError(f"{type(self).__name__} instances are immutable")
        object.__setattr__(self, name, value)

    def _freeze(self) -> None:
        object.__setattr__(self, "_public_frozen", True)

    def __hash__(self) -> int:
        return hash(self._static_cache_key)

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self._static_cache_key == getattr(
            other, "_static_cache_key", None
        )

    def get_dbapi_type(self, dbapi: Any) -> Any:
        return getattr(
            dbapi,
            "SQL_VARCHAR",
            getattr(dbapi, "SQL_LONGVARCHAR", getattr(dbapi, "STRING", None)),
        )

    def bind_processor(self, dialect: Any):
        def process(value: Any) -> Any:
            if value is None:
                return None
            return self._serialize(value, dialect)

        return process

    def result_processor(self, dialect: Any, coltype: Any):
        def process(value: Any) -> Any:
            if value is None:
                return None
            return self._coerce_parsed(parse_complex_value(value), dialect)

        return process

    def literal_processor(self, dialect: Any):
        def process(value: Any) -> str:
            if value is None:
                return "NULL"
            return _quote_string(self._serialize(value, dialect))

        return process


class _CollectionType(_ComplexTypeBase):
    python_type = list

    def adapt(self, cls: type[sa_types.TypeEngine[Any]], **kw: Any):
        """Preserve collection metadata during SQLAlchemy adaptation.

        ``TypeEngine.adapt()`` reconstructs instances from positional
        constructor arguments.  ``element_nullable`` is keyword-only, so an
        explicit clone is required to keep the complete immutable type state.
        """
        if cls is type(self):
            adapted = cls(
                self.element_type,
                element_nullable=self.element_nullable,
            )
            adapted._variant_mapping = self._variant_mapping
            return adapted
        return super().adapt(cls, **kw)

    @property
    def _static_cache_key(self) -> tuple[Any, ...]:
        return (
            type(self),
            ("element_type", _type_cache_key(self.element_type)),
            ("element_nullable", self.element_nullable),
        )

    def __init__(
        self,
        element_type: Any,
        *,
        element_nullable: bool = False,
    ) -> None:
        if element_nullable is not False:
            raise exc.ArgumentError(
                "Informix collection elements must be declared NOT NULL"
            )
        element_type = _coerce_type(element_type)
        _validate_collection_element_type(element_type)
        object.__setattr__(self, "element_type", element_type)
        object.__setattr__(self, "element_nullable", False)
        self._freeze()

    def _serialize(self, value: Any, dialect: Any) -> str:
        if isinstance(value, (str, bytes, bytearray, memoryview)):
            parsed = parse_complex_value(value)
            coerced = self._coerce_parsed(parsed, dialect)
            value = coerced
        if not isinstance(value, Iterable):
            raise TypeError(
                f"Informix {self.__visit_name__} expects an iterable value"
            )

        rendered_values: list[str] = []
        seen: set[str] = set()
        for element in value:
            if element is None:
                raise ValueError(
                    f"Informix {self.__visit_name__} elements cannot be NULL"
                )
            rendered = _render_scalar(element, self.element_type, dialect)
            if self.__visit_name__ == "SET":
                if rendered in seen:
                    continue
                seen.add(rendered)
            rendered_values.append(rendered)

        if self.__visit_name__ == "SET" and isinstance(value, (set, frozenset)):
            rendered_values.sort()
        return f"{self.__visit_name__}{{{', '.join(rendered_values)}}}"

    def _coerce_parsed(self, parsed: Any, dialect: Any) -> Any:
        if isinstance(parsed, _QuotedValue):
            parsed = parse_complex_value(parsed.value)
        if not isinstance(parsed, _ConstructorValue):
            raise ValueError(
                f"Expected {self.__visit_name__} constructor, received {parsed!r}"
            )
        if parsed.name.upper() != self.__visit_name__:
            raise ValueError(
                f"Expected {self.__visit_name__}, received {parsed.name}"
            )
        values = [
            _coerce_scalar(item, self.element_type, dialect)
            for item in parsed.values
        ]
        if any(item is None for item in values):
            raise ValueError(
                f"Informix {self.__visit_name__} values cannot contain NULL elements"
            )
        if self.__visit_name__ == "SET":
            return {
                _hashable_collection_element(item)
                for item in values
            }
        return values


class LIST(_CollectionType):
    """Ordered Informix collection with duplicate elements allowed."""

    __visit_name__ = "LIST"


class SET(_CollectionType):
    """Unordered Informix collection with duplicate elements removed."""

    __visit_name__ = "SET"
    python_type = set


class MULTISET(_CollectionType):
    """Unordered Informix collection with duplicate elements retained."""

    __visit_name__ = "MULTISET"


class ROW(_ComplexTypeBase):
    """Named or anonymous native Informix ROW type."""

    __visit_name__ = "ROW"
    python_type = RowValue

    @property
    def _static_cache_key(self) -> tuple[Any, ...]:
        return (
            type(self),
            (
                "fields",
                tuple(
                    (field.name, _type_cache_key(field.type_), field.nullable)
                    for field in self.fields
                ),
            ),
            ("name", self.name),
            ("owner", self.owner),
        )

    def __init__(
        self,
        fields: Iterable[RowField | tuple[Any, ...]],
        *,
        name: str | None = None,
        owner: str | None = None,
    ) -> None:
        normalized_fields: list[RowField] = []
        for field in fields:
            if isinstance(field, RowField):
                normalized_fields.append(field)
                continue
            if not isinstance(field, tuple) or len(field) not in {2, 3}:
                raise exc.ArgumentError(
                    "ROW fields must be RowField objects or "
                    "(name, type[, nullable]) tuples"
                )
            normalized_fields.append(
                RowField(
                    field[0],
                    field[1],
                    True if len(field) == 2 else field[2],
                )
            )
        if not normalized_fields:
            raise exc.ArgumentError("Informix ROW requires at least one field")
        field_names = [field.name for field in normalized_fields]
        if len(set(field_names)) != len(field_names):
            raise exc.ArgumentError("Informix ROW field names must be unique")
        if name is not None:
            name = _validate_identifier(name, "ROW type name")
        if owner is not None:
            owner = _validate_identifier(owner, "ROW type owner", 32)
        object.__setattr__(self, "fields", tuple(normalized_fields))
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "owner", owner)
        self._freeze()

    @property
    def is_named(self) -> bool:
        return self.name is not None

    def adapt(self, cls: type[sa_types.TypeEngine[Any]], **kw: Any):
        """Preserve named-ROW identity during SQLAlchemy adaptation.

        SQLAlchemy's generic constructor-copy mechanism does not include
        keyword-only constructor arguments.  Without this override, ``name``
        and ``owner`` disappear in the dialect implementation and a bind for a
        named ROW is incorrectly compiled as ``CAST(? AS ROW(...))`` instead
        of ``CAST(? AS owner.row_type)``.
        """
        if cls is type(self):
            adapted = cls(
                self.fields,
                name=self.name,
                owner=self.owner,
            )
            adapted._variant_mapping = self._variant_mapping
            return adapted
        return super().adapt(cls, **kw)

    def bind_expression(self, bindvalue: Any) -> Any:
        # Named ROW values are strongly typed and must be explicitly cast.
        # The same cast is valid for anonymous ROW declarations and gives the
        # server complete field metadata for an ODBC VARCHAR parameter.
        return cast(bindvalue, self)

    def _values_from_python(self, value: Any) -> tuple[Any, ...]:
        if isinstance(value, RowValue):
            if value.fields == tuple(field.name for field in self.fields):
                return value.values
            value = value.as_dict()
        if isinstance(value, Mapping):
            missing = [field.name for field in self.fields if field.name not in value]
            extras = [key for key in value if key not in {f.name for f in self.fields}]
            if missing or extras:
                raise ValueError(
                    "ROW mapping fields do not match declaration; "
                    f"missing={missing!r}, extra={extras!r}"
                )
            return tuple(value[field.name] for field in self.fields)
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            if len(value) != len(self.fields):
                raise ValueError(
                    f"ROW expects {len(self.fields)} values, received {len(value)}"
                )
            return tuple(value)
        raise TypeError("Informix ROW expects a mapping, RowValue, or sequence")

    def _serialize(self, value: Any, dialect: Any) -> str:
        if isinstance(value, (str, bytes, bytearray, memoryview)):
            value = self._coerce_parsed(parse_complex_value(value), dialect)
        values = self._values_from_python(value)
        rendered: list[str] = []
        for field, item in zip(self.fields, values):
            if item is None and not field.nullable:
                raise ValueError(f"Informix ROW field {field.name!r} is NOT NULL")
            rendered.append(_render_scalar(item, field.type_, dialect))
        return f"ROW({', '.join(rendered)})"

    def _coerce_parsed(self, parsed: Any, dialect: Any) -> RowValue:
        if isinstance(parsed, _QuotedValue):
            parsed = parse_complex_value(parsed.value)
        if not isinstance(parsed, _ConstructorValue):
            raise ValueError(f"Expected ROW constructor, received {parsed!r}")
        constructor_name = parsed.name
        normalized_name = constructor_name.upper()
        constructor_leaf = constructor_name.rsplit(".", 1)[-1]
        if normalized_name != "ROW" and (
            self.name is None
            or constructor_leaf.casefold() != self.name.casefold()
        ):
            raise ValueError(
                f"Expected ROW/{self.name or 'anonymous'} constructor, "
                f"received {constructor_name}"
            )
        if len(parsed.values) != len(self.fields):
            raise ValueError(
                f"ROW expects {len(self.fields)} fields, "
                f"received {len(parsed.values)}"
            )
        values = tuple(
            _coerce_scalar(item, field.type_, dialect)
            for field, item in zip(self.fields, parsed.values)
        )
        for field, item in zip(self.fields, values):
            if item is None and not field.nullable:
                raise ValueError(f"Informix ROW field {field.name!r} is NOT NULL")
        return RowValue(
            tuple(field.name for field in self.fields),
            values,
            self.name,
        )


class DISTINCT(_ComplexTypeBase):
    """Reference to an Informix DISTINCT user-defined type."""

    __visit_name__ = "DISTINCT"

    @property
    def _static_cache_key(self) -> tuple[Any, ...]:
        return (
            type(self),
            ("name", self.name),
            ("source_type", _type_cache_key(self.source_type)),
            ("owner", self.owner),
        )

    def __init__(
        self,
        name: str,
        source_type: Any,
        *,
        owner: str | None = None,
    ) -> None:
        object.__setattr__(self, "name", _validate_identifier(name, "DISTINCT name"))
        source_type = _coerce_type(source_type)
        if isinstance(source_type, _CollectionType):
            raise exc.ArgumentError(
                "Informix DISTINCT types cannot use LIST, SET, or MULTISET "
                "as their source type"
            )
        if isinstance(source_type, ROW) and not source_type.is_named:
            raise exc.ArgumentError(
                "Informix DISTINCT types cannot use an anonymous ROW as "
                "their source type"
            )
        _validate_distinct_source_type(source_type)
        object.__setattr__(self, "source_type", source_type)
        if owner is not None:
            owner = _validate_identifier(owner, "DISTINCT owner", 32)
        object.__setattr__(self, "owner", owner)
        self._freeze()

    @property
    def python_type(self) -> type[Any]:
        return self.source_type.python_type

    def adapt(self, cls: type[sa_types.TypeEngine[Any]], **kw: Any):
        """Preserve the owner of a DISTINCT type during adaptation."""
        if cls is type(self):
            adapted = cls(
                self.name,
                self.source_type,
                owner=self.owner,
            )
            adapted._variant_mapping = self._variant_mapping
            return adapted
        return super().adapt(cls, **kw)

    def bind_expression(self, bindvalue: Any) -> Any:
        return cast(bindvalue, self)

    def bind_processor(self, dialect: Any):
        return self.source_type.bind_processor(dialect)

    def result_processor(self, dialect: Any, coltype: Any):
        return self.source_type.result_processor(dialect, coltype)

    def literal_processor(self, dialect: Any):
        return self.source_type.literal_processor(dialect)

    def get_dbapi_type(self, dbapi: Any) -> Any:
        return self.source_type.get_dbapi_type(dbapi)


class CreateRowType(ExecutableDDLElement):
    """Create a named Informix ROW type."""

    inherit_cache = True

    def __init__(
        self,
        row_type: ROW,
        *,
        if_not_exists: bool = False,
        under: ROW | None = None,
    ) -> None:
        if not isinstance(row_type, ROW) or not row_type.is_named:
            raise exc.ArgumentError("CreateRowType requires a named ROW instance")
        if not isinstance(if_not_exists, bool):
            raise exc.ArgumentError("if_not_exists must be a boolean")
        if under is not None and (
            not isinstance(under, ROW) or not under.is_named
        ):
            raise exc.ArgumentError("under must be a named ROW instance")
        self.row_type = row_type
        self.if_not_exists = if_not_exists
        self.under = under
        self.element = row_type


class DropRowType(ExecutableDDLElement):
    """Drop a named Informix ROW type using required RESTRICT semantics."""

    inherit_cache = True

    def __init__(
        self,
        row_type: ROW | str,
        *,
        owner: str | None = None,
        if_exists: bool = False,
    ) -> None:
        if isinstance(row_type, ROW):
            if not row_type.is_named:
                raise exc.ArgumentError("DropRowType requires a named ROW")
            self.name = row_type.name
            self.owner = row_type.owner
        else:
            self.name = _validate_identifier(row_type, "ROW type name")
            self.owner = (
                _validate_identifier(owner, "ROW type owner", 32)
                if owner is not None
                else None
            )
        if not isinstance(if_exists, bool):
            raise exc.ArgumentError("if_exists must be a boolean")
        self.if_exists = if_exists
        self.element = row_type


class CreateDistinctType(ExecutableDDLElement):
    """Create a native Informix DISTINCT type."""

    inherit_cache = True

    def __init__(
        self,
        distinct_type: DISTINCT,
        *,
        if_not_exists: bool = False,
    ) -> None:
        if not isinstance(distinct_type, DISTINCT):
            raise exc.ArgumentError(
                "CreateDistinctType requires an IfxAlchemy.DISTINCT instance"
            )
        if not isinstance(if_not_exists, bool):
            raise exc.ArgumentError("if_not_exists must be a boolean")
        self.distinct_type = distinct_type
        self.if_not_exists = if_not_exists
        self.element = distinct_type


class DropDistinctType(ExecutableDDLElement):
    """Drop a native Informix DISTINCT type using RESTRICT semantics."""

    inherit_cache = True

    def __init__(
        self,
        distinct_type: DISTINCT | str,
        *,
        owner: str | None = None,
        if_exists: bool = False,
    ) -> None:
        if isinstance(distinct_type, DISTINCT):
            self.name = distinct_type.name
            self.owner = distinct_type.owner
        else:
            self.name = _validate_identifier(distinct_type, "DISTINCT type name")
            self.owner = (
                _validate_identifier(owner, "DISTINCT owner", 32)
                if owner is not None
                else None
            )
        if not isinstance(if_exists, bool):
            raise exc.ArgumentError("if_exists must be a boolean")
        self.if_exists = if_exists
        self.element = distinct_type


def _render_row_fields(compiler: Any, row_type: ROW) -> str:
    type_compiler = compiler.dialect.type_compiler_instance
    preparer = compiler.preparer
    rendered: list[str] = []
    for field in row_type.fields:
        field_sql = (
            f"{preparer.quote(field.name)} "
            f"{type_compiler.process(field.type_)}"
        )
        if not field.nullable:
            field_sql += " NOT NULL"
        rendered.append(field_sql)
    return ", ".join(rendered)


@compiles(CreateRowType, "informix")
def _compile_create_row_type(element: CreateRowType, compiler: Any, **kw: Any) -> str:
    prefix = "CREATE ROW TYPE "
    if element.if_not_exists:
        prefix += "IF NOT EXISTS "
    name = _qualified_type_name(
        compiler.preparer,
        element.row_type.name,
        element.row_type.owner,
    )
    sql = f"{prefix}{name} ({_render_row_fields(compiler, element.row_type)})"
    if element.under is not None:
        sql += " UNDER " + _qualified_type_name(
            compiler.preparer,
            element.under.name,
            element.under.owner,
        )
    return sql


@compiles(DropRowType, "informix")
def _compile_drop_row_type(element: DropRowType, compiler: Any, **kw: Any) -> str:
    prefix = "DROP ROW TYPE "
    if element.if_exists:
        prefix += "IF EXISTS "
    return (
        prefix
        + _qualified_type_name(compiler.preparer, element.name, element.owner)
        + " RESTRICT"
    )


@compiles(CreateDistinctType, "informix")
def _compile_create_distinct_type(
    element: CreateDistinctType,
    compiler: Any,
    **kw: Any,
) -> str:
    prefix = "CREATE DISTINCT TYPE "
    if element.if_not_exists:
        prefix += "IF NOT EXISTS "
    distinct_type = element.distinct_type
    name = _qualified_type_name(
        compiler.preparer,
        distinct_type.name,
        distinct_type.owner,
    )
    source = compiler.dialect.type_compiler_instance.process(
        distinct_type.source_type
    )
    return f"{prefix}{name} AS {source}"


@compiles(DropDistinctType, "informix")
def _compile_drop_distinct_type(
    element: DropDistinctType,
    compiler: Any,
    **kw: Any,
) -> str:
    prefix = "DROP TYPE "
    if element.if_exists:
        prefix += "IF EXISTS "
    return (
        prefix
        + _qualified_type_name(compiler.preparer, element.name, element.owner)
        + " RESTRICT"
    )


__all__ = (
    "CreateDistinctType",
    "CreateRowType",
    "DISTINCT",
    "DropDistinctType",
    "DropRowType",
    "LIST",
    "MULTISET",
    "ROW",
    "RowField",
    "RowValue",
    "SET",
    "parse_complex_value",
)
