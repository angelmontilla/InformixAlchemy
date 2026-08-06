# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2008-2019 IBM Corporation
# Copyright (c) 2026 Angel Montilla
#
# Originally derived from IfxAlchemy / OpenInformix.
# Modified by Angel Montilla to adapt IfxAlchemy to SQLAlchemy 2.0.
#
# Original authors: Sathyanesh Krishnan, Shilpa S Jadhav
# Additional authors: Alex Pitigoi, Abhigyan Agrawal, Rahul Priyadarshi, Abhinav Radke
# Contributors: Jaimy Azle, Mike Bayer, Hemlata Bhatt, Angel Montilla
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import re

from sqlalchemy import exc
from sqlalchemy import types as sa_types
from sqlalchemy import util
from sqlalchemy.sql import quoted_name
from sqlalchemy import MetaData
from sqlalchemy.engine import reflection
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope
from .temporal import IFXDateTime
from .temporal import IFXTime
from .temporal import INTERVAL
from .complex import DISTINCT, LIST, MULTISET, ROW, SET, RowField
from .indexes import _ReflectedAccessMethodParameters
from .identity import identity_sequence_name
from .comments import (
    COLUMN_COMMENT_CATALOG,
    TABLE_COMMENT_CATALOG,
    decode_comment,
)
from .fragmentation import (
    AttachedIndexFragmentation,
    ExpressionFragmentation,
    Fragment,
    ListFragmentation,
    RangeIntervalFragmentation,
    RoundRobinFragmentation,
    _ReflectedFragmentExpression,
)

from . import sqla_compat
from ._reflection_helpers import (
    int_or_default as _helper_int_or_default,
    row_value as _helper_row_value,
    single_or_tuple as _helper_single_or_tuple,
)


logger = logging.getLogger(__name__)


class _RemoteSynonymReflectionError(exc.UnreflectableTableError):
    """A synonym exists, but its target catalog is outside this database."""


class IfxInspector(reflection.Inspector):
    """Inspector extension exposing Informix synonym metadata."""

    def get_synonym_names(self, schema=None, **kw):
        with self._operation_context() as connection:
            return self.dialect.get_synonym_names(
                connection,
                schema=schema,
                info_cache=self.info_cache,
                **kw,
            )

    def get_synonyms(self, schema=None, **kw):
        with self._operation_context() as connection:
            return self.dialect.get_synonyms(
                connection,
                schema=schema,
                info_cache=self.info_cache,
                **kw,
            )

    def has_synonym(self, synonym_name, schema=None, **kw):
        with self._operation_context() as connection:
            return self.dialect.has_synonym(
                connection,
                synonym_name,
                schema=schema,
                info_cache=self.info_cache,
                **kw,
            )

    def get_user_defined_types(self, schema=None, **kw):
        """Return structured named ROW and DISTINCT type metadata."""
        with self._operation_context() as connection:
            return self.dialect.get_user_defined_types(
                connection,
                schema=schema,
                info_cache=self.info_cache,
                **kw,
            )


class _ReflectedTableLockLevel(str):
    """String marker for a locking mode read from SYSTABLES."""

    _informix_reflected_lock_level = True


class _ReflectedTablePageSize(int):
    """Integer marker for a dbspace page size read from SYSTABLES.

    The value intentionally remains an ``int`` for SQLAlchemy metadata and
    public inspection APIs.  The marker prevents the DDL compiler from
    confusing reflected, read-only metadata with a user-authored CREATE TABLE
    option.
    """

    _informix_reflected_page_size = True


def _informix_boolean_type():
    """Create the dialect BOOLEAN lazily to avoid an import cycle."""
    from .base import BOOLEAN

    return BOOLEAN()


class BaseReflector(object):
    def __init__(self, dialect):
        self.dialect = dialect
        self.ischema_names = dialect.ischema_names
        self.identifier_preparer = dialect.identifier_preparer

    def _coerce_name(self, name):
        if name is None:
            return None

        if isinstance(name, memoryview):
            name = name.tobytes()

        if isinstance(name, bytearray):
            name = bytes(name)

        if isinstance(name, bytes):
            return name.decode()

        return str(name)

    def normalize_name(self, name):
        name = self._coerce_name(name)
        if name is None:
            return None

        lowered = name.lower()
        uppered = name.upper()

        if uppered == lowered:
            return name

        if (
            uppered == name
            and not sqla_compat.identifier_requires_quotes(
                self.identifier_preparer,
                lowered,
            )
        ):
            return lowered

        if lowered == name:
            return quoted_name(name, quote=True)

        return name

    def denormalize_name(self, name):
        name = self._coerce_name(name)
        if name is None:
            return None

        lowered = name.lower()
        if lowered == name and not sqla_compat.identifier_requires_quotes(
            self.identifier_preparer, lowered
        ):
            return name.upper()

        return name

    def _logical_reflected_name(self, name, schema=None):
        """Return the SQLAlchemy-visible name for a catalog object.

        Constraint and index names belonging to an explicitly schema-owned
        table are stored in Informix with ``<owner>__`` as a physical prefix.
        Reflection must remove that implementation prefix so metadata
        round-trips preserve the logical name supplied by the application.
        Names from the default owner, system-generated names, and names whose
        prefix does not match the requested owner are returned unchanged.
        """
        physical_name = self._coerce_name(name)
        if physical_name is None:
            return None

        schema_name = self._coerce_name(schema)
        if schema_name:
            prefix = f"{schema_name}__"
            if physical_name.casefold().startswith(prefix.casefold()):
                physical_name = physical_name[len(prefix):]

        return self.normalize_name(physical_name)

    def _get_default_schema_name(self, connection):
        """Return: current setting of the schema attribute"""
        default_schema_name = connection.exec_driver_sql(
                    'SELECT USER FROM systables WHERE tabid = 1').scalar()
        if default_schema_name is not None:
            coerced_schema_name = self._coerce_name(default_schema_name)
            default_schema_name = (
                coerced_schema_name.strip()
                if coerced_schema_name is not None
                else None
            )
        return self.normalize_name(default_schema_name)

    @property
    def default_schema_name(self):
        return self.dialect.default_schema_name

    def _normalize_filter_names(self, filter_names):
        if not filter_names:
            return None

        normalized = set()
        for name in filter_names:
            normalized.add(self.normalize_name(name))
            normalized.add(self.denormalize_name(name))
            normalized.add(str(name))
        return normalized


class IfxReflector(BaseReflector):
    ischema = MetaData()

    _INDEX_PART_COUNT = 16
    _MISSING = object()

    _TABLE_LOCK_LEVELS = {
        "P": "PAGE",
        "R": "ROW",
        "B": "PAGE_AND_ROW",
    }

    _FRAGMENT_STRATEGIES = frozenset({"R", "E", "I", "N", "L", "T"})

    _PLAIN_LITERAL_DEFAULT_TYPES = {
        0,   # CHAR
        13,  # VARCHAR
        15,  # NCHAR
        16,  # NVARCHAR
        40,  # LVARCHAR
        45,  # BOOLEAN
    }

    # Informix syscolumns.coltype base codes
    _COLTYPE_CODE_MAP = {
        0: "CHAR",
        1: "SMALLINT",
        2: "INTEGER",
        3: "FLOAT",
        4: "SMALLFLOAT",
        5: "DECIMAL",
        6: "SERIAL",
        7: "DATE",
        8: "MONEY",
        9: "NULL",
        10: "DATETIME",
        11: "BYTE",
        12: "TEXT",
        13: "VARCHAR",
        14: "INTERVAL",
        15: "NCHAR",
        16: "NVARCHAR",
        17: "INT8",
        18: "SERIAL8",
        19: "SET",
        20: "MULTISET",
        21: "LIST",
        22: "ROW",
        40: "LVARCHAR",
        41: "OPAQUE",
        45: "BOOLEAN",
        52: "BIGINT",
        53: "BIGSERIAL",
    }

    _OPAQUE_TYPE_NAMES = {
        "blob": "BLOB",
        "clob": "CLOB",
        "boolean": "BOOLEAN",
        "lvarchar": "LVARCHAR",
        "json": "JSON",
        "bson": "BSON",
    }

    _CHAR_FALLBACK_TYPES = {"CHAR"}
    _VARCHAR_FALLBACK_TYPES = {"VARCHAR"}
    _NCHAR_FALLBACK_TYPES = {"NCHAR"}
    _NVARCHAR_FALLBACK_TYPES = {"NVARCHAR"}
    _INTEGER_FALLBACK_TYPES = {"INTEGER", "SERIAL"}
    _BIG_INTEGER_FALLBACK_TYPES = {"INT8", "SERIAL8", "BIGINT", "BIGSERIAL"}
    _NUMERIC_FALLBACK_TYPES = {"DECIMAL", "NUMERIC", "MONEY"}
    _SIMPLE_FALLBACK_FACTORIES = {
        "SMALLINT": sa_types.SmallInteger,
        "FLOAT": sa_types.Float,
        "SMALLFLOAT": sa_types.Float,
        "DATE": sa_types.Date,
        "DATETIME": sa_types.DateTime,
        "INTERVAL": INTERVAL,
        "TEXT": sa_types.Text,
        "BYTE": sa_types.LargeBinary,
        "BOOLEAN": _informix_boolean_type,
        "NULL": sa_types.NullType,
    }

    _DATETIME_QUALIFIERS = {
        0: "YEAR",
        2: "MONTH",
        4: "DAY",
        6: "HOUR",
        8: "MINUTE",
        10: "SECOND",
        11: "FRACTION(1)",
        12: "FRACTION(2)",
        13: "FRACTION(3)",
        14: "FRACTION(4)",
        15: "FRACTION(5)",
    }

    def _clean_str(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()

    def _comment_catalog_exists(self, connection, catalog_name):
        owner = self._resolved_owner(None)
        row = connection.exec_driver_sql(
            """
            SELECT FIRST 1 t.tabid
            FROM systables t
            WHERE LOWER(t.tabname) = LOWER(?)
              AND LOWER(t.owner) = LOWER(?)
              AND t.tabtype = 'T'
            """,
            (catalog_name, owner),
        ).first()
        return row is not None

    def _decode_comment_value(self, value):
        try:
            return decode_comment(value)
        except ValueError as error:
            util.warn(str(error))
            return None

    def _table_comment_for_tabid(
        self,
        connection,
        tabid,
        owner,
        table_name,
    ):
        if not self._comment_catalog_exists(
            connection,
            TABLE_COMMENT_CATALOG,
        ):
            return None

        row = connection.exec_driver_sql(
            f"""
            SELECT FIRST 1 c.comment_value
            FROM {TABLE_COMMENT_CATALOG} c
            WHERE c.tabid = ?
              AND c.object_owner = ?
              AND c.object_name = ?
            """,
            (int(tabid), owner, table_name),
        ).first()
        if row is None:
            return None
        return self._decode_comment_value(row[0])

    def _column_comments_for_tabid(
        self,
        connection,
        tabid,
        owner,
        table_name,
    ):
        if not self._comment_catalog_exists(
            connection,
            COLUMN_COMMENT_CATALOG,
        ):
            return {}

        rows = connection.exec_driver_sql(
            f"""
            SELECT c.colno, c.column_name, c.comment_value
            FROM {COLUMN_COMMENT_CATALOG} c
            WHERE c.tabid = ?
              AND c.object_owner = ?
              AND c.object_name = ?
            ORDER BY c.colno
            """,
            (int(tabid), owner, table_name),
        ).fetchall()

        comments = {}
        for colno, column_name, comment_value in rows:
            comments[int(colno)] = (
                self._clean_str(column_name),
                self._decode_comment_value(comment_value),
            )
        return comments

    def _clean_default_catalog_value(self, value):
        """Normalize a textual value read from sysdefaults.

        Informix stores sysdefaults.default in a fixed-length catalog
        column. Some ODBC paths expose the terminating catalog padding as
        one or more NUL characters.

        NUL characters at the end are transport/catalog padding and are
        not part of the SQL default. Spaces before the NUL must be
        preserved because they may belong to a character literal.
        """
        value = self._clean_str(value)

        if value is None:
            return None

        return value.rstrip("\x00")

    def _normalize_extended_type_name(self, value):
        value = self._clean_str(value)
        if not value:
            return None
        return value.lower()

    def _resolve_opaque_type_name(self, extended_type_name, base_code):
        normalized = self._normalize_extended_type_name(extended_type_name)

        if normalized in self._OPAQUE_TYPE_NAMES:
            return self._OPAQUE_TYPE_NAMES[normalized]

        if base_code == 40:
            return "LVARCHAR"

        if base_code == 45:
            return "BOOLEAN"

        return None

    def _decode_datetime_qualifiers(self, encoded_len):
        first = (encoded_len & 0x00F0) >> 4
        last = encoded_len & 0x000F
        length = encoded_len >> 8

        return {
            "length": length,
            "first": self._DATETIME_QUALIFIERS.get(first),
            "last": self._DATETIME_QUALIFIERS.get(last),
            "first_code": first,
            "last_code": last,
        }

    def _dbapi_error_types(self):
        """Return SQLAlchemy and driver-specific DBAPI error classes."""
        error_types = [exc.DBAPIError]
        dbapi = getattr(self.dialect, "dbapi", None)
        driver_error = getattr(dbapi, "Error", None)
        if isinstance(driver_error, type) and driver_error not in error_types:
            error_types.append(driver_error)
        return tuple(error_types)

    def _odbc_column_metadata(self, connection, table_name, owner):
        """Return best-effort SQLColumns metadata keyed by column name.

        Informix ``SYSCOLUMNS.collength`` preserves the qualifier range and
        physical storage length, but two adjacent leading precisions can map
        to the same byte length. ODBC SQLColumns exposes the character size
        and fractional scale, which allows exact precision recovery. Drivers
        that do not expose this metadata simply fall back to the catalog.
        """

        setup_errors = (AttributeError, TypeError) + self._dbapi_error_types()
        try:
            proxied = connection.connection
            dbapi_connection = getattr(
                proxied,
                "driver_connection",
                proxied,
            )
            cursor = dbapi_connection.cursor()
        except setup_errors:
            logger.debug(
                "Could not open ODBC metadata cursor; using catalog reflection",
                exc_info=True,
            )
            return {}

        metadata = {}
        error_types = self._dbapi_error_types()
        try:
            rows = cursor.columns(
                table=str(table_name),
                schema=str(owner) if owner else None,
            )
            for row in rows:
                def value(attribute, index):
                    candidate = getattr(row, attribute, None)
                    if candidate is not None:
                        return candidate
                    try:
                        return row[index]
                    except (IndexError, KeyError, TypeError):
                        return None

                column_name = self._clean_str(value("column_name", 3))
                if not column_name:
                    continue
                metadata[column_name.casefold()] = {
                    "type_name": self._clean_str(value("type_name", 5)),
                    "column_size": value("column_size", 6),
                    "decimal_digits": value("decimal_digits", 8),
                    "sql_data_type": value("sql_data_type", 13),
                    "sql_datetime_sub": value("sql_datetime_sub", 14),
                }
        except error_types:
            # Metadata enrichment is optional. Catalog reflection remains the
            # authoritative and portable fallback for older CSDK drivers.
            logger.debug(
                "ODBC column metadata was unavailable; using catalog reflection",
                exc_info=True,
            )
            return {}
        finally:
            try:
                cursor.close()
            except error_types:
                logger.debug("Could not close ODBC metadata cursor", exc_info=True)

        return metadata

    def _resolved_owner(self, schema=None):
        """Convert a SQLAlchemy schema into an Informix owner."""

        owner = (
            self.default_schema_name
            if schema is None
            else schema
        )

        owner = self._clean_str(owner)

        if not owner:
            raise exc.InvalidRequestError(
                "Informix reflection requires a non-empty object owner. "
                "Pass schema explicitly or initialize default_schema_name."
            )

        return owner

    def _normalize_schema_for_output(self, owner, requested_schema=None):
        owner_norm = self.normalize_name(self._clean_str(owner))
        default_norm = self.normalize_name(self._clean_str(self.default_schema_name))
        if requested_schema is None and owner_norm == default_norm:
            return None
        return owner_norm

    def _is_explicitly_quoted(self, name):
        return getattr(name, "quote", None) is True

    def _fold_unquoted_lookup_name(self, name):
        cleaned = self._clean_str(name)
        if cleaned is None:
            return None
        # Informix stores unquoted identifiers in lowercase in the catalog
        # when DELIMIDENT=Y is enabled.
        return cleaned.lower()

    def _get_table_row(self, connection, table_name, schema=None, tabtypes=None):
        owner = self._resolved_owner(schema)
        tabtypes = tuple(tabtypes or ("T", "V"))

        cleaned_name = self._clean_str(table_name)
        if not cleaned_name:
            return None

        is_explicitly_quoted = getattr(table_name, "quote", None) is True
        if is_explicitly_quoted:
            lookup_name = cleaned_name
        else:
            # An unquoted Informix name must be folded to the catalog's
            # case-insensitive form instead of being looked up with the
            # mixed-case value received from Python.
            lookup_name = self._fold_unquoted_lookup_name(cleaned_name)

        placeholders = ", ".join("?" for _ in tabtypes)

        sql_text = f"""
            SELECT FIRST 1
                t.tabid,
                t.tabname,
                t.owner,
                t.tabtype
            FROM systables t
            WHERE t.tabname = ?
            AND LOWER(t.owner) = LOWER(?)
            AND t.tabtype IN ({placeholders})
            ORDER BY t.tabid
        """
        params = (lookup_name, owner, *tabtypes)
        return connection.exec_driver_sql(sql_text, params).first()

    def _require_table_row(self, connection, table_name, schema=None, tabtypes=None):
        row = self._get_table_row(connection, table_name, schema=schema, tabtypes=tabtypes)
        if row is None:
            raise exc.NoSuchTableError(table_name)
        return row

    _SYNONYM_TARGET_TYPES = {
        "T": "table",
        "E": "external_table",
        "V": "view",
        "Q": "sequence",
        "P": "synonym",
        "S": "synonym",
    }

    def _synonym_catalog_rows(
        self,
        connection,
        *,
        schema=None,
        synonym_name=None,
        include_public=True,
    ):
        """Read visible private/public synonyms from SYSTABLES/SYSSYNTABLE."""
        owner = self._resolved_owner(schema)
        predicates = ["s.tabtype IN ('P', 'S')"]
        params = []

        if include_public:
            predicates.append(
                "(s.tabtype = 'S' OR LOWER(s.owner) = LOWER(?))"
            )
            params.append(owner)
        else:
            predicates.extend(
                ["s.tabtype = 'P'", "LOWER(s.owner) = LOWER(?)"]
            )
            params.append(owner)

        if synonym_name is not None:
            cleaned_name = self._clean_str(synonym_name)
            if not cleaned_name:
                return []
            if self._is_explicitly_quoted(synonym_name):
                lookup_name = cleaned_name
            else:
                lookup_name = self._fold_unquoted_lookup_name(cleaned_name)
            predicates.append("s.tabname = ?")
            params.append(lookup_name)

        sql_text = f"""
            SELECT
                s.tabid,
                s.tabname,
                s.owner,
                s.tabtype,
                y.servername,
                y.dbname,
                y.owner,
                y.tabname,
                y.btabid,
                b.tabname AS base_tabname,
                b.owner AS base_owner,
                b.tabtype AS base_tabtype
            FROM systables s
            JOIN syssyntable y
              ON y.tabid = s.tabid
            LEFT OUTER JOIN systables b
              ON b.tabid = y.btabid
            WHERE {' AND '.join(predicates)}
            ORDER BY
                s.tabname,
                s.tabtype,
                s.tabid
        """
        return connection.exec_driver_sql(sql_text, tuple(params)).fetchall()

    def _optional_normalized_catalog_name(self, value):
        cleaned = self._clean_str(value)
        if not cleaned:
            return None
        return self.normalize_name(cleaned)

    def _synonym_info_from_row(self, row, requested_schema=None):
        synonym_type = (self._clean_str(row[3]) or "").upper()
        public = synonym_type == "S"
        synonym_owner = self._optional_normalized_catalog_name(row[2])

        btabid = self._positive_catalog_int(row[8])
        if btabid is not None:
            target_name = self._optional_normalized_catalog_name(row[9])
            target_owner = self._optional_normalized_catalog_name(row[10])
            target_tabtype = (self._clean_str(row[11]) or "").upper()
            target_database = None
            target_server = None
            local = True
        else:
            target_name = self._optional_normalized_catalog_name(row[7])
            target_owner = self._optional_normalized_catalog_name(row[6])
            target_tabtype = ""
            target_database = self._optional_normalized_catalog_name(row[5])
            target_server = self._optional_normalized_catalog_name(row[4])
            local = False

        target_type = self._SYNONYM_TARGET_TYPES.get(target_tabtype)
        target = {
            "name": target_name,
            "owner": target_owner,
            "database": target_database,
            "server": target_server,
            "type": target_type,
            "local": local,
        }

        return {
            "name": self._optional_normalized_catalog_name(row[1]),
            "schema": self._normalize_schema_for_output(
                synonym_owner,
                requested_schema=requested_schema,
            ),
            "owner": synonym_owner,
            "public": public,
            "target": target,
            # Flat aliases keep the API convenient for migration/reporting
            # code without discarding the structured representation.
            "target_name": target_name,
            "target_schema": target_owner,
            "target_database": target_database,
            "target_server": target_server,
            "target_type": target_type,
        }

    @reflection.cache
    def get_synonyms(self, connection, schema=None, **kw):
        """Return structured metadata for visible Informix synonyms.

        Private synonyms owned by ``schema`` and public synonyms are returned.
        Set ``include_public=False`` to restrict the result to private synonyms.
        Synonyms remain separate from :meth:`get_table_names` by design.
        """
        include_public = kw.pop("include_public", True)
        if not isinstance(include_public, bool):
            raise exc.InvalidRequestError("include_public must be a boolean")
        rows = self._synonym_catalog_rows(
            connection,
            schema=schema,
            include_public=include_public,
        )
        return [
            self._synonym_info_from_row(row, requested_schema=schema)
            for row in rows
        ]

    @reflection.cache
    def get_synonym_names(self, connection, schema=None, **kw):
        """Return visible synonym names without mixing them with tables."""
        synonyms = self.get_synonyms(connection, schema=schema, **kw)
        names = []
        seen = set()
        for synonym in synonyms:
            name = synonym["name"]
            key = str(name)
            if key in seen:
                continue
            seen.add(key)
            names.append(name)
        return names

    @reflection.cache
    def has_synonym(
        self,
        connection,
        synonym_name,
        schema=None,
        **kw,
    ):
        """Return whether a visible private or public synonym exists."""
        include_public = kw.pop("include_public", True)
        if not isinstance(include_public, bool):
            raise exc.InvalidRequestError("include_public must be a boolean")
        rows = self._synonym_catalog_rows(
            connection,
            schema=schema,
            synonym_name=synonym_name,
            include_public=include_public,
        )
        return bool(rows)

    def _resolve_reflection_target(
        self,
        connection,
        table_name,
        schema,
        kw,
    ):
        """Resolve a local synonym chain for opt-in Table autoload.

        Complete SQLAlchemy reflection requires local system-catalog metadata.
        External synonyms are therefore reported as unreflectable instead of
        silently returning incomplete column/constraint information.
        """
        resolve = kw.get("informix_resolve_synonyms", False)
        if not isinstance(resolve, bool):
            raise exc.InvalidRequestError(
                "informix_resolve_synonyms must be a boolean"
            )
        if not resolve:
            return table_name, schema, None

        original_name = table_name
        current_name = table_name
        current_schema = schema
        visited = set()
        first_synonym = None

        for _depth in range(16):
            rows = self._synonym_catalog_rows(
                connection,
                schema=current_schema,
                synonym_name=current_name,
                include_public=True,
            )
            if not rows:
                return current_name, current_schema, first_synonym

            info = self._synonym_info_from_row(
                rows[0],
                requested_schema=current_schema,
            )
            if first_synonym is None:
                first_synonym = info

            key = (
                str(info.get("owner")),
                str(info.get("name")),
            )
            if key in visited:
                raise exc.UnreflectableTableError(
                    f"Informix synonym cycle detected while resolving "
                    f"{original_name!r}"
                )
            visited.add(key)

            target = info["target"]
            if not target["local"]:
                raise _RemoteSynonymReflectionError(
                    "Informix remote synonym reflection is not available "
                    "through the current database catalog. The synonym "
                    f"{original_name!r} targets "
                    f"database={target['database']!r}, "
                    f"server={target['server']!r}."
                )

            if target["type"] == "sequence":
                raise exc.UnreflectableTableError(
                    f"Informix synonym {original_name!r} targets a sequence, "
                    "not a table or view"
                )

            if not target["name"]:
                raise exc.UnreflectableTableError(
                    f"Informix synonym {original_name!r} has no resolvable "
                    "local target in SYSSYNTABLE"
                )

            current_name = target["name"]
            current_schema = self._normalize_schema_for_output(
                target["owner"],
                requested_schema=current_schema,
            )

            if target["type"] != "synonym":
                return current_name, current_schema, first_synonym

        raise exc.UnreflectableTableError(
            f"Informix synonym chain for {original_name!r} exceeds the Informix limit of 16 links"
        )

    def _get_column_name_map(self, connection, tabid):
        sql_text = """
            SELECT c.colno, c.colname
            FROM syscolumns c
            WHERE c.tabid = ?
            ORDER BY c.colno
        """
        rows = connection.exec_driver_sql(sql_text, (tabid,)).fetchall()
        return {
            int(row[0]): self.normalize_name(self._clean_str(row[1]))
            for row in rows
        }

    def _extract_index_colnos(self, part_values):
        colnos = []
        desc_by_colno = {}

        for raw in part_values:
            if raw in (None, 0):
                continue

            val = int(raw)
            colno = abs(val)
            colnos.append(colno)

            if val < 0:
                desc_by_colno[colno] = True

        return colnos, desc_by_colno

    def _get_index_parts_row(self, connection, tabid, idxname, owner=None):
        if not idxname:
            return None

        if owner:
            sql_text = f"""
                SELECT
                    i.idxname,
                    i.owner,
                    i.idxtype,
                    {", ".join(f"i.part{n}" for n in range(1, self._INDEX_PART_COUNT + 1))}
                FROM sysindexes i
                WHERE i.tabid = ?
                  AND LOWER(TRIM(i.idxname)) = LOWER(TRIM(?))
                  AND LOWER(TRIM(i.owner)) = LOWER(TRIM(?))
            """
            params = (tabid, idxname, owner)
        else:
            sql_text = f"""
                SELECT
                    i.idxname,
                    i.owner,
                    i.idxtype,
                    {", ".join(f"i.part{n}" for n in range(1, self._INDEX_PART_COUNT + 1))}
                FROM sysindexes i
                WHERE i.tabid = ?
                  AND LOWER(TRIM(i.idxname)) = LOWER(TRIM(?))
            """
            params = (tabid, idxname)

        return connection.exec_driver_sql(sql_text, params).first()

    def _get_index_columns(self, connection, tabid, idxname, owner=None):
        idx_row = self._get_index_parts_row(connection, tabid, idxname, owner=owner)
        if idx_row is None and owner:
            # Constraint owners and index owners do not always match for
            # auto-generated backing indexes, especially for PK/UK metadata.
            idx_row = self._get_index_parts_row(connection, tabid, idxname, owner=None)
        if idx_row is None:
            return [], {}

        colmap = self._get_column_name_map(connection, tabid)
        colnos, desc_by_colno = self._extract_index_colnos(idx_row[3:])

        if not colnos:
            util.warn(
                "Could not derive indexed columns from sysindexes for "
                f"tabid={tabid}, idxname={idxname!r}. "
                "This can happen with functional/generalized indexes."
            )
            return [], {}

        colnames = [colmap.get(colno) for colno in colnos]
        colnames = [c for c in colnames if c is not None]

        column_sorting = {}
        for colno in colnos:
            if desc_by_colno.get(colno):
                colname = colmap.get(colno)
                if colname:
                    column_sorting[colname] = ("desc",)

        return colnames, column_sorting

    def _dbapi_connection(self, connection):
        raw_connection = getattr(connection, "connection", None)
        if raw_connection is None:
            return None
        return getattr(raw_connection, "dbapi_connection", None)

    def _close_cursor(self, cursor):
        if cursor is None:
            return

        try:
            cursor.close()
        except self._dbapi_error_types():
            logger.debug("Could not close reflection cursor", exc_info=True)

    def _fetch_odbc_rows(self, connection, method_name, kwargs):
        dbapi_connection = self._dbapi_connection(connection)
        if dbapi_connection is None:
            return []

        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            method = getattr(cursor, method_name, None)
            if method is None:
                return []
            return method(**kwargs).fetchall()
        except self._dbapi_error_types():
            logger.debug(
                "ODBC reflection method %s was unavailable",
                method_name,
                exc_info=True,
            )
            return []
        finally:
            self._close_cursor(cursor)

    def _row_value(self, row, attr_names, index, default=_MISSING):
        return _helper_row_value(
            row,
            attr_names,
            index,
            default=default,
            missing=self._MISSING,
        )

    def _normalized_clean_name(self, value):
        cleaned = self._clean_str(value)
        if cleaned is None:
            return None
        return self.normalize_name(cleaned)

    def _int_or_default(self, value, default):
        return _helper_int_or_default(value, default)

    def _odbc_primary_key_entry(self, row):
        column_name = self._row_value(row, ("column_name", "COLUMN_NAME"), 3)
        key_seq = self._row_value(row, ("key_seq", "KEY_SEQ"), 4)
        clean_key_seq = self._clean_str(key_seq)
        if clean_key_seq is None:
            return None
        return int(clean_key_seq), self._normalized_clean_name(column_name)

    def _get_pk_columns_via_odbc(self, connection, table_name, schema=None):
        kwargs = {"table": table_name}
        if schema is not None:
            kwargs["schema"] = schema

        rows = self._fetch_odbc_rows(connection, "primaryKeys", kwargs)
        by_seq = []
        for row in rows:
            try:
                pk_entry = self._odbc_primary_key_entry(row)
                if pk_entry is not None:
                    by_seq.append(pk_entry)
            except (IndexError, KeyError, TypeError, ValueError):
                continue

        by_seq.sort(key=lambda item: item[0])
        return [name for _, name in by_seq if name]

    def _odbc_index_entry(self, row):
        return (
            self._row_value(row, ("index_name", "INDEX_NAME"), 5),
            self._row_value(row, ("column_name", "COLUMN_NAME"), 8),
            self._row_value(row, ("ordinal_position", "ORDINAL_POSITION"), 7),
            self._row_value(row, ("non_unique", "NON_UNIQUE"), 3),
        )

    def _odbc_unique_filter_allows(self, raw_non_unique, unique_only):
        if unique_only is True:
            return not bool(raw_non_unique)
        if unique_only is False:
            return bool(raw_non_unique)
        return True

    def _group_odbc_index_columns(self, rows, unique_only):
        grouped = {}

        for row in rows:
            try:
                (
                    raw_index_name,
                    raw_column_name,
                    raw_ordinal,
                    raw_non_unique,
                ) = self._odbc_index_entry(row)
            except (IndexError, KeyError, TypeError, ValueError):
                continue

            normalized_index_name = self._normalized_clean_name(raw_index_name)
            column_name = self._normalized_clean_name(raw_column_name)
            if not normalized_index_name or not column_name:
                continue

            if not self._odbc_unique_filter_allows(raw_non_unique, unique_only):
                continue

            ordinal = self._int_or_default(
                raw_ordinal,
                len(grouped.get(normalized_index_name, [])) + 1,
            )
            grouped.setdefault(normalized_index_name, []).append(
                (ordinal, column_name)
            )

        return grouped

    def _select_grouped_entries(self, grouped, wanted_name=None):
        if wanted_name:
            return grouped.get(wanted_name)
        if len(grouped) == 1:
            return next(iter(grouped.values()))
        return None

    def _get_index_columns_via_odbc(
        self,
        connection,
        table_name,
        schema=None,
        index_name=None,
        unique_only=None,
    ):
        kwargs = {"table": table_name, "unique": bool(unique_only), "quick": True}
        if schema is not None:
            kwargs["schema"] = schema

        rows = self._fetch_odbc_rows(connection, "statistics", kwargs)
        wanted_name = self.normalize_name(self._clean_str(index_name)) if index_name else None
        grouped = self._group_odbc_index_columns(rows, unique_only)
        selected = self._select_grouped_entries(grouped, wanted_name)
        if selected is None:
            return []

        selected.sort(key=lambda item: item[0])
        return [column_name for _, column_name in selected]

    def _odbc_foreign_key_entry(self, row):
        return (
            self._row_value(row, ("fk_name", "FK_NAME"), 11),
            self._row_value(row, ("key_seq", "KEY_SEQ"), 13),
            self._row_value(row, ("fkcolumn_name", "FKCOLUMN_NAME"), 7),
            self._row_value(row, ("pkcolumn_name", "PKCOLUMN_NAME"), 3),
        )

    def _group_odbc_foreign_key_columns(self, rows, wanted_name=None):
        grouped = {}

        for row in rows:
            try:
                (
                    raw_fk_name,
                    raw_key_seq,
                    raw_fk_column,
                    raw_pk_column,
                ) = self._odbc_foreign_key_entry(row)
            except (IndexError, KeyError, TypeError, ValueError):
                continue

            normalized_fk_name = self._normalized_clean_name(raw_fk_name)
            if wanted_name and normalized_fk_name != wanted_name:
                continue

            if not normalized_fk_name:
                normalized_fk_name = "__unnamed_fk__"

            key_seq = self._int_or_default(
                raw_key_seq,
                len(grouped.get(normalized_fk_name, [])) + 1,
            )
            grouped.setdefault(normalized_fk_name, []).append(
                (
                    key_seq,
                    self._normalized_clean_name(raw_fk_column),
                    self._normalized_clean_name(raw_pk_column),
                )
            )

        return grouped

    def _get_foreign_key_columns_via_odbc(
        self,
        connection,
        table_name,
        schema=None,
        fk_name=None,
    ):
        kwargs = {"foreignTable": table_name}
        if schema is not None:
            kwargs["foreignSchema"] = schema

        rows = self._fetch_odbc_rows(connection, "foreignKeys", kwargs)
        wanted_name = self.normalize_name(self._clean_str(fk_name)) if fk_name else None
        grouped = self._group_odbc_foreign_key_columns(rows, wanted_name)
        selected = self._select_grouped_entries(grouped, wanted_name)
        if selected is None:
            return [], []

        selected.sort(key=lambda item: item[0])
        constrained_columns = [fk_col for _, fk_col, _ in selected if fk_col]
        referred_columns = [pk_col for _, _, pk_col in selected if pk_col]
        return constrained_columns, referred_columns

    def _odbc_lookup_token(self, value):
        cleaned = self._clean_str(value)
        if cleaned is None:
            return None

        if getattr(value, "quote", None) is True:
            return {
                "lookup": cleaned,
                "wanted": cleaned,
                "quoted": True,
            }

        lookup = self._fold_unquoted_lookup_name(cleaned)
        return {
            "lookup": lookup,
            "wanted": self._normalized_clean_name(lookup),
            "quoted": False,
        }

    def _odbc_table_lookup(self, table_name, schema=None):
        cleaned_name = self._clean_str(table_name)
        if not cleaned_name:
            return None

        table_token = self._odbc_lookup_token(table_name)
        if table_token is None:
            return None

        schema_token = (
            self._odbc_lookup_token(schema) if schema is not None else None
        )
        return {
            "table_lookup": table_token["lookup"],
            "wanted_name": table_token["wanted"],
            "table_quoted": table_token["quoted"],
            "schema_lookup": (
                schema_token["lookup"] if schema_token is not None else None
            ),
            "wanted_schema": (
                schema_token["wanted"] if schema_token is not None else None
            ),
            "schema_quoted": (
                schema_token["quoted"] if schema_token is not None else False
            ),
        }

    def _odbc_table_rows(self, connection, lookup):
        kwargs = {"table": lookup["table_lookup"]}
        if lookup["schema_lookup"] is not None:
            kwargs["schema"] = lookup["schema_lookup"]
        return self._fetch_odbc_rows(connection, "tables", kwargs)

    def _odbc_names_match(self, raw_value, wanted_value, quoted):
        if quoted:
            return raw_value == wanted_value
        return self.normalize_name(raw_value) == wanted_value

    def _odbc_table_row_matches(self, row, lookup):
        raw_name = self._clean_str(
            self._row_value(row, ("table_name", "TABLE_NAME"), 2, default=None)
        )
        if raw_name is None:
            return False

        if not self._odbc_names_match(
            raw_name,
            lookup["wanted_name"],
            lookup["table_quoted"],
        ):
            return False

        if lookup["wanted_schema"] is None:
            return True

        raw_schema = self._clean_str(
            self._row_value(row, ("table_schem", "TABLE_SCHEM"), 1, default=None)
        )
        if raw_schema is None:
            return False

        return self._odbc_names_match(
            raw_schema,
            lookup["wanted_schema"],
            lookup["schema_quoted"],
        )

    def _has_table_via_odbc(self, connection, table_name, schema=None):
        lookup = self._odbc_table_lookup(table_name, schema=schema)
        if lookup is None:
            return False
        return any(
            self._odbc_table_row_matches(row, lookup)
            for row in self._odbc_table_rows(connection, lookup)
        )

    def _render_probe_identifier(self, schema_token, table_token, quoted):
        table_identifier = (
            quoted_name(table_token, True) if quoted else table_token
        )
        rendered_table = self.identifier_preparer.quote(table_identifier)

        if not schema_token:
            return rendered_table

        rendered_schema = self.identifier_preparer.quote_schema(schema_token)
        return "%s.%s" % (rendered_schema, rendered_table)

    def _probe_table_candidates(self, table_name):
        cleaned_name = self._clean_str(table_name)
        if not cleaned_name:
            return []

        if getattr(table_name, "quote", None) is True:
            return [(cleaned_name, True)]

        return [(self._fold_unquoted_lookup_name(cleaned_name), False)]

    def _probe_schema_candidates(self, schema):
        if schema is None:
            return [None]

        cleaned_schema = self._clean_str(schema)
        if not cleaned_schema:
            return [None]

        if getattr(schema, "quote", None) is True:
            return [quoted_name(cleaned_schema, True)]

        schema_candidates = []
        folded_schema = self._fold_unquoted_lookup_name(cleaned_schema)
        for token in (folded_schema, cleaned_schema):
            if token and token not in schema_candidates:
                schema_candidates.append(token)
        return schema_candidates

    def _iter_probe_identifiers(self, table_name, schema=None):
        name_candidates = self._probe_table_candidates(table_name)
        if not name_candidates:
            return

        schema_candidates = self._probe_schema_candidates(schema)

        for schema_token in schema_candidates:
            for table_token, quoted in name_candidates:
                yield self._render_probe_identifier(
                    schema_token,
                    table_token,
                    quoted,
                )

    def _has_table_via_sql_probe(self, connection, table_name, schema=None):
        for from_token in self._iter_probe_identifiers(table_name, schema=schema):
            sql_text = "SELECT COUNT(*) FROM %s" % from_token

            try:
                connection.exec_driver_sql(sql_text).scalar()
                return True
            except exc.DBAPIError:
                continue

        return False

    def _open_dbapi_cursor(self, connection):
        dbapi_connection = self._dbapi_connection(connection)
        if dbapi_connection is None:
            return None
        return dbapi_connection.cursor()

    def _execute_dbapi_probe(self, cursor, from_token):
        sql_text = "SELECT COUNT(*) FROM %s" % from_token
        error_types = self._dbapi_error_types()
        try:
            cursor.execute(sql_text)
            cursor.fetchone()
            return True
        except error_types:
            logger.debug(
                "DBAPI table probe failed for %s",
                from_token,
                exc_info=True,
            )
            return False

    def _has_table_via_dbapi_probe(self, connection, table_name, schema=None):
        probe_identifiers = tuple(
            self._iter_probe_identifiers(table_name, schema=schema)
        )
        if not probe_identifiers:
            return False

        error_types = self._dbapi_error_types()
        try:
            cursor = self._open_dbapi_cursor(connection)
        except error_types:
            logger.debug("Could not open DBAPI table-probe cursor", exc_info=True)
            return False

        if cursor is None:
            return False

        try:
            for from_token in probe_identifiers:
                if self._execute_dbapi_probe(cursor, from_token):
                    return True
        finally:
            self._close_cursor(cursor)

        return False

    def _decode_literal_default(self, default_value, base_code):
        value = self._clean_default_catalog_value(default_value)
        if value is None:
            return None

        if base_code in self._PLAIN_LITERAL_DEFAULT_TYPES:
            return value

        parts = value.split(maxsplit=1)
        if len(parts) != 2:
            return value

        _encoded_value, sql_value = parts
        return sql_value.strip() or value

    def _decode_default(self, default_type, default_value, base_code):
        default_type = self._clean_default_catalog_value(default_type)

        if not default_type:
            return None

        if default_type == "L":
            return self._decode_literal_default(
                default_value,
                base_code,
            )

        if default_type == "T":
            return "TODAY"

        if default_type == "U":
            return "USER"

        if default_type == "N":
            return None

        default_value = self._clean_default_catalog_value(default_value)

        if default_type == "C":
            return (default_value or "CURRENT").strip()

        if default_type == "S":
            return default_value or "DBSERVERNAME"

        return default_value

    def _instantiate_registered_type(self, type_name, args):
        entry = self.ischema_names.get(type_name, self._MISSING)
        if entry is self._MISSING or entry is None:
            return self._MISSING

        try:
            if isinstance(entry, type) or callable(entry):
                return entry(*args)
            return entry
        except TypeError:
            return self._instantiate_registered_without_args(entry)

    def _instantiate_registered_without_args(self, entry):
        if not isinstance(entry, type):
            return self._MISSING

        try:
            return entry()
        except TypeError:
            return self._MISSING

    def _instantiate_fallback_type(self, type_name, args):
        if type_name in self._CHAR_FALLBACK_TYPES:
            return sa_types.CHAR(args[0] if args else None)
        if type_name in self._VARCHAR_FALLBACK_TYPES:
            return sa_types.VARCHAR(args[0] if args else None)
        if type_name in self._NCHAR_FALLBACK_TYPES:
            return sa_types.NCHAR(args[0] if args else None)
        if type_name in self._NVARCHAR_FALLBACK_TYPES:
            return sa_types.NVARCHAR(args[0] if args else None)
        if type_name in self._INTEGER_FALLBACK_TYPES:
            return sa_types.Integer()
        if type_name in self._BIG_INTEGER_FALLBACK_TYPES:
            return sa_types.BigInteger()
        if type_name in self._NUMERIC_FALLBACK_TYPES:
            return self._instantiate_numeric_fallback(args)

        factory = self._SIMPLE_FALLBACK_FACTORIES.get(type_name)
        return factory() if factory is not None else self._MISSING

    def _instantiate_numeric_fallback(self, args):
        if len(args) >= 2:
            return sa_types.Numeric(args[0], args[1])
        return sa_types.Numeric()

    def _instantiate_ischema_type(self, type_name, *args):
        registered = self._instantiate_registered_type(type_name, args)
        if registered is not self._MISSING:
            return registered

        fallback = self._instantiate_fallback_type(type_name, args)
        if fallback is not self._MISSING:
            return fallback

        util.warn(f"Did not recognize Informix type '{type_name}'")
        return sa_types.NullType()

    def _normalized_encoded_length(self, collength):
        encoded_len = int(collength) if collength is not None else 0
        if encoded_len < 0:
            encoded_len += 65536
        return encoded_len

    def _ifx_type_result(self, type_name, autoincrement, nullable, *args):
        return (
            self._instantiate_ischema_type(type_name, *args),
            autoincrement,
            nullable,
        )

    def _fetch_extended_type_metadata(self, connection, extended_id):
        row = connection.exec_driver_sql(
            """
            SELECT
                x.extended_id,
                x.mode,
                x.owner,
                x.name,
                x.type,
                x.source,
                x.maxlen,
                x.length,
                x.locator
            FROM sysxtdtypes x
            WHERE x.extended_id = ?
            """,
            (int(extended_id),),
        ).first()
        if row is None:
            return None
        return {
            "extended_id": int(row[0]),
            "mode": self._clean_str(row[1]),
            "owner": self._clean_str(row[2]),
            "name": self._clean_str(row[3]),
            "type": int(row[4]) if row[4] is not None else 0,
            "source": int(row[5]) if row[5] is not None else 0,
            "maxlen": int(row[6]) if row[6] is not None else 0,
            "length": int(row[7]) if row[7] is not None else 0,
            "locator": int(row[8]) if row[8] is not None else 0,
        }

    def _fetch_attribute_type_rows(self, connection, extended_id):
        rows = connection.exec_driver_sql(
            """
            SELECT
                a.seqno,
                a.levelno,
                a.parent_no,
                a.fieldname,
                a.fieldno,
                a.type,
                a.length,
                a.xtd_type_id
            FROM sysattrtypes a
            WHERE a.extended_id = ?
            ORDER BY a.seqno
            """,
            (int(extended_id),),
        ).fetchall()
        result = []
        for row in rows:
            result.append(
                {
                    "extended_id": int(extended_id),
                    "seqno": int(row[0]),
                    "levelno": int(row[1]),
                    "parent_no": int(row[2]),
                    "fieldname": self._clean_str(row[3]),
                    "fieldno": int(row[4]) if row[4] is not None else 0,
                    "type": int(row[5]) if row[5] is not None else 0,
                    "length": int(row[6]) if row[6] is not None else 0,
                    "xtd_type_id": int(row[7]) if row[7] is not None else 0,
                }
            )
        return result

    def _build_complex_attribute_tree(self, rows, *, extended_id):
        """Normalize the SYSATTRTYPES hierarchy for one complex type.

        Informix documents ``parent_no`` as the ``seqno`` of the containing
        complex node.  Current 14.10/15.x catalogs normally follow that rule,
        but collection element rows can also be emitted with ``parent_no``
        equal to their own ``seqno``.  The latter is not a usable tree edge,
        although ``levelno`` and the catalog's preorder ``seqno`` still
        identify the parent unambiguously.

        Prefer a valid explicit parent.  For self-references, missing parents,
        or level-inconsistent references, recover the edge from the nearest
        preceding node at ``levelno - 1``.  This preserves the documented
        representation while accepting the native collection layout observed
        on Informix 15.0 and compatible 14.10 servers.
        """
        ordered = sorted(rows, key=lambda item: item["seqno"])
        if not ordered:
            raise exc.UnreflectableTableError(
                "Informix complex type has no SYSATTRTYPES rows: "
                f"extended_id={extended_id!r}"
            )

        by_seqno = {}
        for row in ordered:
            seqno = row["seqno"]
            if seqno in by_seqno:
                raise exc.UnreflectableTableError(
                    "Informix complex type contains duplicate SYSATTRTYPES "
                    f"seqno={seqno!r}; extended_id={extended_id!r}"
                )
            by_seqno[seqno] = row

        minimum_level = min(item["levelno"] for item in ordered)
        roots = [item for item in ordered if item["levelno"] == minimum_level]
        if len(roots) != 1:
            raise exc.UnreflectableTableError(
                "Informix complex type catalog must contain exactly one root; "
                f"extended_id={extended_id!r}, roots="
                f"{[item['seqno'] for item in roots]!r}"
            )
        root = roots[0]

        children_by_parent = {}
        preceding_by_level = {root["levelno"]: root}
        root_seen = False

        for row in ordered:
            if row is root:
                root_seen = True
                continue
            if not root_seen:
                raise exc.UnreflectableTableError(
                    "Informix complex type root does not precede its members; "
                    f"extended_id={extended_id!r}, root_seqno={root['seqno']!r}"
                )

            levelno = row["levelno"]
            expected_parent_level = levelno - 1
            explicit_parent = by_seqno.get(row["parent_no"])
            explicit_parent_is_valid = (
                explicit_parent is not None
                and explicit_parent["seqno"] != row["seqno"]
                and explicit_parent["seqno"] < row["seqno"]
                and explicit_parent["levelno"] == expected_parent_level
            )

            if explicit_parent_is_valid:
                parent = explicit_parent
            else:
                parent = preceding_by_level.get(expected_parent_level)

            if parent is None:
                raise exc.UnreflectableTableError(
                    "Unable to resolve Informix complex type parent from "
                    "SYSATTRTYPES; "
                    f"extended_id={extended_id!r}, seqno={row['seqno']!r}, "
                    f"levelno={levelno!r}, parent_no={row['parent_no']!r}"
                )

            children_by_parent.setdefault(parent["seqno"], []).append(row)

            for stale_level in tuple(preceding_by_level):
                if stale_level >= levelno:
                    del preceding_by_level[stale_level]
            preceding_by_level[levelno] = row

        return root, children_by_parent

    def _reflect_attr_scalar_type(
        self,
        connection,
        node,
        cache,
        stack,
    ):
        xtd_type_id = node["xtd_type_id"]
        if xtd_type_id:
            return self._reflect_extended_type(
                connection,
                xtd_type_id,
                cache=cache,
                stack=stack,
            )
        reflected, _autoincrement, _nullable = self._decode_ifx_type(
            node["type"],
            node["length"],
        )
        return reflected

    def _reflect_attr_node(
        self,
        connection,
        node,
        children_by_parent,
        cache,
        stack,
    ):
        # A nonzero xtd_type_id identifies a separately registered named ROW,
        # DISTINCT, opaque, or complex type.  It takes precedence over the
        # low-byte type code; otherwise named ROW fields would be reconstructed
        # incorrectly as anonymous rows with no local children.
        if node["xtd_type_id"]:
            return self._reflect_extended_type(
                connection,
                node["xtd_type_id"],
                cache=cache,
                stack=stack,
            )

        base_code = int(node["type"]) & 0x00FF
        children = children_by_parent.get(node["seqno"], ())

        if base_code in (19, 20, 21):
            if len(children) != 1:
                raise exc.UnreflectableTableError(
                    "Informix collection catalog entry must contain exactly "
                    f"one element node; extended_id={node.get('extended_id')!r}, "
                    f"seqno={node['seqno']!r}"
                )
            element_type = self._reflect_attr_node(
                connection,
                children[0],
                children_by_parent,
                cache,
                stack,
            )
            collection_cls = {19: SET, 20: MULTISET, 21: LIST}[base_code]
            return collection_cls(element_type)

        if base_code == 22:
            ordered_children = sorted(
                children,
                key=lambda item: (item["fieldno"], item["seqno"]),
            )
            fields = []
            for index, child in enumerate(ordered_children, start=1):
                field_name = child["fieldname"] or f"field_{index}"
                field_type = self._reflect_attr_node(
                    connection,
                    child,
                    children_by_parent,
                    cache,
                    stack,
                )
                nullable = not bool(int(child["type"]) & 0x0100)
                fields.append(RowField(field_name, field_type, nullable))
            return ROW(tuple(fields))

        return self._reflect_attr_scalar_type(
            connection,
            node,
            cache,
            stack,
        )

    def _reflect_complex_attributes(
        self,
        connection,
        metadata,
        cache,
        stack,
    ):
        rows = self._fetch_attribute_type_rows(
            connection,
            metadata["extended_id"],
        )
        if not rows:
            raise exc.UnreflectableTableError(
                "Informix complex type has no SYSATTRTYPES rows: "
                f"extended_id={metadata['extended_id']!r}, "
                f"name={metadata['name']!r}"
            )
        root, children_by_parent = self._build_complex_attribute_tree(
            rows,
            extended_id=metadata["extended_id"],
        )
        reflected = self._reflect_attr_node(
            connection,
            root,
            children_by_parent,
            cache,
            stack,
        )
        if metadata["mode"] == "R":
            if not isinstance(reflected, ROW):
                raise exc.UnreflectableTableError(
                    "Named Informix ROW type catalog root is not ROW: "
                    f"{metadata['name']!r}"
                )
            reflected = ROW(
                reflected.fields,
                name=metadata["name"],
                owner=metadata["owner"],
            )
        return reflected

    def _reflect_extended_type(
        self,
        connection,
        extended_id,
        *,
        cache=None,
        stack=(),
    ):
        extended_id = int(extended_id)
        if cache is None:
            cache = {}
        if extended_id in cache:
            return cache[extended_id]
        if extended_id in stack:
            raise exc.UnreflectableTableError(
                "Recursive Informix extended type dependency detected: "
                + " -> ".join(map(str, (*stack, extended_id)))
            )

        metadata = self._fetch_extended_type_metadata(connection, extended_id)
        if metadata is None:
            util.warn(
                "Informix SYSXTDTYPES entry not found for "
                f"extended_id={extended_id!r}"
            )
            return sa_types.NullType()

        next_stack = (*stack, extended_id)
        mode = metadata["mode"]
        if mode in {"C", "R"}:
            reflected = self._reflect_complex_attributes(
                connection,
                metadata,
                cache,
                next_stack,
            )
        elif mode == "D":
            source_id = metadata["source"]
            if source_id:
                source_type = self._reflect_extended_type(
                    connection,
                    source_id,
                    cache=cache,
                    stack=next_stack,
                )
            else:
                source_code = int(metadata["type"]) - 0x0800
                if source_code < 0:
                    source_code = int(metadata["type"]) & 0x00FF
                source_type, _autoincrement, _nullable = self._decode_ifx_type(
                    source_code,
                    metadata["length"] or metadata["maxlen"],
                )
            reflected = DISTINCT(
                metadata["name"],
                source_type,
                owner=metadata["owner"],
            )
        else:
            reflected, _autoincrement, _nullable = self._decode_ifx_type(
                metadata["type"],
                metadata["length"] or metadata["maxlen"],
                extended_id=metadata["extended_id"],
                extended_type_name=metadata["name"],
                extended_maxlen=metadata["maxlen"],
            )
        cache[extended_id] = reflected
        return reflected

    def _unknown_ifx_type_result(
        self,
        coltype,
        base_code,
        extended_id,
        autoincrement,
        nullable,
    ):
        util.warn(
            "Did not recognize Informix coltype code "
            f"{coltype!r} (base={base_code}, extended_id={extended_id!r})"
        )
        return sa_types.NullType(), autoincrement, nullable

    def _decode_opaque_ifx_type(
        self,
        base_code,
        encoded_len,
        extended_id,
        extended_type_name,
        extended_maxlen,
        autoincrement,
        nullable,
    ):
        opaque_type_name = self._resolve_opaque_type_name(
            extended_type_name,
            base_code,
        )

        if opaque_type_name is None:
            util.warn(
                "Did not recognize Informix opaque type "
                f"extended_id={extended_id!r}, "
                f"name={extended_type_name!r}"
            )
            return sa_types.NullType(), autoincrement, nullable

        if opaque_type_name == "LVARCHAR":
            # For a native LVARCHAR column, SYSCOLUMNS.collength stores
            # the declared maximum or 2048 when the declaration omitted
            # it. SYSXTDTYPES.maxlen describes the opaque type domain and
            # must not overwrite that column-specific value.
            if base_code == 40:
                length = encoded_len or extended_maxlen or None
            else:
                length = extended_maxlen or encoded_len or None
            if length is not None:
                length = self._int_or_default(length, None)
            return self._ifx_type_result(
                "LVARCHAR",
                autoincrement,
                nullable,
                length,
            )

        return self._ifx_type_result(opaque_type_name, autoincrement, nullable)

    def _decode_temporal_ifx_type(
        self,
        type_name,
        encoded_len,
        autoincrement,
        nullable,
        interval_metadata=None,
    ):
        qualifiers = self._decode_datetime_qualifiers(encoded_len)
        first_code = qualifiers["first_code"]
        last_code = qualifiers["last_code"]

        if type_name == "INTERVAL":
            interval_metadata = interval_metadata or {}
            satype = INTERVAL.from_catalog(
                first_code=first_code,
                last_code=last_code,
                storage_length=qualifiers["length"],
                odbc_column_size=interval_metadata.get("column_size"),
                odbc_decimal_digits=interval_metadata.get("decimal_digits"),
            )
            setattr(satype, "_informix_qualifiers", qualifiers)
            setattr(satype, "_informix_odbc_metadata", interval_metadata)
            return satype, autoincrement, nullable

        if 11 <= last_code <= 15:
            fraction_digits = last_code - 10
        else:
            fraction_digits = 0

        # DATETIME HOUR TO ... represents a time without a date.
        if first_code == 6:
            satype = IFXTime(fraction_digits=fraction_digits)
        else:
            satype = IFXDateTime(fraction_digits=fraction_digits)

        setattr(satype, "_informix_qualifiers", qualifiers)
        return satype, autoincrement, nullable

    def _ifx_type_args(self, base_code, encoded_len):
        if base_code in (0, 15):
            return [encoded_len]
        if base_code in (13, 16):
            return [encoded_len & 0x00FF]
        if base_code in (5, 8):
            return [encoded_len >> 8, encoded_len & 0x00FF]
        return []

    def _decode_ifx_type(
        self,
        coltype,
        collength,
        extended_id=None,
        extended_type_name=None,
        extended_maxlen=None,
        interval_metadata=None,
    ):
        coltype_int = int(coltype)
        nullable = not bool(coltype_int & 0x0100)
        base_code = coltype_int & 0x00FF
        encoded_len = self._normalized_encoded_length(collength)
        type_name = self._COLTYPE_CODE_MAP.get(base_code)
        autoincrement = base_code in (6, 18, 53)

        if type_name is None:
            return self._unknown_ifx_type_result(
                coltype,
                base_code,
                extended_id,
                autoincrement,
                nullable,
            )

        if base_code in (40, 41, 45):
            return self._decode_opaque_ifx_type(
                base_code,
                encoded_len,
                extended_id,
                extended_type_name,
                extended_maxlen,
                autoincrement,
                nullable,
            )

        if base_code in (10, 14):
            return self._decode_temporal_ifx_type(
                type_name,
                encoded_len,
                autoincrement,
                nullable,
                interval_metadata=interval_metadata,
            )

        return self._ifx_type_result(
            type_name,
            autoincrement,
            nullable,
            *self._ifx_type_args(base_code, encoded_len),
        )

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        original_name = table_name
        original_schema = schema
        try:
            table_name, schema, _synonym = self._resolve_reflection_target(
                connection,
                table_name,
                schema,
                kw,
            )
        except _RemoteSynonymReflectionError:
            # A remote synonym is still a registered object, even though the
            # current database catalogs cannot provide complete Table
            # autoload metadata for its target.
            return self.has_synonym(
                connection,
                original_name,
                schema=original_schema,
            )
        row = self._get_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T", "V"),
        )
        if row is not None:
            return True

        # TEMP TABLES can be connection-local and not always discoverable
        # through systables in the same way as permanent objects.
        if self._has_table_via_sql_probe(
            connection,
            table_name,
            schema=schema,
        ):
            return True

        if self._has_table_via_dbapi_probe(
            connection,
            table_name,
            schema=schema,
        ):
            return True

        # SQLTables without an explicit owner can return a table with the
        # requested name from another owner in an ANSI database.  That would
        # make has_table(name) report a cross-schema false positive.
        if schema is None:
            return False

        return self._has_table_via_odbc(
            connection,
            table_name,
            schema=schema,
        )

    @reflection.cache
    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        _ = kw
        owner = self._resolved_owner(schema)
        sql_text = """
            SELECT FIRST 1 s.seqid
            FROM syssequences s
            JOIN systables t
              ON t.tabid = s.tabid
            WHERE LOWER(t.tabname) = LOWER(?)
              AND LOWER(t.owner) = LOWER(?)
        """
        row = connection.exec_driver_sql(sql_text, (sequence_name, owner)).first()
        return row is not None

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        """List object owners and users capable of owning objects."""

        _ = kw

        object_owner_rows = connection.exec_driver_sql(
            """
            SELECT DISTINCT t.owner
            FROM systables t
            WHERE t.owner IS NOT NULL
            ORDER BY t.owner
            """
        ).fetchall()

        authorization_rows = connection.exec_driver_sql(
            """
            SELECT u.username
            FROM sysusers u
            WHERE u.username IS NOT NULL
            AND u.usertype IN ('D', 'R')
            ORDER BY u.username
            """
        ).fetchall()

        owners = {}

        for row in (*object_owner_rows, *authorization_rows):
            owner = self.normalize_name(
                self._clean_str(row[0])
            )

            if owner is not None:
                owners[str(owner)] = owner

        return [
            owners[key]
            for key in sorted(
                owners,
                key=str.casefold,
            )
        ]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        _ = kw
        owner = self._resolved_owner(schema)
        sql_text = """
            SELECT t.tabname
            FROM systables t
            WHERE LOWER(t.owner) = LOWER(?)
              AND t.tabtype = 'T'
              AND t.tabid >= 100
              AND LOWER(t.tabname) NOT IN (
                  'ifx_sqla_table_comments',
                  'ifx_sqla_column_comments'
              )
            ORDER BY t.tabname
        """
        rows = connection.exec_driver_sql(sql_text, (owner,)).fetchall()
        return [self.normalize_name(self._clean_str(r[0])) for r in rows]

    def _empty_reflection_names(self, object_kind):
        _ = object_kind
        return []

    def get_temp_table_names(self, connection, schema=None, **kw):
        _ = (connection, schema, kw)
        # Informix temp tables are connection-local and, with the ODBC
        # driver used by this dialect, are not exposed through a stable
        # catalog query or metadata API that lets us enumerate them
        # reliably. We therefore keep the contract explicit: has_table()
        # works for known temp names on the same connection, but listing
        # temp table names is not supported and returns an empty list.
        return self._empty_reflection_names("temporary tables")

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        _ = kw
        owner = self._resolved_owner(schema)
        sql_text = """
            SELECT t.tabname
            FROM systables t
            WHERE LOWER(t.owner) = LOWER(?)
              AND t.tabtype = 'V'
              AND t.tabid >= 100
            ORDER BY t.tabname
        """
        rows = connection.exec_driver_sql(sql_text, (owner,)).fetchall()
        return [self.normalize_name(self._clean_str(r[0])) for r in rows]

    def _extend_names_for_kinds(self, names, connection, schema, kind, getters, kw):
        for object_kind, getter in getters:
            if object_kind in kind:
                names.extend(getter(connection, schema=schema, **kw))

    def _filtered_unique_names(self, names, filter_names):
        if not filter_names:
            return list(dict.fromkeys(names))

        filtered_names = []
        for name in names:
            reflected_name = self._matched_filter_name(name, filter_names)
            if reflected_name is not None:
                filtered_names.append(reflected_name)
        return list(dict.fromkeys(filtered_names))

    def _matched_filter_name(self, name, filter_names):
        name_variants = {
            name,
            self.normalize_name(name),
            self.denormalize_name(name),
        }

        for filter_name in filter_names:
            filter_variants = {
                self.normalize_name(filter_name),
                self.denormalize_name(filter_name),
                str(filter_name),
            }
            if name_variants.isdisjoint(filter_variants):
                continue

            if getattr(filter_name, "quote", None) is True:
                return quoted_name(str(filter_name), True)
            return name

        return None

    def _table_names_for_multi(
        self,
        connection,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        if filter_names and scope is ObjectScope.ANY and kind is ObjectKind.ANY:
            return list(filter_names)

        names = []
        if ObjectScope.DEFAULT in scope:
            self._extend_names_for_kinds(
                names,
                connection,
                schema,
                kind,
                (
                    (ObjectKind.TABLE, self.get_table_names),
                    (ObjectKind.VIEW, self.get_view_names),
                    (ObjectKind.MATERIALIZED_VIEW, self.get_materialized_view_names),
                ),
                kw,
            )

        if ObjectScope.TEMPORARY in scope:
            self._extend_names_for_kinds(
                names,
                connection,
                schema,
                kind,
                (
                    (ObjectKind.TABLE, self.get_temp_table_names),
                    (ObjectKind.VIEW, self.get_temp_view_names),
                ),
                kw,
            )

        return self._filtered_unique_names(names, filter_names)

    def get_materialized_view_names(self, connection, schema=None, **kw):
        _ = (connection, schema, kw)
        # Informix does not expose a materialized-view concept through this
        # dialect, so the contract is explicit and empty.
        return self._empty_reflection_names("materialized views")

    @staticmethod
    def _normalize_check_catalog_text(sqltext):
        """Remove Informix catalog formatting without changing SQL literals.

        Informix adds whitespace immediately before closing parentheses when
        it stores CHECK expressions in ``syschecks``. That whitespace is not
        semantically relevant, but it prevents SQLAlchemy's compliance suite
        from comparing the reflected expression with the original one.

        Quoted strings and quoted identifiers are preserved verbatim.
        """
        if sqltext is None:
            return ""

        text = str(sqltext)
        normalized = []
        quote = None
        index = 0

        while index < len(text):
            character = text[index]

            if quote is not None:
                quote, index = (
                    IfxReflector._append_quoted_check_character(
                        normalized,
                        text,
                        index,
                        quote,
                    )
                )
                continue

            if character in ("'", '"'):
                quote = character
                normalized.append(character)
                index += 1
                continue

            if character == ")":
                IfxReflector._remove_trailing_whitespace(normalized)

            normalized.append(character)
            index += 1

        return "".join(normalized).rstrip()

    @staticmethod
    def _append_quoted_check_character(
        normalized,
        text,
        index,
        quote,
    ):
        """Append a quoted character, preserving doubled quote escapes."""
        character = text[index]
        normalized.append(character)

        if character != quote:
            return quote, index + 1

        # SQL escapes a quote by doubling it:
        # 'it''s valid' or "quoted""identifier".
        if index + 1 < len(text) and text[index + 1] == quote:
            normalized.append(text[index + 1])
            return quote, index + 2

        return None, index + 1

    @staticmethod
    def _remove_trailing_whitespace(characters):
        """Remove catalog-added whitespace before a closing parenthesis."""
        while characters and characters[-1].isspace():
            characters.pop()

    @staticmethod
    def _check_constraint_sort_key(constraint):
        """Return a stable semantic ordering key for reflected CHECKs."""
        sqltext = constraint.get("sqltext") or ""

        comparable_text = " ".join(
            sqltext
            .replace("(", " ")
            .replace(")", " ")
            .replace("`", " ")
            .split()
        ).casefold()

        name = constraint.get("name")

        return (
            comparable_text,
            str(name).casefold() if name is not None else "",
        )

    @reflection.cache
    def get_check_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kw,
    ):
        """Reflect CHECK constraints defined on an Informix table.

        Informix stores the textual representation of each CHECK constraint
        in multiple fixed-width rows of ``syschecks``. The rows must be
        concatenated in ``seqno`` order before removing final catalog padding.
        """
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )

        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T",),
        )
        tabid = int(table_row[0])

        sql_text = """
            SELECT
                c.constrid,
                c.constrname,
                ch.seqno,
                ch.checktext
            FROM sysconstraints c
            JOIN syschecks ch
            ON ch.constrid = c.constrid
            WHERE c.tabid = ?
            AND c.constrtype = 'C'
            AND UPPER(TRIM(ch.type)) = 'T'
            ORDER BY
                c.constrid,
                ch.seqno
        """

        rows = connection.exec_driver_sql(
            sql_text,
            (tabid,),
        ).fetchall()

        constraints = []

        current_id = None
        current_name = None
        current_parts = []

        def append_current():
            if current_id is None:
                return

            # Do not strip individual CHAR(32) fragments: a space at the end
            # of a non-final fragment can be part of the original expression.
            raw_sqltext = "".join(current_parts).rstrip()

            sqltext = self._normalize_check_catalog_text(
                raw_sqltext
            )

            constraints.append(
                {
                    "name": self._logical_reflected_name(
                        current_name,
                        schema=schema,
                    ),
                    "sqltext": sqltext,
                }
            )

        for constrid, constrname, _seqno, checktext in rows:
            constrid = int(constrid)

            if current_id is not None and constrid != current_id:
                append_current()
                current_parts = []

            if constrid != current_id:
                current_id = constrid
                current_name = constrname

            part = self._coerce_name(checktext)

            if part is not None:
                current_parts.append(part)

        append_current()

        constraints.sort(
            key=self._check_constraint_sort_key
        )

        return constraints

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T", "V"),
        )
        tabid = int(table_row[0])
        physical_table_name = self._clean_str(table_row[1])
        physical_owner = self._clean_str(table_row[2])
        return {
            "text": self._table_comment_for_tabid(
                connection,
                tabid,
                physical_owner,
                physical_table_name,
            )
        }

    @staticmethod
    def _positive_catalog_int(value):
        """Return a positive catalog integer, or ``None`` when absent."""
        try:
            converted = int(value)
        except (TypeError, ValueError):
            return None

        return converted if converted > 0 else None

    def _table_options_from_catalog_row(self, row):
        """Translate one SYSTABLES row to SQLAlchemy dialect options."""
        if row is None:
            return {}

        tabtype = (self._clean_str(row[0]) or "").upper()
        if tabtype != "T":
            # Views participate in SQLAlchemy's multi-reflection API, but
            # Informix storage and lock options apply only to base tables.
            return {}

        options = {}
        lock_code = (self._clean_str(row[1]) or "").upper()
        lock_level = self._TABLE_LOCK_LEVELS.get(lock_code)
        if lock_level is not None:
            options["informix_lock_level"] = _ReflectedTableLockLevel(
                lock_level
            )

        for key, raw_value in (
            ("informix_first_extent", row[2]),
            ("informix_next_extent", row[3]),
        ):
            value = self._positive_catalog_int(raw_value)
            if value is not None:
                options[key] = value

        page_size = self._positive_catalog_int(row[4])
        if page_size is not None:
            options["informix_page_size"] = _ReflectedTablePageSize(
                page_size
            )

        return options

    def _fragment_rows(self, connection, tabid, *, index_name=None):
        """Read ordered table/index fragmentation metadata.

        ``exprtext`` is a TEXT catalog column.  The Informix ODBC driver can
        return it as a Python string directly, avoiding a large LVARCHAR cast
        that could itself approach the maximum row size.
        """
        if not hasattr(connection, "exec_driver_sql"):
            # Some isolated reflection-unit tests use a sentinel connection
            # while monkeypatching all catalog readers. Fragmentation is an
            # additive metadata layer, so absence of a DBAPI facade means
            # simply that no fragment rows are available in that test.
            return []

        if index_name is None:
            sql_text = """
                SELECT
                    f.fragtype,
                    f.indexname,
                    f.strategy,
                    f.evalpos,
                    f.exprtext,
                    f.dbspace,
                    f.partition,
                    f.flags
                FROM sysfragments f
                WHERE f.tabid = ?
                  AND f.fragtype = 'T'
                ORDER BY f.evalpos
            """
            parameters = (tabid,)
        else:
            sql_text = """
                SELECT
                    f.fragtype,
                    f.indexname,
                    f.strategy,
                    f.evalpos,
                    f.exprtext,
                    f.dbspace,
                    f.partition,
                    f.flags
                FROM sysfragments f
                WHERE f.tabid = ?
                  AND f.fragtype = 'I'
                  AND f.indexname = ?
                ORDER BY f.evalpos
            """
            parameters = (tabid, self._clean_str(index_name))
        result = connection.exec_driver_sql(sql_text, parameters)
        if hasattr(result, "fetchall"):
            return result.fetchall()
        if hasattr(result, "all"):
            return result.all()
        return []

    def _sysfragexprudrdep_columns(self, connection):
        """Discover optional SYSFRAGEXPRUDRDEP columns across server levels."""
        rows = connection.exec_driver_sql(
            """
            SELECT c.colname
            FROM systables t
            JOIN syscolumns c ON c.tabid = t.tabid
            WHERE t.tabname = 'sysfragexprudrdep'
            ORDER BY c.colno
            """
        ).fetchall()
        return {
            (self._clean_str(row[0]) or "").strip().lower()
            for row in rows
            if row and self._clean_str(row[0])
        }

    def _fragment_udr_dependencies(self, connection, tabid, *, index_name=None):
        """Return UDR names keyed by fragment evaluation position.

        The catalog table is present only when the server tracks UDRs used by
        fragmentation expressions.  Its layout has varied across Informix
        releases, so reflection discovers the available columns and queries
        only a conservative, documented subset.
        """
        columns = self._sysfragexprudrdep_columns(connection)
        if "procid" not in columns or "tabid" not in columns:
            return {}
        if index_name is not None and "indexname" not in columns:
            return {}

        select_columns = []
        if "evalpos" in columns:
            select_columns.append("d.evalpos")
        else:
            select_columns.append("CAST(NULL AS INTEGER) AS evalpos")
        select_columns.extend(("p.procname", "p.owner"))

        predicates = []
        parameters = []
        predicates.append("d.tabid = ?")
        parameters.append(tabid)
        if index_name is not None and "indexname" in columns:
            predicates.append("d.indexname = ?")
            parameters.append(self._clean_str(index_name))
        elif index_name is None and "fragtype" in columns:
            predicates.append("d.fragtype = 'T'")

        sql_text = (
            "SELECT "
            + ", ".join(select_columns)
            + " FROM sysfragexprudrdep d "
            + "JOIN sysprocedures p ON p.procid = d.procid"
        )
        if predicates:
            sql_text += " WHERE " + " AND ".join(predicates)

        try:
            rows = connection.exec_driver_sql(
                sql_text,
                tuple(parameters),
            ).fetchall()
        except exc.DBAPIError as err:  # pragma: no cover - server-version fallback
            logger.debug(
                "SYSFRAGEXPRUDRDEP reflection failed; omitting UDR details",
                exc_info=True,
            )
            util.warn(
                "Could not reflect SYSFRAGEXPRUDRDEP metadata; "
                f"fragment expressions remain available without UDR details: {err}"
            )
            return {}

        dependencies = {}
        for evalpos, procname, owner in rows:
            name = self._qualified_catalog_name(procname, owner)
            if name is None:
                continue
            key = int(evalpos) if evalpos is not None else None
            dependencies.setdefault(key, []).append(name)
        return {
            key: tuple(dict.fromkeys(values))
            for key, values in dependencies.items()
        }

    @staticmethod
    def _strip_catalog_selector_prefix(value, prefixes):
        text = (value or "").strip()
        upper = text.upper()
        for prefix in prefixes:
            if upper.startswith(prefix):
                return text[len(prefix):].strip()
        return text

    @staticmethod
    def _catalog_dbspace_list(value):
        """Decode the simple identifier list stored at evalpos=-1."""
        text = (value or "").strip()
        text = re.sub(r"^STORE\s+IN\s*", "", text, flags=re.I)
        if text.startswith("(") and text.endswith(")"):
            text = text[1:-1]
        result = []
        for item in text.split(","):
            item = item.strip()
            if not item:
                continue
            if len(item) >= 2 and item[0] == item[-1] == '"':
                item = item[1:-1].replace('""', '"')
            result.append(item)
        return tuple(result)

    def _reflected_fragment_expression(self, text, dependencies=()):
        cleaned = (self._clean_str(text) or "").strip()
        if not cleaned:
            return None
        return _ReflectedFragmentExpression(cleaned, tuple(dependencies))

    def _fragmentation_from_rows(self, rows, dependencies=None):
        """Build one immutable public fragmentation structure."""
        if not rows:
            return None, None

        dependencies = dependencies or {}
        strategy = (self._clean_str(rows[0][2]) or "").upper()
        if strategy not in self._FRAGMENT_STRATEGIES:
            util.warn(f"Unsupported SYSFRAGMENTS strategy {strategy!r}")
            return None, None

        normalized = []
        for row in rows:
            evalpos = int(row[3]) if row[3] is not None else 0
            normalized.append(
                {
                    "evalpos": evalpos,
                    "exprtext": self._clean_str(row[4]),
                    "dbspace": self._clean_str(row[5]),
                    "partition": self.normalize_name(row[6]) if row[6] else None,
                    "flags": int(row[7]) if len(row) > 7 and row[7] is not None else 0,
                }
            )

        if strategy == "I":
            dbspace = next(
                (item["dbspace"] for item in normalized if item["dbspace"]),
                None,
            )
            return None, dbspace

        if strategy == "T":
            return AttachedIndexFragmentation(), None

        fragment_rows = [item for item in normalized if item["evalpos"] >= 0]

        if strategy == "R":
            named = any(item["partition"] for item in fragment_rows)
            if named:
                fragments = tuple(
                    Fragment(
                        name=item["partition"],
                        dbspace=item["dbspace"],
                    )
                    for item in fragment_rows
                )
                return RoundRobinFragmentation(fragments=fragments), None
            dbspaces = tuple(item["dbspace"] for item in fragment_rows)
            return RoundRobinFragmentation(dbspaces=dbspaces), None

        if strategy == "E":
            fragments = []
            for item in fragment_rows:
                text = (item["exprtext"] or "").strip()
                normalized_selector = re.sub(r"\s+", " ", text.upper())
                if normalized_selector == "REMAINDER":
                    fragment = Fragment(
                        name=item["partition"],
                        dbspace=item["dbspace"],
                        remainder=True,
                    )
                elif normalized_selector in {
                    "NULL",
                    "VALUES (NULL)",
                    "VALUES IS NULL",
                }:
                    fragment = Fragment(
                        name=item["partition"],
                        dbspace=item["dbspace"],
                        is_null=True,
                    )
                else:
                    expression = self._reflected_fragment_expression(
                        text,
                        dependencies.get(item["evalpos"], dependencies.get(None, ())),
                    )
                    if expression is None:
                        continue
                    fragment = Fragment(
                        name=item["partition"],
                        dbspace=item["dbspace"],
                        expression=expression,
                    )
                fragments.append(fragment)
            return ExpressionFragmentation(tuple(fragments)), None

        key_row = next(
            (item for item in normalized if item["evalpos"] == -3),
            None,
        )
        if key_row is None:
            util.warn(
                "SYSFRAGMENTS did not return the fragmentation key row "
                f"for strategy {strategy!r}"
            )
            return None, None
        key = self._reflected_fragment_expression(
            key_row["exprtext"],
            dependencies.get(-3, dependencies.get(None, ())),
        )
        if key is None:
            return None, None

        if strategy == "L":
            fragments = []
            for item in fragment_rows:
                text = (item["exprtext"] or "").strip()
                normalized_selector = re.sub(r"\s+", " ", text.upper())
                common = {
                    "name": item["partition"],
                    "dbspace": item["dbspace"],
                }
                if normalized_selector == "REMAINDER":
                    fragments.append(Fragment(remainder=True, **common))
                    continue
                if normalized_selector in {
                    "NULL",
                    "VALUES (NULL)",
                    "VALUES IS NULL",
                }:
                    fragments.append(Fragment(is_null=True, **common))
                    continue
                selector = self._reflected_fragment_expression(
                    text,
                    dependencies.get(item["evalpos"], dependencies.get(None, ())),
                )
                if selector is None:
                    continue
                fragments.append(
                    Fragment(
                        _catalog_selector=selector,
                        **common,
                    )
                )
            return ListFragmentation(key=key, fragments=tuple(fragments)), None

        if strategy == "N":
            interval_row = next(
                (item for item in normalized if item["evalpos"] == -2),
                None,
            )
            if interval_row is None:
                util.warn("SYSFRAGMENTS did not return the interval-size row")
                return None, None
            interval = self._reflected_fragment_expression(
                interval_row["exprtext"],
                dependencies.get(-2, dependencies.get(None, ())),
            )
            store_row = next(
                (item for item in normalized if item["evalpos"] == -1),
                None,
            )
            store_in = (
                self._catalog_dbspace_list(store_row["exprtext"])
                if store_row is not None
                else ()
            )
            fragments = []
            for item in fragment_rows:
                text = (item["exprtext"] or "").strip()
                normalized_selector = re.sub(r"\s+", " ", text.upper())
                common = {
                    "name": item["partition"],
                    "dbspace": item["dbspace"],
                }
                if normalized_selector in {
                    "NULL",
                    "VALUES (NULL)",
                    "VALUES IS NULL",
                }:
                    fragments.append(Fragment(is_null=True, **common))
                    continue
                selector = self._reflected_fragment_expression(
                    text,
                    dependencies.get(item["evalpos"], dependencies.get(None, ())),
                )
                if selector is None:
                    continue
                fragments.append(
                    Fragment(
                        _catalog_selector=selector,
                        **common,
                    )
                )
            return RangeIntervalFragmentation(
                key=key,
                interval=interval,
                fragments=tuple(fragments),
                store_in=store_in,
            ), None

        return None, None

    def _reflect_partial_index(self, connection, tabid, *, index_name):
        """Return a trusted predicate and dbspace for a native partial index.

        Informix implements partial indexes as ``FRAGMENT BY EXPRESSION``
        with one indexed fragment and one ``INDEX OFF`` fragment.  The dialect
        emits the grammar-complete form ``REMAINDER IN <dbspace> INDEX OFF``
        with deterministic partition names.  Older server/catalog variants can
        instead expose ``INDEX OFF`` in the dbspace field, so reflection accepts
        both shapes.
        """
        rows = self._fragment_rows(connection, tabid, index_name=index_name)
        if not rows:
            return None, None
        strategy = (self._clean_str(rows[0][2]) or "").upper()
        if strategy != "E":
            return None, None

        active = None
        index_off = False
        for row in rows:
            evalpos = int(row[3]) if row[3] is not None else 0
            if evalpos < 0:
                continue
            exprtext = (self._clean_str(row[4]) or "").strip()
            dbspace = (self._clean_str(row[5]) or "").strip()
            partition = (self._clean_str(row[6]) or "").strip()
            normalized_expr = re.sub(r"\s+", " ", exprtext.upper())
            normalized_space = re.sub(r"\s+", " ", dbspace.upper())
            normalized_partition = partition.casefold()
            dialect_generated_off_partition = normalized_partition.endswith(
                "__ifx_off"
            )
            legacy_index_off = normalized_space == "INDEX OFF"
            inline_index_off = normalized_expr.endswith(" INDEX OFF")
            if normalized_expr.startswith("REMAINDER") and (
                legacy_index_off
                or inline_index_off
                or dialect_generated_off_partition
            ):
                index_off = True
                continue
            if (
                normalized_space != "INDEX OFF"
                and not normalized_expr.startswith("REMAINDER")
            ):
                active = (exprtext, dbspace)

        if not index_off or active is None or not active[0] or not active[1]:
            return None, None
        dependencies = self._fragment_udr_dependencies(
            connection,
            tabid,
            index_name=index_name,
        )
        predicate = self._reflected_fragment_expression(
            active[0],
            dependencies.get(None, ()),
        )
        return predicate, active[1]

    def _reflect_fragmentation(self, connection, tabid, *, index_name=None):
        rows = self._fragment_rows(connection, tabid, index_name=index_name)
        if not rows:
            return None, None
        strategy = (self._clean_str(rows[0][2]) or "").upper()
        dependencies = {}
        if strategy in {"E", "L", "N"}:
            dependencies = self._fragment_udr_dependencies(
                connection,
                tabid,
                index_name=index_name,
            )
        return self._fragmentation_from_rows(rows, dependencies)

    @reflection.cache
    def get_table_options(self, connection, table_name, schema=None, **kw):
        """Reflect native Informix storage and lock options from SYSTABLES."""
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )

        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T", "V"),
        )
        tabid = int(table_row[0])

        fragment_by = None
        dbspace = None
        tabtype = (self._clean_str(table_row[3]) or "").upper()
        if tabtype == "T":
            fragment_by, dbspace = self._reflect_fragmentation(
                connection,
                tabid,
            )

        row = connection.exec_driver_sql(
            """
            SELECT FIRST 1
                t.tabtype,
                t.locklevel,
                t.fextsize,
                t.nextsize,
                t.pagesize
            FROM systables t
            WHERE t.tabid = ?
            """,
            (tabid,),
        ).first()

        if row is None:
            raise exc.NoSuchTableError(table_name)

        options = self._table_options_from_catalog_row(row)
        if fragment_by is not None:
            options["informix_fragment_by"] = fragment_by
        if dbspace is not None:
            options["informix_dbspace"] = dbspace
        return options

    def get_temp_view_names(self, connection, schema=None, **kw):
        _ = (connection, schema, kw)
        # Informix does not support TEMP VIEW creation in the same way as
        # PostgreSQL/SQLite, so temp view enumeration is intentionally
        # unsupported for this dialect.
        return self._empty_reflection_names("temporary views")

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        viewname, schema, _synonym = self._resolve_reflection_target(
            connection,
            viewname,
            schema,
            kw,
        )
        view_row = self._get_table_row(
            connection,
            viewname,
            schema=schema,
            tabtypes=("V",),
        )
        if view_row is None:
            raise exc.NoSuchTableError(viewname)

        tabid = int(view_row[0])

        sql_text = """
            SELECT v.viewtext
            FROM sysviews v
            WHERE v.tabid = ?
            ORDER BY v.seqno
        """
        rows = connection.exec_driver_sql(sql_text, (tabid,)).fetchall()
        if not rows:
            return None

        return "".join((r[0] or "") for r in rows).rstrip()

    @reflection.cache
    def get_user_defined_types(self, connection, schema=None, **kw):
        """Reflect named ROW and DISTINCT types from Informix catalogs.

        The result is deliberately structured instead of returning catalog
        text.  Each item contains ``name``, ``schema``, ``kind`` and a fully
        reconstructed SQLAlchemy ``type`` object.
        """
        owner = self._resolved_owner(schema)
        rows = connection.exec_driver_sql(
            """
            SELECT x.extended_id, x.mode, x.owner, x.name
            FROM sysxtdtypes x
            WHERE x.mode IN ('R', 'D')
              AND LOWER(x.owner) = LOWER(?)
            ORDER BY x.name, x.extended_id
            """,
            (owner,),
        ).fetchall()
        cache = {}
        reflected = []
        for row in rows:
            extended_id = int(row[0])
            mode = self._clean_str(row[1])
            type_owner = self._clean_str(row[2])
            type_name = self._clean_str(row[3])
            reflected.append(
                {
                    "name": self.normalize_name(type_name),
                    "schema": self._normalize_schema_for_output(
                        type_owner,
                        requested_schema=schema,
                    ),
                    "kind": "row" if mode == "R" else "distinct",
                    "type": self._reflect_extended_type(
                        connection,
                        extended_id,
                        cache=cache,
                    ),
                }
            )
        return reflected

    def _identity_sequence_metadata(
        self,
        connection,
        table_name,
        column_name,
        schema=None,
    ):
        """Reflect a private Identity sequence into SQLAlchemy metadata."""
        owner = self.denormalize_name(self._resolved_owner(schema))
        sequence_name = identity_sequence_name(
            table_name,
            column_name,
            schema,
        )
        row = connection.exec_driver_sql(
            """
            SELECT FIRST 1
                s.start_val,
                s.inc_val,
                s.min_val,
                s.max_val,
                s.cycle,
                s.cache,
                s.order
            FROM syssequences s
            JOIN systables t
              ON t.tabid = s.tabid
            WHERE LOWER(t.tabname) = LOWER(?)
              AND LOWER(t.owner) = LOWER(?)
            """,
            (sequence_name, owner),
        ).first()

        if row is None:
            return None

        cycle = self._clean_str(row[4])
        order = self._clean_str(row[6]) if len(row) > 6 else None
        return {
            "always": False,
            "start": int(row[0]),
            "increment": int(row[1]),
            "minvalue": int(row[2]),
            "maxvalue": int(row[3]),
            "cycle": str(cycle).strip().upper() in {"1", "T", "Y", "TRUE"},
            "cache": int(row[5]) if row[5] is not None else None,
            "order": str(order).strip().upper() in {"1", "T", "Y", "TRUE"},
        }

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T", "V"),
        )
        tabid = int(table_row[0])
        physical_table_name = self._clean_str(table_row[1])
        physical_owner = self._clean_str(table_row[2])
        column_comments = self._column_comments_for_tabid(
            connection,
            tabid,
            physical_owner,
            physical_table_name,
        )
        odbc_column_metadata = self._odbc_column_metadata(
            connection,
            physical_table_name,
            physical_owner,
        )

        sql_text = """
            SELECT
                c.colname,
                c.colno,
                c.coltype,
                c.collength,
                c.extended_id,
                xt.name AS extended_type_name,
                xt.maxlen AS extended_maxlen,
                d.type AS default_type,
                d.default AS default_value
            FROM syscolumns c
            LEFT OUTER JOIN sysxtdtypes xt
              ON xt.extended_id = c.extended_id
            LEFT OUTER JOIN sysdefaults d
              ON d.tabid = c.tabid
             AND d.colno = c.colno
             AND d.class = 'T'
            WHERE c.tabid = ?
            ORDER BY c.colno
        """
        rows = connection.exec_driver_sql(sql_text, (tabid,)).fetchall()

        sa_columns = []
        extended_type_cache = {}
        extended_metadata_cache = {}
        for row in rows:
            colname = self._clean_str(row[0])
            colno = int(row[1])
            coltype = int(row[2])
            base_code = coltype & 0x00FF
            collength = int(row[3]) if row[3] is not None else 0
            extended_id = row[4]
            extended_type_name = row[5]
            extended_maxlen = row[6]
            default_type = row[7]
            default_value = row[8]

            metadata = None
            normalized_extended_name = self._normalize_extended_type_name(
                extended_type_name
            )
            needs_structured_lookup = bool(extended_id) and (
                base_code in {19, 20, 21, 22}
                or normalized_extended_name not in self._OPAQUE_TYPE_NAMES
            )
            if needs_structured_lookup:
                extended_id = int(extended_id)
                metadata = extended_metadata_cache.get(extended_id)
                if metadata is None:
                    metadata = self._fetch_extended_type_metadata(
                        connection,
                        extended_id,
                    )
                    extended_metadata_cache[extended_id] = metadata

            if metadata is not None and metadata["mode"] in {"C", "R", "D"}:
                satype = self._reflect_extended_type(
                    connection,
                    extended_id,
                    cache=extended_type_cache,
                )
                autoincrement = False
                nullable = not bool(coltype & 0x0100)
            else:
                satype, autoincrement, nullable = self._decode_ifx_type(
                    coltype=coltype,
                    collength=collength,
                    extended_id=extended_id,
                    extended_type_name=extended_type_name,
                    extended_maxlen=extended_maxlen,
                    interval_metadata=odbc_column_metadata.get(
                        colname.casefold(),
                    ),
                )

            identity = self._identity_sequence_metadata(
                connection,
                physical_table_name,
                colname,
                schema=physical_owner,
            )
            column_info = {
                "name": self.normalize_name(colname),
                "type": satype,
                "nullable": nullable,
                "default": self._decode_default(
                    default_type,
                    default_value,
                    base_code,
                ),
                "autoincrement": bool(identity) or autoincrement,
                "comment": (
                    column_comments.get(colno, (None, None))[1]
                    if column_comments.get(colno, (None, None))[0]
                    in (None, colname)
                    else None
                ),
            }
            if identity is not None:
                column_info["identity"] = identity

            sa_columns.append(column_info)

        return sa_columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T",),
        )
        tabid = int(table_row[0])

        sql_text = """
            SELECT
                c.constrname,
                c.owner,
                c.idxname
            FROM sysconstraints c
            WHERE c.tabid = ?
              AND c.constrtype = 'P'
            ORDER BY c.constrid
        """
        row = connection.exec_driver_sql(sql_text, (tabid,)).first()

        if row is None:
            return {"name": None, "constrained_columns": []}

        constrname = self._clean_str(row[0])
        owner = self._clean_str(row[1])
        idxname = self._clean_str(row[2])

        colnames, _column_sorting = self._get_index_columns(
            connection,
            tabid,
            idxname,
            owner=owner,
        )
        if not colnames:
            colnames = self._get_pk_columns_via_odbc(
                connection,
                table_name,
                schema=schema,
            )

        return {
            "name": (
                self._logical_reflected_name(constrname, schema=schema)
                if constrname
                else None
            ),
            "constrained_columns": colnames,
        }

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return self.get_pk_constraint(
            connection,
            table_name,
            schema=schema,
            **kw,
        ).get("constrained_columns", [])

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T",),
        )
        tabid = int(table_row[0])

        sql_text = """
            SELECT
                c.constrid,
                c.constrname,
                c.owner,
                c.idxname,
                r.primary,
                r.ptabid,
                r.delrule,
                pc.constrname AS pk_constrname,
                pc.owner AS pk_constr_owner,
                pc.idxname AS pk_idxname,
                pt.tabname AS pk_tabname,
                pt.owner AS pk_tabowner
            FROM sysconstraints c
            JOIN sysreferences r
              ON r.constrid = c.constrid
            JOIN sysconstraints pc
              ON pc.constrid = r.primary
            JOIN systables pt
              ON pt.tabid = r.ptabid
            WHERE c.tabid = ?
              AND c.constrtype = 'R'
            ORDER BY c.constrid
        """
        rows = connection.exec_driver_sql(sql_text, (tabid,)).fetchall()

        fkeys = []
        for row in rows:
            constrname = self._clean_str(row[1])
            fk_owner = self._clean_str(row[2])
            fk_idxname = self._clean_str(row[3])
            ptabid = int(row[5])
            delrule = self._clean_str(row[6])
            pk_idxname = self._clean_str(row[9])
            pk_tabname = self._clean_str(row[10])
            pk_tabowner = self._clean_str(row[11])

            constrained_columns, _fk_sorting = self._get_index_columns(
                connection,
                tabid,
                fk_idxname,
                owner=fk_owner,
            )
            referred_columns, _pk_sorting = self._get_index_columns(
                connection,
                ptabid,
                pk_idxname,
                owner=pk_tabowner,
            )
            if not constrained_columns or not referred_columns:
                (
                    odbc_constrained_columns,
                    odbc_referred_columns,
                ) = self._get_foreign_key_columns_via_odbc(
                    connection,
                    table_name,
                    schema=schema,
                    fk_name=constrname,
                )
                if not constrained_columns:
                    constrained_columns = odbc_constrained_columns
                if not referred_columns:
                    referred_columns = odbc_referred_columns

            referred_schema = self._normalize_schema_for_output(
                pk_tabowner,
                requested_schema=schema,
            )

            options = {}
            if delrule == "C":
                options["ondelete"] = "CASCADE"

            fkeys.append(
                {
                    "name": self._logical_reflected_name(
                        constrname,
                        schema=schema,
                    ),
                    "constrained_columns": constrained_columns,
                    "referred_schema": referred_schema,
                    "referred_table": self.normalize_name(pk_tabname),
                    "referred_columns": referred_columns,
                    "options": options,
                }
            )

        return sorted(
            fkeys,
            key=lambda item: (
                item.get("name") is not None,
                str(item.get("name") or "").lower(),
                tuple(item.get("constrained_columns") or ()),
            ),
        )

    @reflection.cache
    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T",),
        )
        target_tabid = int(table_row[0])
        target_owner = self._clean_str(table_row[2])
        target_schema_out = self._normalize_schema_for_output(
            target_owner,
            requested_schema=schema,
        )

        sql_text = """
            SELECT
                c.constrid,
                c.constrname,
                c.owner,
                c.tabid,
                c.idxname,
                ct.tabname AS fk_tabname,
                ct.owner AS fk_tabowner,
                pc.idxname AS pk_idxname,
                pc.owner AS pk_owner,
                pt.tabname AS pk_tabname,
                pt.owner AS pk_tabowner
            FROM sysreferences r
            JOIN sysconstraints c
              ON c.constrid = r.constrid
            JOIN systables ct
              ON ct.tabid = c.tabid
            JOIN sysconstraints pc
              ON pc.constrid = r.primary
            JOIN systables pt
              ON pt.tabid = r.ptabid
            WHERE r.ptabid = ?
              AND c.constrtype = 'R'
            ORDER BY c.constrid
        """
        rows = connection.exec_driver_sql(sql_text, (target_tabid,)).fetchall()

        incoming = []
        for row in rows:
            constrname = self._clean_str(row[1])
            fk_owner = self._clean_str(row[2])
            fk_tabid = int(row[3])
            fk_idxname = self._clean_str(row[4])
            fk_tabname = self._clean_str(row[5])
            fk_tabowner = self._clean_str(row[6])
            pk_idxname = self._clean_str(row[7])
            pk_owner = self._clean_str(row[8])
            pk_tabname = self._clean_str(row[9])

            constrained_columns, _ = self._get_index_columns(
                connection,
                fk_tabid,
                fk_idxname,
                owner=fk_owner,
            )
            referred_columns, _ = self._get_index_columns(
                connection,
                target_tabid,
                pk_idxname,
                owner=pk_owner,
            )

            incoming.append(
                {
                    "name": self._logical_reflected_name(
                        constrname,
                        schema=fk_tabowner,
                    ),
                    "constrained_schema": self._normalize_schema_for_output(
                        fk_tabowner,
                        requested_schema=schema,
                    ),
                    "constrained_table": self.normalize_name(fk_tabname),
                    "constrained_columns": constrained_columns,
                    "referred_schema": target_schema_out,
                    "referred_table": self.normalize_name(pk_tabname),
                    "referred_columns": referred_columns,
                }
            )

        return incoming

    def _constraint_duplicates_by_index(self, connection, tabid):
        constr_sql = """
            SELECT constrtype, constrname, idxname
            FROM sysconstraints
            WHERE tabid = ?
              AND constrtype IN ('P', 'U', 'R')
              AND idxname IS NOT NULL
        """
        constr_rows = connection.exec_driver_sql(constr_sql, (tabid,)).fetchall()
        constraint_by_index = {}
        for ctype, cname, idxname in constr_rows:
            clean_idxname = self._clean_str(idxname)
            if clean_idxname:
                constraint_by_index[clean_idxname.lower()] = (
                    self._clean_str(ctype),
                    self.normalize_name(self._clean_str(cname)),
                )
        return constraint_by_index

    _COLUMN_INDEX_KEY_RE = re.compile(
        r"^\s*(?P<colno>-?\d+)\s*(?:\[\s*(?P<opclassid>\d+)\s*\])?\s*$"
    )
    _FUNCTION_INDEX_KEY_RE = re.compile(
        r"^\s*<\s*(?P<procid>\d+)\s*>\s*"
        r"\((?P<columns>[^)]*)\)\s*"
        r"(?:\[\s*(?P<opclassid>\d+)\s*\])?\s*$"
    )

    def _split_index_key_specs(self, value):
        """Split INDEXKEYARRAY output without splitting function arguments."""
        text = self._clean_str(value)
        if not text:
            return []

        parts = []
        current = []
        depth = 0

        for character in text:
            if character == "(":
                depth += 1
            elif character == ")":
                depth = max(0, depth - 1)

            if character == "," and depth == 0:
                part = "".join(current).strip()
                if part:
                    parts.append(part)
                current = []
            else:
                current.append(character)

        part = "".join(current).strip()
        if part:
            parts.append(part)

        return parts

    def _parse_indexkeys(self, value):
        """Decode the textual representation of SYSINDICES.indexkeys.

        Informix renders ordinary keys as ``-3 [1]`` and functional keys as
        ``<574> (-3, 2) [1]``. The sign marks descending order. The value in
        brackets is the operator-class identifier.
        """
        components = []

        for spec in self._split_index_key_specs(value):
            function_match = self._FUNCTION_INDEX_KEY_RE.match(spec)
            if function_match is not None:
                raw_columns = [
                    item.strip()
                    for item in function_match.group("columns").split(",")
                    if item.strip()
                ]
                if not raw_columns:
                    raise ValueError(
                        f"functional index key has no columns: {spec!r}"
                    )

                signed_colnos = [int(item) for item in raw_columns]
                components.append(
                    {
                        "kind": "function",
                        "procid": int(function_match.group("procid")),
                        "colnos": [abs(value) for value in signed_colnos],
                        "descending": signed_colnos[0] < 0,
                        "opclassid": (
                            int(function_match.group("opclassid"))
                            if function_match.group("opclassid")
                            else None
                        ),
                    }
                )
                continue

            column_match = self._COLUMN_INDEX_KEY_RE.match(spec)
            if column_match is not None:
                signed_colno = int(column_match.group("colno"))
                components.append(
                    {
                        "kind": "column",
                        "colnos": [abs(signed_colno)],
                        "descending": signed_colno < 0,
                        "opclassid": (
                            int(column_match.group("opclassid"))
                            if column_match.group("opclassid")
                            else None
                        ),
                    }
                )
                continue

            raise ValueError(f"unrecognized index key specification: {spec!r}")

        return components

    def _index_rows(self, connection, tabid):
        """Read native index metadata from SYSINDICES.

        INDEXKEYARRAY is a built-in opaque type. An explicit LVARCHAR cast
        invokes its output representation while remaining usable through ODBC.
        8 KiB covers the textual form of the maximum documented key count
        without approaching Informix's 32 KiB row-size boundary.
        """
        idx_sql = """
            SELECT
                i.idxname,
                i.owner,
                i.idxtype,
                CAST(i.indexkeys AS LVARCHAR(8192)) AS indexkeys,
                i.amid,
                a.am_name,
                i.collation,
                i.tabid,
                i.amparam,
                i.nhashcols,
                i.nbuckets,
                i.indexattr,
                o.state
            FROM sysindices i
            LEFT JOIN sysams a
              ON a.am_id = i.amid
            LEFT JOIN sysobjstate o
              ON o.objtype = 'I'
             AND o.tabid = i.tabid
             AND o.name = i.idxname
             AND o.owner = i.owner
            WHERE i.tabid = ?
            ORDER BY i.idxname
        """
        return connection.exec_driver_sql(idx_sql, (tabid,)).fetchall()

    def _catalog_rows_by_ids(
        self,
        connection,
        *,
        table_name,
        id_column,
        selected_columns,
        identifiers,
    ):
        identifiers = sorted({int(value) for value in identifiers if value is not None})
        if not identifiers:
            return []

        placeholders = ", ".join("?" for _ in identifiers)
        sql_text = f"""
            SELECT {selected_columns}
            FROM {table_name}
            WHERE {id_column} IN ({placeholders})
        """
        return connection.exec_driver_sql(
            sql_text,
            tuple(identifiers),
        ).fetchall()

    def _index_procedure_map(self, connection, procids):
        rows = self._catalog_rows_by_ids(
            connection,
            table_name="sysprocedures",
            id_column="procid",
            selected_columns="procid, procname, owner",
            identifiers=procids,
        )
        return {
            int(row[0]): {
                "name": self._clean_str(row[1]),
                "owner": self._clean_str(row[2]),
            }
            for row in rows
        }

    def _index_opclass_map(self, connection, opclassids):
        rows = self._catalog_rows_by_ids(
            connection,
            table_name="sysopclasses",
            id_column="opclassid",
            selected_columns="opclassid, opclassname, owner, amid",
            identifiers=opclassids,
        )
        return {
            int(row[0]): {
                "name": self._clean_str(row[1]),
                "owner": self._clean_str(row[2]),
                "amid": int(row[3]) if row[3] is not None else None,
            }
            for row in rows
        }

    def _quote_reflected_identifier(self, value):
        value = self._clean_str(value)
        if value is None:
            return None
        return self.identifier_preparer.quote(value)

    def _qualified_catalog_name(self, name, owner, default_owner=None):
        rendered_name = self._quote_reflected_identifier(name)
        if rendered_name is None:
            return None

        cleaned_owner = self._clean_str(owner)
        cleaned_default = self._clean_str(default_owner)
        if cleaned_owner and (
            cleaned_default is None
            or cleaned_owner.casefold() != cleaned_default.casefold()
        ):
            return (
                f"{self._quote_reflected_identifier(cleaned_owner)}."
                f"{rendered_name}"
            )

        return rendered_name

    @staticmethod
    def _single_or_tuple(values):
        return _helper_single_or_tuple(values)

    def _index_info_from_row(
        self,
        tabid,
        row,
        components,
        constraint_by_index,
        procedure_map,
        opclass_map,
        colmap,
        schema=None,
    ):
        _ = tabid
        idxname = self._clean_str(row[0])
        owner = self._clean_str(row[1])
        idxtype = self._clean_str(row[2])
        amid = int(row[4]) if row[4] is not None else None
        access_method = self._clean_str(row[5])
        _collation = self._clean_str(row[6])
        catalog_tabid = int(row[7]) if len(row) > 7 and row[7] is not None else tabid
        amparam = self._clean_str(row[8]) if len(row) > 8 else None
        nhashcols = int(row[9]) if len(row) > 9 and row[9] is not None else 0
        nbuckets = int(row[10]) if len(row) > 10 and row[10] is not None else 0
        indexattr = int(row[11]) if len(row) > 11 and row[11] is not None else 0
        state_code = self._clean_str(row[12]) if len(row) > 12 else None
        if catalog_tabid != tabid:
            util.warn(
                "SYSINDICES returned an unexpected table identifier for "
                f"idxname={idxname!r}: expected tabid={tabid}, "
                f"received tabid={catalog_tabid}"
            )
            return None

        key = idxname.lower() if idxname else None
        duplicated = constraint_by_index.get(key) if key else None

        if duplicated and duplicated[0] in ("P", "U", "R"):
            return None

        column_names = []
        expressions = []
        column_sorting = {}
        procedures = []
        opclasses = []
        has_function = False

        for component in components:
            colnos = component["colnos"]
            colnames = [colmap.get(colno) for colno in colnos]
            if any(colname is None for colname in colnames):
                util.warn(
                    "Could not resolve SYSINDICES index key columns for "
                    f"tabid={tabid}, idxname={idxname!r}, colnos={colnos!r}"
                )
                return None

            opclass = opclass_map.get(component.get("opclassid"))
            if opclass is not None:
                opclass_amid = opclass.get("amid")
                if (
                    amid is not None
                    and opclass_amid is not None
                    and amid != opclass_amid
                ):
                    util.warn(
                        "SYSOPCLASSES access method does not match "
                        f"SYSINDICES for idxname={idxname!r}, "
                        f"opclassid={component.get('opclassid')!r}"
                    )
                opclasses.append(
                    self._qualified_catalog_name(
                        opclass["name"],
                        opclass["owner"],
                        default_owner=owner,
                    )
                )

            if component["kind"] == "column":
                colname = colnames[0]
                column_names.append(colname)
                expression = self._quote_reflected_identifier(colname)
                if component["descending"]:
                    column_sorting[colname] = ("desc",)
                    # For a mixed functional/ordinary index SQLAlchemy must
                    # consume ``expressions`` rather than only
                    # ``column_names``.  Preserve the native descending key
                    # directly in that expression as well as in the standard
                    # ``column_sorting`` mapping, otherwise metadata
                    # round-trips silently rebuild the key in ascending order.
                    expression += " DESC"
                expressions.append(expression)
                continue

            has_function = True
            procedure = procedure_map.get(component.get("procid"))
            if procedure is None:
                util.warn(
                    "Could not resolve functional-index procedure "
                    f"procid={component.get('procid')!r} for index {idxname!r}"
                )
                return None

            procedure_name = self._qualified_catalog_name(
                procedure["name"],
                procedure["owner"],
                default_owner=owner,
            )
            procedures.append(procedure_name)

            rendered_columns = ", ".join(
                self._quote_reflected_identifier(colname)
                for colname in colnames
            )
            expression = f"{procedure_name}({rendered_columns})"
            if component["descending"]:
                expression += " DESC"

            column_names.append(None)
            expressions.append(expression)

        if not column_names:
            return None

        idx_info = {
            "name": self._logical_reflected_name(
                idxname,
                schema=schema,
            ),
            "column_names": column_names,
            "unique": idxtype in ("U", "u"),
        }

        if column_sorting:
            idx_info["column_sorting"] = column_sorting

        dialect_options = {}
        if has_function:
            idx_info["expressions"] = expressions
            dialect_options.update(
                {
                    "informix_procedure": self._single_or_tuple(procedures),
                    "informix_access_method": access_method,
                    "informix_opclass": self._single_or_tuple(opclasses),
                }
            )
        elif access_method and access_method.casefold() not in {"btree", "b-tree"}:
            dialect_options["informix_using"] = access_method
            reflected_opclasses = self._single_or_tuple(opclasses)
            if reflected_opclasses is not None:
                dialect_options["informix_opclass"] = reflected_opclasses

        if nhashcols > 0:
            hash_columns = [
                name for name in column_names[:nhashcols] if name is not None
            ]
            if len(hash_columns) == nhashcols:
                dialect_options["informix_hash_on"] = tuple(hash_columns)
                if nbuckets > 0:
                    dialect_options["informix_buckets"] = nbuckets

        if indexattr & 0x00000002:
            dialect_options["informix_compressed"] = True
        if indexattr & 0x00000010:
            dialect_options["informix_visible"] = False
        if amparam and access_method and access_method.casefold() not in {"btree", "b-tree"}:
            dialect_options["informix_amparam"] = _ReflectedAccessMethodParameters(amparam)

        state_modes = {
            "E": "ENABLED",
            "D": "DISABLED",
            "F": "FILTERING WITHOUT ERROR",
            "G": "FILTERING WITH ERROR",
        }
        if state_code in state_modes:
            dialect_options["informix_mode"] = state_modes[state_code]

        if dialect_options:
            idx_info["dialect_options"] = {
                key: value
                for key, value in dialect_options.items()
                if value is not None
            }

        return idx_info

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T",),
        )
        tabid = int(table_row[0])
        constraint_by_index = self._constraint_duplicates_by_index(connection, tabid)
        colmap = self._get_column_name_map(connection, tabid)

        parsed_rows = []
        procids = set()
        opclassids = set()

        for row in self._index_rows(connection, tabid):
            idxname = self._clean_str(row[0])
            try:
                components = self._parse_indexkeys(row[3])
            except (TypeError, ValueError) as err:
                util.warn(
                    "Could not decode SYSINDICES.indexkeys for "
                    f"tabid={tabid}, idxname={idxname!r}: {err}"
                )
                continue

            if not components:
                util.warn(
                    "SYSINDICES returned no key components for "
                    f"tabid={tabid}, idxname={idxname!r}"
                )
                continue

            parsed_rows.append((row, components))
            procids.update(
                component.get("procid")
                for component in components
                if component.get("procid") is not None
            )
            opclassids.update(
                component.get("opclassid")
                for component in components
                if component.get("opclassid") is not None
            )

        procedure_map = self._index_procedure_map(connection, procids)
        opclass_map = self._index_opclass_map(connection, opclassids)

        indexes = []
        for row, components in parsed_rows:
            idx_info = self._index_info_from_row(
                tabid,
                row,
                components,
                constraint_by_index,
                procedure_map,
                opclass_map,
                colmap,
                schema=schema,
            )
            if idx_info is not None:
                index_name = self._clean_str(row[0])
                predicate, partial_dbspace = self._reflect_partial_index(
                    connection,
                    tabid,
                    index_name=index_name,
                )
                if predicate is not None:
                    dialect_options = idx_info.setdefault("dialect_options", {})
                    dialect_options["informix_where"] = predicate
                    dialect_options["informix_dbspace"] = partial_dbspace
                else:
                    fragment_by, dbspace = self._reflect_fragmentation(
                        connection,
                        tabid,
                        index_name=index_name,
                    )
                    if fragment_by is not None or dbspace is not None:
                        dialect_options = idx_info.setdefault(
                            "dialect_options",
                            {},
                        )
                        if fragment_by is not None:
                            dialect_options["informix_fragment_by"] = fragment_by
                        if dbspace is not None:
                            dialect_options["informix_dbspace"] = dbspace
                indexes.append(idx_info)

        return indexes

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        table_name, schema, _synonym = self._resolve_reflection_target(
            connection,
            table_name,
            schema,
            kw,
        )
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T",),
        )
        tabid = int(table_row[0])

        sql_text = """
            SELECT
                c.constrname,
                c.owner,
                c.idxname
            FROM sysconstraints c
            WHERE c.tabid = ?
              AND c.constrtype = 'U'
            ORDER BY c.constrid
        """
        rows = connection.exec_driver_sql(sql_text, (tabid,)).fetchall()

        unique_constraints = []
        for row in rows:
            constrname = self._clean_str(row[0])
            owner = self._clean_str(row[1])
            idxname = self._clean_str(row[2])

            colnames, _ = self._get_index_columns(
                connection,
                tabid,
                idxname,
                owner=owner,
            )
            if not colnames:
                colnames = self._get_index_columns_via_odbc(
                    connection,
                    table_name,
                    schema=schema,
                    index_name=idxname or constrname,
                    unique_only=True,
                )

            unique_constraints.append(
                {
                    "name": (
                        self._logical_reflected_name(constrname, schema=schema)
                        if constrname
                        else None
                    ),
                    "column_names": colnames,
                }
            )

        return sorted(
            unique_constraints,
            key=lambda item: (
                item.get("name") is not None,
                str(item.get("name") or "").lower(),
                tuple(item.get("column_names") or ()),
            ),
        )

    def get_multi_columns(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        yield from self._multi_reflect(
            connection,
            self.get_columns,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

    def get_multi_pk_constraint(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        yield from self._multi_reflect(
            connection,
            self.get_pk_constraint,
            view_default_factory=lambda: {
                "name": None,
                "constrained_columns": [],
            },
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

    def get_multi_foreign_keys(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        yield from self._multi_reflect(
            connection,
            self.get_foreign_keys,
            view_default_factory=list,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

    def get_multi_indexes(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        yield from self._multi_reflect(
            connection,
            self.get_indexes,
            view_default_factory=list,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

    def get_multi_unique_constraints(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        yield from self._multi_reflect(
            connection,
            self.get_unique_constraints,
            view_default_factory=list,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

    def get_multi_check_constraints(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        yield from self._multi_reflect(
            connection,
            self.get_check_constraints,
            view_default_factory=list,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

    def get_multi_table_comment(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        names = self._table_names_for_multi(
            connection,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )
        if not names:
            return

        owner = self._resolved_owner(schema)
        catalog_exists = self._comment_catalog_exists(
            connection,
            TABLE_COMMENT_CATALOG,
        )
        if catalog_exists:
            rows = connection.exec_driver_sql(
                f"""
                SELECT t.tabname, c.comment_value
                FROM systables t
                LEFT OUTER JOIN {TABLE_COMMENT_CATALOG} c
                  ON c.tabid = t.tabid
                 AND c.object_owner = t.owner
                 AND c.object_name = t.tabname
                WHERE LOWER(t.owner) = LOWER(?)
                  AND t.tabid >= 100
                  AND t.tabtype IN ('T', 'V')
                ORDER BY t.tabname
                """,
                (owner,),
            ).fetchall()
        else:
            rows = connection.exec_driver_sql(
                """
                SELECT t.tabname
                FROM systables t
                WHERE LOWER(t.owner) = LOWER(?)
                  AND t.tabid >= 100
                  AND t.tabtype IN ('T', 'V')
                ORDER BY t.tabname
                """,
                (owner,),
            ).fetchall()

        existing_comments = {}
        for row in rows:
            cleaned_name = self._clean_str(row[0])
            reflected_name = self.normalize_name(cleaned_name)
            stored_value = row[1] if catalog_exists else None
            comment = self._decode_comment_value(stored_value)
            existing_comments[str(cleaned_name)] = comment
            existing_comments[str(reflected_name)] = comment

        # SQLAlchemy requires non-existent names in filter_names to be omitted.
        # _table_names_for_multi() deliberately avoids an extra catalog scan
        # for the broad ANY/ANY filtered case, so this batch query is also the
        # authoritative existence check for that combination.
        for name in names:
            key = str(name)
            if key not in existing_comments:
                continue
            yield (schema, name), {"text": existing_comments[key]}

    def get_multi_table_options(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        **kw,
    ):
        yield from self._multi_reflect(
            connection,
            self.get_table_options,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

    def _remember_unreflectable(self, unreflectable, key, error):
        if key not in unreflectable:
            unreflectable[key] = error

    def _multi_reflect_one(
        self,
        connection,
        single_table_method,
        name,
        schema,
        unreflectable,
        kw,
    ):
        key = (schema, name)
        for candidate_name in (name, quoted_name(name, True)):
            try:
                return single_table_method(
                    connection,
                    candidate_name,
                    schema=schema,
                    **kw,
                )
            except exc.UnreflectableTableError as err:
                self._remember_unreflectable(unreflectable, key, err)
                return self._MISSING
            except exc.NoSuchTableError:
                continue

        return self._MISSING

    def _multi_reflect(
        self,
        connection,
        single_table_method,
        *,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=ObjectScope.DEFAULT,
        view_default_factory=None,
        **kw,
    ):
        unreflectable = kw.pop("unreflectable", {})

        names = self._table_names_for_multi(
            connection,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

        view_names = set()

        if (
            view_default_factory is not None
            and ObjectScope.DEFAULT in scope
            and ObjectKind.VIEW in kind
        ):
            view_names = {
                str(name)
                for name in self.get_view_names(
                    connection,
                    schema=schema,
                    **kw,
                )
            }

        for name in names:
            key = (schema, name)

            if (
                view_default_factory is not None
                and str(name) in view_names
            ):
                yield (
                    key,
                    view_default_factory(),
                )
                continue

            reflected = self._multi_reflect_one(
                connection,
                single_table_method,
                name,
                schema,
                unreflectable,
                kw,
            )

            if reflected is self._MISSING:
                continue

            yield (
                key,
                reflected,
            )

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
        _ = kw
        owner = self._resolved_owner(schema)
        sql_text = """
            SELECT t.tabname
            FROM syssequences s
            JOIN systables t
              ON t.tabid = s.tabid
            WHERE LOWER(t.owner) = LOWER(?)
            ORDER BY t.tabname
        """
        rows = connection.exec_driver_sql(sql_text, (owner,)).fetchall()
        return [self.normalize_name(self._clean_str(r[0])) for r in rows]
