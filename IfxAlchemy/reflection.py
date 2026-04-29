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
from sqlalchemy import exc
from sqlalchemy import types as sa_types
from sqlalchemy import sql, util
from sqlalchemy.sql import quoted_name
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope
import re

from . import sqla_compat

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
        if (
            name.upper() == name
            and not sqla_compat.identifier_requires_quotes(
                self.identifier_preparer, lowered
            )
        ):
            return lowered

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
    }

    _CHAR_FALLBACK_TYPES = {"CHAR", "NCHAR"}
    _VARCHAR_FALLBACK_TYPES = {"VARCHAR", "NVARCHAR", "LVARCHAR"}
    _INTEGER_FALLBACK_TYPES = {"INTEGER", "SERIAL"}
    _BIG_INTEGER_FALLBACK_TYPES = {"INT8", "SERIAL8", "BIGINT", "BIGSERIAL"}
    _NUMERIC_FALLBACK_TYPES = {"DECIMAL", "NUMERIC", "MONEY"}
    _SIMPLE_FALLBACK_FACTORIES = {
        "SMALLINT": sa_types.SmallInteger,
        "FLOAT": sa_types.Float,
        "SMALLFLOAT": sa_types.Float,
        "DATE": sa_types.Date,
        "DATETIME": sa_types.DateTime,
        "INTERVAL": sa_types.Interval,
        "TEXT": sa_types.Text,
        "BYTE": sa_types.LargeBinary,
        "BOOLEAN": sa_types.Boolean,
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

    def _resolved_owner(self, schema=None):
        owner = schema if schema is not None else self.default_schema_name
        return self._clean_str(owner)

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
        # Informix persiste los identificadores no quoted en minúsculas
        # dentro del catálogo con DELIMIDENT=Y.
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
            # Un nombre no quoted en Informix debe plegarse a la forma
            # case-insensitive del catálogo, no buscarse con el mixed-case
            # recibido desde Python.
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
                  AND LOWER(i.idxname) = LOWER(?)
                  AND LOWER(i.owner) = LOWER(?)
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
                  AND LOWER(i.idxname) = LOWER(?)
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
        except Exception:
            pass

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
        except Exception:
            return []
        finally:
            self._close_cursor(cursor)

    def _row_value(self, row, attr_names, index, default=_MISSING):
        for attr_name in attr_names:
            value = getattr(row, attr_name, None)
            if value is not None:
                return value

        try:
            return row[index]
        except Exception:
            if default is self._MISSING:
                raise
            return default

    def _normalized_clean_name(self, value):
        cleaned = self._clean_str(value)
        if cleaned is None:
            return None
        return self.normalize_name(cleaned)

    def _int_or_default(self, value, default):
        try:
            return int(value)
        except Exception:
            return default

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
            except Exception:
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
            except Exception:
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
            except Exception:
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

    def _is_dbapi_probe_error(self, error):
        if isinstance(error, exc.DBAPIError):
            return True

        module_name = type(error).__module__.split(".", 1)[0].lower()
        return module_name in {"pyodbc", "ifxpy", "ifxpydbi"}

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
        try:
            cursor.execute(sql_text)
            cursor.fetchone()
            return True
        except Exception as err:
            if not self._is_dbapi_probe_error(err):
                raise
            return False

    def _has_table_via_dbapi_probe(self, connection, table_name, schema=None):
        probe_identifiers = tuple(
            self._iter_probe_identifiers(table_name, schema=schema)
        )
        if not probe_identifiers:
            return False

        try:
            cursor = self._open_dbapi_cursor(connection)
        except Exception as err:
            if not self._is_dbapi_probe_error(err):
                raise
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

    def _decode_default(self, default_type, default_value):
        default_type = self._clean_str(default_type)
        default_value = self._clean_str(default_value)

        if not default_type:
            return None

        if default_type == "L":
            return default_value
        if default_type == "T":
            return "TODAY"
        if default_type == "U":
            return "USER"
        if default_type == "C":
            return default_value or "CURRENT"
        if default_type == "S":
            return default_value or "DBSERVERNAME"
        if default_type == "N":
            return None

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
        except Exception:
            return self._MISSING

    def _instantiate_fallback_type(self, type_name, args):
        if type_name in self._CHAR_FALLBACK_TYPES:
            return sa_types.CHAR(args[0] if args else None)
        if type_name in self._VARCHAR_FALLBACK_TYPES:
            return sa_types.VARCHAR(args[0] if args else None)
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
            length = extended_maxlen or encoded_len or None
            if length is not None:
                length = self._int_or_default(length, None)
            return self._ifx_type_result(
                "VARCHAR",
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
    ):
        qualifiers = self._decode_datetime_qualifiers(encoded_len)
        satype = self._instantiate_ischema_type(type_name)
        # Preserve Informix metadata without pretending SQLAlchemy has a
        # portable generic representation for every Informix qualifier.
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
            )

        return self._ifx_type_result(
            type_name,
            autoincrement,
            nullable,
            *self._ifx_type_args(base_code, encoded_len),
        )

    def has_table(self, connection, table_name, schema=None, **kw):
        _ = kw
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
        if self._has_table_via_odbc(connection, table_name, schema=schema):
            return True

        if self._has_table_via_sql_probe(connection, table_name, schema=schema):
            return True

        return self._has_table_via_dbapi_probe(connection, table_name, schema=schema)

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
        _ = kw
        sql_text = """
            SELECT DISTINCT t.owner
            FROM systables t
            WHERE t.owner IS NOT NULL
            ORDER BY t.owner
        """
        rows = connection.exec_driver_sql(sql_text).fetchall()
        return [self.normalize_name(self._clean_str(r[0])) for r in rows if r[0]]

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

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        _ = (connection, table_name, schema, kw)
        # Explicit contract for a reflection surface we don't currently
        # implement for Informix in this dialect.
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        _ = (connection, table_name, schema, kw)
        # Informix table comments are not currently reflected by this
        # dialect; return the stable SQLAlchemy structure explicitly.
        return {"text": None}

    def get_table_options(self, connection, table_name, schema=None, **kw):
        _ = (connection, table_name, schema, kw)
        # Informix-specific table options are not currently reflected by
        # this dialect; return the stable SQLAlchemy structure explicitly.
        return {}

    def get_temp_view_names(self, connection, schema=None, **kw):
        _ = (connection, schema, kw)
        # Informix does not support TEMP VIEW creation in the same way as
        # PostgreSQL/SQLite, so temp view enumeration is intentionally
        # unsupported for this dialect.
        return self._empty_reflection_names("temporary views")

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        _ = kw
        view_row = self._get_table_row(
            connection,
            viewname,
            schema=schema,
            tabtypes=("V",),
        )
        if view_row is None:
            return None

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
    def get_columns(self, connection, table_name, schema=None, **kw):
        _ = kw
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T", "V"),
        )
        tabid = int(table_row[0])

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
        for row in rows:
            colname = self._clean_str(row[0])
            coltype = int(row[2])
            collength = int(row[3]) if row[3] is not None else 0
            extended_id = row[4]
            extended_type_name = row[5]
            extended_maxlen = row[6]
            default_type = row[7]
            default_value = row[8]

            satype, autoincrement, nullable = self._decode_ifx_type(
                coltype=coltype,
                collength=collength,
                extended_id=extended_id,
                extended_type_name=extended_type_name,
                extended_maxlen=extended_maxlen,
            )

            sa_columns.append(
                {
                    "name": self.normalize_name(colname),
                    "type": satype,
                    "nullable": nullable,
                    "default": self._decode_default(default_type, default_value),
                    "autoincrement": autoincrement,
                }
            )

        return sa_columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        _ = kw
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
            "name": self.normalize_name(constrname) if constrname else None,
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
        _ = kw
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
            elif delrule == "R":
                options["ondelete"] = "RESTRICT"

            fkeys.append(
                {
                    "name": self.normalize_name(constrname) if constrname else None,
                    "constrained_columns": constrained_columns,
                    "referred_schema": referred_schema,
                    "referred_table": self.normalize_name(pk_tabname),
                    "referred_columns": referred_columns,
                    "options": options,
                }
            )

        return fkeys

    @reflection.cache
    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        _ = kw
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
                    "name": self.normalize_name(constrname) if constrname else None,
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
              AND constrtype IN ('P', 'U')
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

    def _index_rows(self, connection, tabid):
        idx_sql = f"""
            SELECT
                i.idxname,
                i.owner,
                i.idxtype,
                {", ".join(f"i.part{n}" for n in range(1, self._INDEX_PART_COUNT + 1))}
            FROM sysindexes i
            WHERE i.tabid = ?
            ORDER BY i.idxname
        """
        return connection.exec_driver_sql(idx_sql, (tabid,)).fetchall()

    def _index_info_from_row(self, connection, tabid, row, constraint_by_index):
        idxname = self._clean_str(row[0])
        owner = self._clean_str(row[1])
        idxtype = self._clean_str(row[2])

        key = idxname.lower() if idxname else None
        duplicated = constraint_by_index.get(key) if key else None

        if duplicated and duplicated[0] == "P":
            return None

        colnames, column_sorting = self._get_index_columns(
            connection,
            tabid,
            idxname,
            owner=owner,
        )
        if not colnames:
            return None

        idx_info = {
            "name": self.normalize_name(idxname),
            "column_names": colnames,
            "unique": idxtype in ("U", "u"),
        }

        if column_sorting:
            idx_info["column_sorting"] = column_sorting

        if duplicated and duplicated[0] == "U":
            idx_info["duplicates_constraint"] = duplicated[1]

        return idx_info

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        _ = kw
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T",),
        )
        tabid = int(table_row[0])
        constraint_by_index = self._constraint_duplicates_by_index(connection, tabid)

        indexes = []
        for row in self._index_rows(connection, tabid):
            idx_info = self._index_info_from_row(
                connection,
                tabid,
                row,
                constraint_by_index,
            )
            if idx_info is not None:
                indexes.append(idx_info)

        return indexes

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        _ = kw
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
                    "name": self.normalize_name(constrname) if constrname else None,
                    "column_names": colnames,
                }
            )

        return unique_constraints

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
        yield from self._multi_reflect(
            connection,
            self.get_table_comment,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

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

        for name in names:
            key = (schema, name)
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
