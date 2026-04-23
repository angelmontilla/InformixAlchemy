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
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
import re

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
            and not self.identifier_preparer._requires_quotes(lowered)
        ):
            return lowered

        return name

    def denormalize_name(self, name):
        name = self._coerce_name(name)
        if name is None:
            return None

        lowered = name.lower()
        if lowered == name and not self.identifier_preparer._requires_quotes(
            lowered
        ):
            return name.upper()

        return name

    def _get_default_schema_name(self, connection):
        """Return: current setting of the schema attribute"""
        default_schema_name = connection.exec_driver_sql(
                    'SELECT USER FROM systables WHERE tabid = 1').scalar()
        if default_schema_name is not None:
            default_schema_name = self._coerce_name(default_schema_name).strip()
        return self.normalize_name(default_schema_name)

    @property
    def default_schema_name(self):
        return self.dialect.default_schema_name

class IfxReflector(BaseReflector):
    ischema = MetaData()

    _INDEX_PART_COUNT = 16

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
        45: "BOOLEAN",
        52: "BIGINT",
        53: "BIGSERIAL",
    }

    def _clean_str(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()

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

    def _get_pk_columns_via_odbc(self, connection, table_name, schema=None):
        dbapi_connection = getattr(connection.connection, "dbapi_connection", None)
        if dbapi_connection is None:
            return []

        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            if not hasattr(cursor, "primaryKeys"):
                return []

            kwargs = {"table": table_name}
            if schema is not None:
                kwargs["schema"] = schema

            rows = cursor.primaryKeys(**kwargs).fetchall()
        except Exception:
            return []
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

        if not rows:
            return []

        by_seq = []
        for row in rows:
            try:
                key_seq = getattr(row, "key_seq", None)
                if key_seq is None:
                    key_seq = getattr(row, "KEY_SEQ", None)

                column_name = getattr(row, "column_name", None)
                if column_name is None:
                    column_name = getattr(row, "COLUMN_NAME", None)

                if column_name is None:
                    column_name = row[3]
                if key_seq is None:
                    key_seq = row[4]

                by_seq.append((int(key_seq), self.normalize_name(self._clean_str(column_name))))
            except Exception:
                continue

        by_seq.sort(key=lambda item: item[0])
        return [name for _, name in by_seq if name]

    def _get_index_columns_via_odbc(
        self,
        connection,
        table_name,
        schema=None,
        index_name=None,
        unique_only=None,
    ):
        dbapi_connection = getattr(connection.connection, "dbapi_connection", None)
        if dbapi_connection is None:
            return []

        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            if not hasattr(cursor, "statistics"):
                return []

            kwargs = {"table": table_name, "unique": bool(unique_only), "quick": True}
            if schema is not None:
                kwargs["schema"] = schema

            rows = cursor.statistics(**kwargs).fetchall()
        except Exception:
            return []
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

        grouped = {}
        wanted_name = self.normalize_name(self._clean_str(index_name)) if index_name else None

        for row in rows:
            try:
                raw_index_name = getattr(row, "index_name", None)
                if raw_index_name is None:
                    raw_index_name = getattr(row, "INDEX_NAME", None)
                if raw_index_name is None:
                    raw_index_name = row[5]

                raw_column_name = getattr(row, "column_name", None)
                if raw_column_name is None:
                    raw_column_name = getattr(row, "COLUMN_NAME", None)
                if raw_column_name is None:
                    raw_column_name = row[8]

                raw_ordinal = getattr(row, "ordinal_position", None)
                if raw_ordinal is None:
                    raw_ordinal = getattr(row, "ORDINAL_POSITION", None)
                if raw_ordinal is None:
                    raw_ordinal = row[7]

                raw_non_unique = getattr(row, "non_unique", None)
                if raw_non_unique is None:
                    raw_non_unique = getattr(row, "NON_UNIQUE", None)
                if raw_non_unique is None:
                    raw_non_unique = row[3]
            except Exception:
                continue

            normalized_index_name = self.normalize_name(self._clean_str(raw_index_name))
            column_name = self.normalize_name(self._clean_str(raw_column_name))
            if not normalized_index_name or not column_name:
                continue

            if unique_only is True and bool(raw_non_unique):
                continue
            if unique_only is False and not bool(raw_non_unique):
                continue

            try:
                ordinal = int(raw_ordinal)
            except Exception:
                ordinal = len(grouped.get(normalized_index_name, [])) + 1

            grouped.setdefault(normalized_index_name, []).append((ordinal, column_name))

        if not grouped:
            return []

        if wanted_name and wanted_name in grouped:
            selected = grouped[wanted_name]
        elif wanted_name:
            return []
        elif len(grouped) == 1:
            selected = next(iter(grouped.values()))
        else:
            return []

        selected.sort(key=lambda item: item[0])
        return [column_name for _, column_name in selected]

    def _get_foreign_key_columns_via_odbc(
        self,
        connection,
        table_name,
        schema=None,
        fk_name=None,
    ):
        dbapi_connection = getattr(connection.connection, "dbapi_connection", None)
        if dbapi_connection is None:
            return [], []

        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            if not hasattr(cursor, "foreignKeys"):
                return [], []

            kwargs = {"foreignTable": table_name}
            if schema is not None:
                kwargs["foreignSchema"] = schema

            rows = cursor.foreignKeys(**kwargs).fetchall()
        except Exception:
            return [], []
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

        wanted_name = self.normalize_name(self._clean_str(fk_name)) if fk_name else None
        grouped = {}

        for row in rows:
            try:
                raw_fk_name = getattr(row, "fk_name", None)
                if raw_fk_name is None:
                    raw_fk_name = getattr(row, "FK_NAME", None)
                if raw_fk_name is None:
                    raw_fk_name = row[11]

                raw_key_seq = getattr(row, "key_seq", None)
                if raw_key_seq is None:
                    raw_key_seq = getattr(row, "KEY_SEQ", None)
                if raw_key_seq is None:
                    raw_key_seq = row[13]

                raw_fk_column = getattr(row, "fkcolumn_name", None)
                if raw_fk_column is None:
                    raw_fk_column = getattr(row, "FKCOLUMN_NAME", None)
                if raw_fk_column is None:
                    raw_fk_column = row[7]

                raw_pk_column = getattr(row, "pkcolumn_name", None)
                if raw_pk_column is None:
                    raw_pk_column = getattr(row, "PKCOLUMN_NAME", None)
                if raw_pk_column is None:
                    raw_pk_column = row[3]
            except Exception:
                continue

            normalized_fk_name = self.normalize_name(self._clean_str(raw_fk_name))
            if wanted_name and normalized_fk_name != wanted_name:
                continue

            if not normalized_fk_name:
                normalized_fk_name = "__unnamed_fk__"

            try:
                key_seq = int(raw_key_seq)
            except Exception:
                key_seq = len(grouped.get(normalized_fk_name, [])) + 1

            grouped.setdefault(normalized_fk_name, []).append(
                (
                    key_seq,
                    self.normalize_name(self._clean_str(raw_fk_column)),
                    self.normalize_name(self._clean_str(raw_pk_column)),
                )
            )

        if not grouped:
            return [], []

        if wanted_name and wanted_name in grouped:
            selected = grouped[wanted_name]
        elif wanted_name:
            return [], []
        elif len(grouped) == 1:
            selected = next(iter(grouped.values()))
        else:
            return [], []

        selected.sort(key=lambda item: item[0])
        constrained_columns = [fk_col for _, fk_col, _ in selected if fk_col]
        referred_columns = [pk_col for _, _, pk_col in selected if pk_col]
        return constrained_columns, referred_columns

    def _has_table_via_odbc(self, connection, table_name, schema=None):
        dbapi_connection = getattr(connection.connection, "dbapi_connection", None)
        if dbapi_connection is None:
            return False

        cleaned_name = self._clean_str(table_name)
        if not cleaned_name:
            return False

        is_explicitly_quoted = getattr(table_name, "quote", None) is True
        if is_explicitly_quoted:
            metadata_lookup_name = cleaned_name
            wanted_name = cleaned_name
        else:
            metadata_lookup_name = self._fold_unquoted_lookup_name(cleaned_name)
            wanted_name = self.normalize_name(self._clean_str(metadata_lookup_name))

        wanted_schema = None
        metadata_lookup_schema = None
        if schema is not None:
            cleaned_schema = self._clean_str(schema)
            if getattr(schema, "quote", None) is True:
                metadata_lookup_schema = cleaned_schema
                wanted_schema = cleaned_schema
            else:
                metadata_lookup_schema = self._fold_unquoted_lookup_name(cleaned_schema)
                wanted_schema = self.normalize_name(self._clean_str(metadata_lookup_schema))

        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            if not hasattr(cursor, "tables"):
                return False

            kwargs = {"table": metadata_lookup_name}
            if metadata_lookup_schema is not None:
                kwargs["schema"] = metadata_lookup_schema

            rows = cursor.tables(**kwargs).fetchall()
        except Exception:
            return False
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

        for row in rows:
            try:
                raw_name = getattr(row, "table_name", None)
                if raw_name is None:
                    raw_name = getattr(row, "TABLE_NAME", None)
                if raw_name is None:
                    raw_name = row[2]
            except Exception:
                continue

            raw_name = self._clean_str(raw_name)
            if raw_name is None:
                continue

            if is_explicitly_quoted:
                name_matches = raw_name == wanted_name
            else:
                name_matches = self.normalize_name(raw_name) == wanted_name

            if not name_matches:
                continue

            if wanted_schema is not None:
                try:
                    raw_schema = getattr(row, "table_schem", None)
                    if raw_schema is None:
                        raw_schema = getattr(row, "TABLE_SCHEM", None)
                    if raw_schema is None:
                        raw_schema = row[1]
                except Exception:
                    raw_schema = None

                raw_schema = self._clean_str(raw_schema)
                if raw_schema is None:
                    continue

                if getattr(schema, "quote", None) is True:
                    schema_matches = raw_schema == wanted_schema
                else:
                    schema_matches = self.normalize_name(raw_schema) == wanted_schema

                if not schema_matches:
                    continue

            return True

        return False

    def _has_table_via_sql_probe(self, connection, table_name, schema=None):
        cleaned_name = self._clean_str(table_name)
        if not cleaned_name:
            return False

        is_explicitly_quoted = getattr(table_name, "quote", None) is True
        if is_explicitly_quoted:
            # Solo probar la forma quoted exacta.
            name_candidates = [self.identifier_preparer.quote(cleaned_name)]
        else:
            # Solo probar la forma unquoted/case-insensitive del backend.
            name_candidates = [self._fold_unquoted_lookup_name(cleaned_name)]

        if schema is not None:
            cleaned_schema = self._clean_str(schema)
            if not cleaned_schema:
                schema_candidates = [None]
            elif getattr(schema, "quote", None) is True:
                schema_candidates = [self.identifier_preparer.quote_schema(cleaned_schema)]
            else:
                schema_candidates = []
                folded_schema = self._fold_unquoted_lookup_name(cleaned_schema)
                for token in (folded_schema, cleaned_schema):
                    if token and token not in schema_candidates:
                        schema_candidates.append(token)
        else:
            schema_candidates = [None]

        for schema_token in schema_candidates:
            for table_token in name_candidates:
                if schema_token:
                    from_token = "%s.%s" % (schema_token, table_token)
                else:
                    from_token = table_token

                sql_text = "SELECT COUNT(*) FROM %s" % from_token

                try:
                    connection.exec_driver_sql(sql_text).scalar()
                    return True
                except Exception:
                    continue

        return False

    def _has_table_via_dbapi_probe(self, connection, table_name, schema=None):
        dbapi_connection = getattr(connection.connection, "dbapi_connection", None)
        if dbapi_connection is None:
            return False

        cleaned_name = self._clean_str(table_name)
        if not cleaned_name:
            return False

        is_explicitly_quoted = getattr(table_name, "quote", None) is True
        if is_explicitly_quoted:
            # Solo probar la forma quoted exacta.
            name_candidates = [self.identifier_preparer.quote(cleaned_name)]
        else:
            # Solo probar la forma unquoted/case-insensitive del backend.
            name_candidates = [self._fold_unquoted_lookup_name(cleaned_name)]

        if schema is not None:
            cleaned_schema = self._clean_str(schema)
            if not cleaned_schema:
                schema_candidates = [None]
            elif getattr(schema, "quote", None) is True:
                schema_candidates = [self.identifier_preparer.quote_schema(cleaned_schema)]
            else:
                schema_candidates = []
                folded_schema = self._fold_unquoted_lookup_name(cleaned_schema)
                for token in (folded_schema, cleaned_schema):
                    if token and token not in schema_candidates:
                        schema_candidates.append(token)
        else:
            schema_candidates = [None]

        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            for schema_token in schema_candidates:
                for table_token in name_candidates:
                    if schema_token:
                        from_token = "%s.%s" % (schema_token, table_token)
                    else:
                        from_token = table_token

                    sql_text = "SELECT COUNT(*) FROM %s" % from_token
                    try:
                        cursor.execute(sql_text)
                        cursor.fetchone()
                        return True
                    except Exception:
                        continue
        except Exception:
            return False
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

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

    def _instantiate_ischema_type(self, type_name, *args):
        entry = self.ischema_names.get(type_name)

        if entry is not None:
            try:
                if isinstance(entry, type):
                    return entry(*args)
                if callable(entry):
                    return entry(*args)
                return entry
            except TypeError:
                try:
                    if isinstance(entry, type):
                        return entry()
                except Exception:
                    pass

        # Fallbacks genéricos
        if type_name in ("CHAR", "NCHAR"):
            return sa_types.CHAR(args[0] if args else None)
        if type_name in ("VARCHAR", "NVARCHAR", "LVARCHAR"):
            return sa_types.VARCHAR(args[0] if args else None)
        if type_name == "SMALLINT":
            return sa_types.SmallInteger()
        if type_name in ("INTEGER", "SERIAL"):
            return sa_types.Integer()
        if type_name in ("INT8", "SERIAL8", "BIGINT", "BIGSERIAL"):
            return sa_types.BigInteger()
        if type_name in ("DECIMAL", "NUMERIC", "MONEY"):
            if len(args) >= 2:
                return sa_types.Numeric(args[0], args[1])
            return sa_types.Numeric()
        if type_name in ("FLOAT", "SMALLFLOAT"):
            return sa_types.Float()
        if type_name == "DATE":
            return sa_types.Date()
        if type_name == "DATETIME":
            return sa_types.DateTime()
        if type_name == "INTERVAL":
            return sa_types.Interval()
        if type_name == "TEXT":
            return sa_types.Text()
        if type_name == "BYTE":
            return sa_types.LargeBinary()
        if type_name == "BOOLEAN":
            return sa_types.Boolean()
        if type_name == "NULL":
            return sa_types.NullType()

        util.warn(f"Did not recognize Informix type '{type_name}'")
        return sa_types.NullType()

    def _decode_ifx_type(self, coltype, collength, extended_id=None):
        nullable = not bool(int(coltype) & 0x0100)
        base_code = int(coltype) & 0x00FF
        encoded_len = int(collength) if collength is not None else 0

        # Normaliza SMALLINT signed -> unsigned 16-bit cuando aplique
        if encoded_len < 0:
            encoded_len += 65536

        type_name = self._COLTYPE_CODE_MAP.get(base_code)
        autoincrement = base_code in (6, 18, 53)

        if type_name is None:
            util.warn(
                "Did not recognize Informix coltype code "
                f"{coltype!r} (base={base_code}, extended_id={extended_id!r})"
            )
            return sa_types.NullType(), autoincrement, nullable

        # CHAR / NCHAR / LVARCHAR
        if base_code in (0, 15, 40):
            return (
                self._instantiate_ischema_type(type_name, encoded_len),
                autoincrement,
                nullable,
            )

        # VARCHAR / NVARCHAR
        if base_code in (13, 16):
            length = encoded_len & 0x00FF
            return (
                self._instantiate_ischema_type(type_name, length),
                autoincrement,
                nullable,
            )

        # DECIMAL / MONEY: precision * 256 + scale
        if base_code in (5, 8):
            precision = encoded_len >> 8
            scale = encoded_len & 0x00FF
            return (
                self._instantiate_ischema_type(type_name, precision, scale),
                autoincrement,
                nullable,
            )

        return (
            self._instantiate_ischema_type(type_name),
            autoincrement,
            nullable,
        )

    def has_table(self, connection, table_name, schema=None, **kw):
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

    def get_temp_table_names(self, connection, schema=None, **kw):
        # Informix temp tables are connection-local and, with the ODBC
        # driver used by this dialect, are not exposed through a stable
        # catalog query or metadata API that lets us enumerate them
        # reliably. We therefore keep the contract explicit: has_table()
        # works for known temp names on the same connection, but listing
        # temp table names is not supported and returns an empty list.
        return []

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
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

    def get_materialized_view_names(self, connection, schema=None, **kw):
        # Informix does not expose a materialized-view concept through this
        # dialect, so the contract is explicit and empty.
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        # Explicit contract for a reflection surface we don't currently
        # implement for Informix in this dialect.
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        # Informix table comments are not currently reflected by this
        # dialect; return the stable SQLAlchemy structure explicitly.
        return {"text": None}

    def get_table_options(self, connection, table_name, schema=None, **kw):
        # Informix-specific table options are not currently reflected by
        # this dialect; return the stable SQLAlchemy structure explicitly.
        return {}

    def get_temp_view_names(self, connection, schema=None, **kw):
        # Informix does not support TEMP VIEW creation in the same way as
        # PostgreSQL/SQLite, so temp view enumeration is intentionally
        # unsupported for this dialect.
        return []

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
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
                d.type AS default_type,
                d.default AS default_value
            FROM syscolumns c
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
            default_type = row[5]
            default_value = row[6]

            satype, autoincrement, nullable = self._decode_ifx_type(
                coltype=coltype,
                collength=collength,
                extended_id=extended_id,
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

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        table_row = self._require_table_row(
            connection,
            table_name,
            schema=schema,
            tabtypes=("T",),
        )
        tabid = int(table_row[0])

        # Mapa de índices que duplican constraints
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
            if idxname:
                constraint_by_index[self._clean_str(idxname).lower()] = (
                    self._clean_str(ctype),
                    self.normalize_name(self._clean_str(cname)),
                )

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
        rows = connection.exec_driver_sql(idx_sql, (tabid,)).fetchall()

        indexes = []
        for row in rows:
            idxname = self._clean_str(row[0])
            owner = self._clean_str(row[1])
            idxtype = self._clean_str(row[2])

            key = idxname.lower() if idxname else None
            duplicated = constraint_by_index.get(key)

            # No duplicar el índice implícito de la PK
            if duplicated and duplicated[0] == "P":
                continue

            colnames, column_sorting = self._get_index_columns(
                connection,
                tabid,
                idxname,
                owner=owner,
            )
            if not colnames:
                continue

            idx_info = {
                "name": self.normalize_name(idxname),
                "column_names": colnames,
                "unique": idxtype in ("U", "u"),
            }

            if column_sorting:
                idx_info["column_sorting"] = column_sorting

            if duplicated and duplicated[0] == "U":
                idx_info["duplicates_constraint"] = duplicated[1]

            indexes.append(idx_info)

        return indexes

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
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

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
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
