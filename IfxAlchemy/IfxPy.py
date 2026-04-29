# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2008-2016 IBM Corporation
# Copyright (c) 2026 Angel Montilla
#
# Originally derived from IfxAlchemy / OpenInformix.
# Modified by Angel Montilla to adapt IfxAlchemy to SQLAlchemy 2.0.
#
# Original authors: Sathyanesh Krishnan, Shilpa S Jadhav, Tim Powell
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

"""Legacy IfxPy backend.

The supported backend for this fork is ``informix+pyodbc``.  This module is
kept for source compatibility with older applications and tests, but it is not
registered as a public SQLAlchemy entry point.
"""

import re
from sqlalchemy import types as sa_types, util
from sqlalchemy.exc import ArgumentError
try:
    from sqlalchemy.engine import processors
except ImportError:  # pragma: no cover - compatibility with older SQLAlchemy
    from sqlalchemy import processors

from .base import IfxExecutionContext, IfxDialect

SQL_TXN_READ_UNCOMMITTED = 1
SQL_TXN_READ_COMMITTED = 2
SQL_TXN_REPEATABLE_READ = 4
SQL_TXN_SERIALIZABLE = 8
SQL_ATTR_TXN_ISOLATION = 108

VERSION_RE = re.compile(r'(\d+)\.(\d+)(.+\d+)')

CALLPROC_NAME_RE = re.compile(
    r"^\s*(?:EXECUTE\s+PROCEDURE|CALL)\s+([^\s(]+)",
    re.IGNORECASE,
)


class _IFX_Numeric_IfxPy(sa_types.Numeric):
    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            return None
        else:
            return processors.to_float


class IfxExecutionContext_IfxPy(IfxExecutionContext):
    _callproc_result = None
    _out_parameters = None

    #def get_lastrowid(self):
    #    return self.cursor.last_identity_val

    def get_lastrowid(self):
        return self._lastrowid

    def get_out_parameter_values(self, out_param_names):
        if not self._callproc_result or not self._out_parameters:
            return super().get_out_parameter_values(out_param_names)

        return [
            self._callproc_result[self.compiled.positiontup.index(name)]
            for name in out_param_names
        ]


class IfxDialect_IfxPy(IfxDialect):
    """Deprecated legacy dialect kept outside the supported public contract."""

    driver = "IfxPy"
    supports_unicode_statements = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_native_decimal = False
    supports_char_length = True
    supports_default_values = False
    insert_returning = False
    update_returning = False
    delete_returning = False
    supports_multivalues_insert = False
    use_insertmanyvalues = False
    use_insertmanyvalues_wo_returning = False
    supports_identity_columns = False
    supports_statement_cache = True

    execution_ctx_cls = IfxExecutionContext_IfxPy

    colspecs = util.update_copy(
        IfxDialect.colspecs,
        {
            sa_types.Numeric: _IFX_Numeric_IfxPy,
        },
    )

    _isolation_lookup = {
        "READ STABILITY",
        "RS",
        "UNCOMMITTED READ",
        "UR",
        "CURSOR STABILITY",
        "CS",
        "REPEATABLE READ",
        "RR",
    }

    _isolation_levels_cli = {
        "RR": SQL_TXN_SERIALIZABLE,
        "REPEATABLE READ": SQL_TXN_SERIALIZABLE,
        "UR": SQL_TXN_READ_UNCOMMITTED,
        "UNCOMMITTED READ": SQL_TXN_READ_UNCOMMITTED,
        "RS": SQL_TXN_REPEATABLE_READ,
        "READ STABILITY": SQL_TXN_REPEATABLE_READ,
        "CS": SQL_TXN_READ_COMMITTED,
        "CURSOR STABILITY": SQL_TXN_READ_COMMITTED,
    }

    _isolation_levels_returned = {
        value: key for key, value in _isolation_levels_cli.items()
    }

    @classmethod
    def import_dbapi(cls):
        """Return the underlying DBAPI driver module."""
        import IfxPyDbi as module

        return module

    @staticmethod
    def _extract_procedure_name(statement):
        match = CALLPROC_NAME_RE.match(statement or "")
        if match:
            return match.group(1)
        return statement

    @staticmethod
    def _normalize_isolation_level(level):
        if level is None:
            return "CS"

        normalized = level.strip()
        if not normalized:
            return "CS"

        return normalized.upper().replace("-", " ")

    def do_execute(self, cursor, statement, parameters, context=None):
        if context is not None and getattr(context, "_out_parameters", None):
            procedure_name = self._extract_procedure_name(statement)
            callproc_parameters = [] if parameters is None else parameters
            context._callproc_result = cursor.callproc(
                procedure_name, callproc_parameters
            )
            return

        if parameters is None:
            cursor.execute(statement)
        else:
            cursor.execute(statement, parameters)

    def _get_cli_isolation_levels(self, level):
        return self._isolation_levels_cli[level]

    def set_isolation_level(self, connection, level):
        normalized_level = self._normalize_isolation_level(level)

        if normalized_level not in self._isolation_lookup:
            raise ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s"
                % (
                    normalized_level,
                    self.name,
                    ", ".join(sorted(self._isolation_lookup)),
                )
            )

        attrib = {
            SQL_ATTR_TXN_ISOLATION: self._get_cli_isolation_levels(
                normalized_level
            )
        }
        connection.set_option(attrib)

    def reset_isolation_level(self, connection):
        self.set_isolation_level(connection, "CS")

    def create_connect_args(self, url):
        opts = url.translate_connect_args(
            username="uid",
            password="pwd",
            host="server",
            port="service",
        )

        query_items = {
            key.upper(): value
            for key, value in url.query.items()
            if value is not None
        }

        normalized_opts = {}
        for key, value in opts.items():
            if value is None:
                continue
            normalized_opts[key.upper()] = value

        for key, value in query_items.items():
            normalized_opts[key] = value

        connstr = ";".join(
            "%s=%s" % (key, value) for key, value in normalized_opts.items()
        )
        return ([connstr], {})

    def _get_default_schema_name(self, connection):
        current_schema = connection.connection.get_current_schema()
        return self.normalize_name(current_schema)

    def _get_server_version_info(self, connection):
        version = getattr(connection.connection, "dbms_ver", None)
        if not version:
            return ()

        match = VERSION_RE.match(version)
        if not match:
            return ()

        major, minor, rest = match.groups()
        return (int(major), int(minor), rest)

    def is_disconnect(self, ex, connection, cursor):
        dbapi_module = getattr(self, "dbapi", None)
        error_types = tuple(
            err_type
            for err_type in (
                getattr(dbapi_module, "ProgrammingError", None),
                getattr(dbapi_module, "OperationalError", None),
            )
            if isinstance(err_type, type)
        )

        if error_types and not isinstance(ex, error_types):
            return False

        message = str(ex)
        connection_errors = (
            "Connection is not active",
            "connection is no longer active",
            "Connection Resource cannot be found",
            "SQL30081N",
            "CLI0108E",
            "CLI0106E",
            "SQL1224N",
        )
        return any(err_msg in message for err_msg in connection_errors)


dialect = IfxDialect_IfxPy
