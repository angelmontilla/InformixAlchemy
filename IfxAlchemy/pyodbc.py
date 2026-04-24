# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2008-2016 IBM Corporation
# Copyright (c) 2026 Angel Montilla
#
# Originally derived from IfxAlchemy / OpenInformix.
# Modified by Angel Montilla for pyodbc, local packaging, and SQLAlchemy 2.0 compatibility.
#
# Original authors: Sathyanesh Krishnan, Shilpa S Jadhav
# Additional authors: Jaimy Azle, Rahul Priyadarshi
# Contributors: Mike Bayer, Angel Montilla
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

from urllib.parse import unquote_plus

from sqlalchemy import util
from sqlalchemy.connectors.pyodbc import PyODBCConnector

from .base import _SelectLastRowIDMixin, IfxExecutionContext, IfxDialect
from . import reflection as ifx_reflection


def _quote_odbc_value(value, force=False):
    if value is None:
        value = ""

    value = str(value)
    needs_quotes = (
        force
        or ";" in value
        or "{" in value
        or "}" in value
        or value[:1].isspace()
        or value[-1:].isspace()
    )

    if needs_quotes:
        return "{%s}" % value.replace("}", "}}")

    return value


def _pop_key_case_insensitive(mapping, key, default=None):
    lowered = key.lower()
    for existing_key in list(mapping):
        if existing_key.lower() == lowered:
            return mapping.pop(existing_key)
    return default


class IfxExecutionContext_pyodbc(_SelectLastRowIDMixin, IfxExecutionContext):
    pass


class IfxDialect_pyodbc(PyODBCConnector, IfxDialect):
    supports_unicode_statements = True
    supports_char_length = True
    supports_native_decimal = False
    supports_statement_cache = False

    execution_ctx_cls = IfxExecutionContext_pyodbc

    pyodbc_driver_name = "IBM INFORMIX ODBC DRIVER (64-bit)"

    @classmethod
    def import_dbapi(cls):
        return __import__("pyodbc")

    dbapi = import_dbapi

    def on_connect(self):
        super_ = super().on_connect()

        def _handle_infx_bigint(value):
            if value is None:
                return None

            if isinstance(value, memoryview):
                value = value.tobytes()

            if isinstance(value, bytearray):
                value = bytes(value)

            if isinstance(value, bytes):
                raw = value.rstrip(b"\x00").strip()
                if not raw:
                    return None

                # SQL_INFX_BIGINT is typically surfaced as a textual 64-bit
                # integer payload by the Informix ODBC driver.
                try:
                    return int(raw.decode("ascii"))
                except (UnicodeDecodeError, ValueError):
                    if len(raw) in (1, 2, 4, 8):
                        try:
                            return int.from_bytes(raw, byteorder="little", signed=True)
                        except ValueError:
                            pass
                    text = raw.decode("latin1", errors="ignore").strip()
                    if text:
                        return int(text)
                    raise

            return int(value)

        def on_connect(conn):
            if super_ is not None:
                super_(conn)

            # IBM Informix exposes BIGINT/BIGSERIAL as SQL_INFX_BIGINT (-114)
            # unless the driver is asked for standard ODBC types. Register a
            # fallback converter so pyodbc can still consume these values.
            conn.add_output_converter(-114, _handle_infx_bigint)

        return on_connect

    def do_ping(self, dbapi_connection):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT FIRST 1 tabname FROM systables ORDER BY tabname")
            cursor.fetchone()
            return True
        finally:
            cursor.close()

    def create_connect_args(self, url):
        """
        Build a pyodbc connection string for Informix using either:
        - odbc_connect=<full encoded ODBC string>
        - explicit parameters in the SQLAlchemy URL

        Supported URL/query params:
            user / password
            host
            database
            driver
            server
            protocol
            service
            port     -> mapped to SERVICE if service is not provided
            dsn
            tctx=1 / trusted_context=true -> mapped to TCTX=1
            NeedODBCTypesOnly (defaults to 1)
            ansi
            unicode_results
            autocommit
            AutoTranslate / odbc_autotranslate
            <any other param> -> appended as KEY=VALUE
        """
        opts = dict(url.translate_connect_args(username="user"))
        opts.update(dict(url.query))

        connect_args = {}
        for param in ("ansi", "unicode_results", "autocommit"):
            value = _pop_key_case_insensitive(opts, param)
            if value is not None:
                connect_args[param] = util.asbool(value)

        odbc_connect = _pop_key_case_insensitive(opts, "odbc_connect")
        if odbc_connect is not None:
            return [[unquote_plus(odbc_connect)], connect_args]

        keys = dict(opts)

        need_odbc_types_only = _pop_key_case_insensitive(
            keys, "NeedODBCTypesOnly"
        )
        if need_odbc_types_only is None:
            # Ask the Informix ODBC driver to report standard ODBC types when
            # possible; this avoids pyodbc failures on Informix-specific types
            # such as SQL_INFX_BIGINT (-114).
            need_odbc_types_only = "1"

        delimident = _pop_key_case_insensitive(keys, "DELIMIDENT")
        auto_translate = _pop_key_case_insensitive(keys, "AutoTranslate")
        odbc_auto_translate = _pop_key_case_insensitive(
            keys, "odbc_autotranslate"
        )
        if auto_translate is None:
            auto_translate = odbc_auto_translate

        user = _pop_key_case_insensitive(keys, "user")
        password = _pop_key_case_insensitive(keys, "password", "")
        uid = _pop_key_case_insensitive(keys, "UID")
        pwd = _pop_key_case_insensitive(keys, "PWD")
        if user is None and uid is not None:
            user = uid
            password = "" if pwd is None else pwd
        elif pwd is not None and password in (None, ""):
            password = pwd

        tctx = _pop_key_case_insensitive(keys, "TCTX")
        trusted_context = _pop_key_case_insensitive(keys, "trusted_context")
        trusted_context_enabled = (
            (tctx is not None and util.asbool(tctx))
            or (
                trusted_context is not None
                and util.asbool(trusted_context)
            )
        )

        dsn = _pop_key_case_insensitive(keys, "dsn")

        # 1) odbc_connect was handled above as a literal passthrough.
        # 2) DSN mode uses only DSN plus optional auth/driver options.
        if dsn is not None:
            connectors = ["DSN=%s" % _quote_odbc_value(dsn)]

        # 3) Explicit Informix connection string.
        else:
            driver = _pop_key_case_insensitive(
                keys, "driver", self.pyodbc_driver_name
            )
            host = _pop_key_case_insensitive(keys, "host", "")
            database = _pop_key_case_insensitive(keys, "database", "")
            server = _pop_key_case_insensitive(keys, "server", "")
            protocol = _pop_key_case_insensitive(keys, "protocol", "")
            service = _pop_key_case_insensitive(keys, "service", "")

            # Backward compatibility: if only port is passed, use it as SERVICE
            if not service:
                port = _pop_key_case_insensitive(keys, "port")
                if port is not None:
                    service = str(port)

            connectors = [
                "DRIVER=%s" % _quote_odbc_value(driver, force=True)
            ]

            if host:
                connectors.append("HOST=%s" % _quote_odbc_value(host))
            if service:
                connectors.append("SERVICE=%s" % _quote_odbc_value(service))
            if server:
                connectors.append("SERVER=%s" % _quote_odbc_value(server))
            if database:
                connectors.append("DATABASE=%s" % _quote_odbc_value(database))
            if protocol:
                connectors.append("PROTOCOL=%s" % _quote_odbc_value(protocol))

        if user:
            connectors.append("UID=%s" % _quote_odbc_value(user))
            connectors.append("PWD=%s" % _quote_odbc_value(password))
        elif trusted_context_enabled:
            connectors.append("TCTX=1")

        connectors.append(
            "NeedODBCTypesOnly=%s" % _quote_odbc_value(need_odbc_types_only)
        )
        if delimident is not None:
            connectors.append("DELIMIDENT=%s" % _quote_odbc_value(delimident))
        if auto_translate is not None:
            connectors.append(
                "AutoTranslate=%s" % _quote_odbc_value(auto_translate)
            )

        # Append any remaining params untouched
        for k, v in keys.items():
            if v is not None:
                connectors.append("%s=%s" % (k, _quote_odbc_value(v)))

        return [[";".join(connectors)], connect_args]


# Alias expected by some SQLAlchemy dialect loaders
dialect = IfxDialect_pyodbc
