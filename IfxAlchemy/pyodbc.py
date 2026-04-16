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
            ansi
            unicode_results
            autocommit
            odbc_autotranslate
            <any other param> -> appended as KEY=VALUE
        """
        opts = dict(url.translate_connect_args(username="user"))
        opts.update(dict(url.query))

        connect_args = {}
        for param in ("ansi", "unicode_results", "autocommit"):
            if param in opts:
                connect_args[param] = util.asbool(opts.pop(param))

        if "odbc_connect" in opts:
            return [[unquote_plus(opts.pop("odbc_connect"))], connect_args]

        keys = dict(opts)
        lowered_keys = {k.lower() for k in keys}

        if "needodbctypesonly" not in lowered_keys:
            # Ask the Informix ODBC driver to report standard ODBC types when
            # possible; this avoids pyodbc failures on Informix-specific types
            # such as SQL_INFX_BIGINT (-114).
            keys["NeedODBCTypesOnly"] = "1"

        # 1) DSN mode
        if "dsn" in keys or ("host" in keys and "database" not in keys and "server" not in keys):
            dsn = keys.pop("dsn", None) or keys.pop("host", "")
            connectors = [f"DSN={dsn}"]

        # 2) Explicit Informix connection string
        else:
            driver = keys.pop("driver", self.pyodbc_driver_name)
            host = keys.pop("host", "")
            database = keys.pop("database", "")
            server = keys.pop("server", "")
            protocol = keys.pop("protocol", "")
            service = keys.pop("service", "")

            # Backward compatibility: if only port is passed, use it as SERVICE
            if not service and "port" in keys:
                service = str(keys.pop("port"))

            connectors = [f"DRIVER={{{driver}}}"]

            if host:
                connectors.append(f"HOST={host}")
            if service:
                connectors.append(f"SERVICE={service}")
            if server:
                connectors.append(f"SERVER={server}")
            if database:
                connectors.append(f"DATABASE={database}")
            if protocol:
                connectors.append(f"PROTOCOL={protocol}")

        user = keys.pop("user", None)
        password = keys.pop("password", "")

        if user:
            connectors.append(f"UID={user}")
            connectors.append(f"PWD={password}")
        else:
            # Keep compatibility for DSN/trusted scenarios
            connectors.append("Trusted_Connection=Yes")

        if "odbc_autotranslate" in keys:
            connectors.append(f"AutoTranslate={keys.pop('odbc_autotranslate')}")

        # Append any remaining params untouched
        for k, v in keys.items():
            connectors.append(f"{k}={v}")

        return [[";".join(connectors)], connect_args]


# Alias expected by some SQLAlchemy dialect loaders
dialect = IfxDialect_pyodbc
