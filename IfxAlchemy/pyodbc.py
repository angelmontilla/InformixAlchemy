# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2008-2016 IBM Corporation
# Copyright (c) 2026 Angel Montilla
#
# Originally derived from IfxAlchemy / OpenInformix.
# Modified by Angel Montilla for pyodbc, local packaging,
# and SQLAlchemy 2.0 compatibility.
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

from urllib.parse import unquote

from sqlalchemy import types as sa_types
from sqlalchemy import util
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.engine import BindTyping

from .base import (
    DBCLOB,
    LONGVARGRAPHIC,
    XML,
    _SelectLastRowIDMixin,
    IfxDialect,
    IfxExecutionContext,
)


SQL_INFX_BIGINT = -114
INFX_BIGINT_BINARY_SIZE = 8


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


def _is_decimal_text(value):
    """
    Return True when ``value`` is a valid base-10 integer representation.

    Accepted examples:

        123
        +123
        -123

    Empty strings, isolated signs and non-decimal text are rejected.
    """
    if not value:
        return False

    return value.lstrip("+-").isdigit()


def _decode_infx_bigint_bytes(value):
    """
    Decode a bytes value returned by the Informix ODBC driver for
    SQL_INFX_BIGINT.

    Depending on the driver configuration and version, Informix may return
    BIGINT/BIGSERIAL values in either of these representations:

    1. Decimal ASCII bytes::

           b"1234567890123"
           b"-1234567890123"
           b"123\\x00\\x00"

    2. A signed 64-bit little-endian binary integer::

           b"\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00"

       The previous example represents the integer 1.

    Decimal ASCII must be checked before the binary format. Otherwise, an
    eight-character decimal value such as b"12345678" would be incorrectly
    interpreted as a binary integer.
    """
    if not value:
        return None

    # The driver may return an ASCII decimal value padded with NUL bytes.
    # This normalized copy is used only for the ASCII detection. The original
    # bytes object must remain untouched for binary decoding.
    ascii_candidate = value.rstrip(b"\x00").strip()

    if ascii_candidate:
        try:
            text_value = ascii_candidate.decode("ascii").strip()
        except UnicodeDecodeError:
            text_value = None

        if text_value is not None and _is_decimal_text(text_value):
            return int(text_value, 10)

    # IBM Informix can return SQL_INFX_BIGINT as an eight-byte signed integer
    # in little-endian order. Use the original value, including all NUL bytes.
    if len(value) == INFX_BIGINT_BINARY_SIZE:
        return int.from_bytes(
            value,
            byteorder="little",
            signed=True,
        )

    raise ValueError(
        "SQL_INFX_BIGINT returned bytes in an unknown binary format: "
        f"length={len(value)}, hex={value.hex()}"
    )


def _handle_infx_bigint(value):
    """
    Convert a SQL_INFX_BIGINT value returned by pyodbc into a Python int.

    pyodbc normally returns an int, but the IBM Informix ODBC Driver may also
    expose SQL_INFX_BIGINT values as decimal text or as an eight-byte signed
    little-endian binary value.
    """
    if value is None:
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, memoryview):
        value = value.tobytes()
    elif isinstance(value, bytearray):
        value = bytes(value)

    if isinstance(value, str):
        text_value = value.strip()

        if not text_value:
            return None

        if not _is_decimal_text(text_value):
            raise ValueError(
                "SQL_INFX_BIGINT returned a non-decimal string: "
                f"{text_value!r}"
            )

        return int(text_value, 10)

    if isinstance(value, bytes):
        return _decode_infx_bigint_bytes(value)

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "SQL_INFX_BIGINT returned an unsupported value: "
            f"type={type(value).__name__}, value={value!r}"
        ) from exc


class _IFXText_pyodbc(sa_types.Text):
    """Informix TEXT bound as ODBC SQL_LONGVARCHAR through pyodbc."""

    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_LONGVARCHAR


class IfxExecutionContext_pyodbc(
    _SelectLastRowIDMixin,
    IfxExecutionContext,
):
    pass


class IfxDialect_pyodbc(PyODBCConnector, IfxDialect):
    # Informix TEXT parameters must be described to the ODBC driver as
    # SQL_LONGVARCHAR. Restrict setinputsizes() to that DBAPI type so that
    # ordinary parameters continue to use pyodbc's normal type inference.
    bind_typing = BindTyping.SETINPUTSIZES
    include_set_input_sizes = frozenset()

    colspecs = dict(IfxDialect.colspecs)
    colspecs.update(
        {
            sa_types.Text: _IFXText_pyodbc,

            # CLOB, UnicodeText and the Informix-specific derived types are
            # subclasses of Text. Exact mappings prevent SQLAlchemy from
            # adapting them accidentally to _IFXText_pyodbc.
            sa_types.CLOB: sa_types.CLOB,
            sa_types.UnicodeText: sa_types.UnicodeText,
            DBCLOB: DBCLOB,
            LONGVARGRAPHIC: LONGVARGRAPHIC,
            XML: XML,
        }
    )

    driver = "pyodbc"
    supports_unicode_statements = True
    supports_char_length = True
    supports_native_decimal = False

    # SQLAlchemy requires concrete third-party dialects to opt in locally.
    supports_statement_cache = True

    execution_ctx_cls = IfxExecutionContext_pyodbc

    pyodbc_driver_name = "IBM INFORMIX ODBC DRIVER (64-bit)"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if self.dbapi is not None:
            sql_longvarchar = getattr(
                self.dbapi,
                "SQL_LONGVARCHAR",
                None,
            )

            if sql_longvarchar is not None:
                self.include_set_input_sizes = {
                    sql_longvarchar,
                }

    def do_set_input_sizes(
        self,
        cursor,
        list_of_tuples,
        context,
    ):
        # SQLAlchemy includes one entry per bound parameter. Parameters not
        # selected by include_set_input_sizes carry dbtype=None.
        #
        # Avoid calling cursor.setinputsizes() unless the statement contains
        # at least one Informix TEXT parameter.
        if not any(
            dbtype is not None
            for _, dbtype, _ in list_of_tuples
        ):
            return

        super().do_set_input_sizes(
            cursor,
            list_of_tuples,
            context,
        )

    @classmethod
    def import_dbapi(cls):
        return __import__("pyodbc")

    def on_connect(self):
        super_on_connect = super().on_connect()

        def on_connect(conn):
            if super_on_connect is not None:
                super_on_connect(conn)

            # IBM Informix exposes BIGINT/BIGSERIAL as SQL_INFX_BIGINT (-114)
            # unless the driver reports standard ODBC types. Register a
            # fallback converter that supports:
            #
            # - native Python integers;
            # - decimal strings;
            # - decimal ASCII bytes;
            # - signed 64-bit little-endian binary bytes.
            conn.add_output_converter(
                SQL_INFX_BIGINT,
                _handle_infx_bigint,
            )

        return on_connect

    def do_ping(self, dbapi_connection):
        cursor = dbapi_connection.cursor()

        try:
            cursor.execute(
                "SELECT FIRST 1 tabname "
                "FROM systables "
                "ORDER BY tabname"
            )
            cursor.fetchone()
            return True
        finally:
            cursor.close()

    def create_connect_args(self, url):
        """
        Build a pyodbc connection string for Informix using either:

        - odbc_connect=<full encoded ODBC string>
        - explicit parameters in the SQLAlchemy URL

        Supported URL/query parameters:

            user / password
            host
            database
            driver
            server
            protocol
            service
            port
                Mapped to SERVICE if service is not provided.
            dsn
            tctx=1 / trusted_context=true
                Mapped to TCTX=1.
            NeedODBCTypesOnly
                Defaults to 1.
            ansi
            unicode_results
            autocommit
            AutoTranslate / odbc_autotranslate
            <any other parameter>
                Appended as KEY=VALUE.
        """
        opts = dict(
            url.translate_connect_args(username="user")
        )
        opts.update(dict(url.query))

        connect_args = {}

        for param in (
            "ansi",
            "unicode_results",
            "autocommit",
        ):
            value = _pop_key_case_insensitive(
                opts,
                param,
            )

            if value is not None:
                connect_args[param] = util.asbool(value)

        odbc_connect = _pop_key_case_insensitive(
            opts,
            "odbc_connect",
        )

        if odbc_connect is not None:
            return [
                [unquote(odbc_connect)],
                connect_args,
            ]

        keys = dict(opts)

        need_odbc_types_only = _pop_key_case_insensitive(
            keys,
            "NeedODBCTypesOnly",
        )

        if need_odbc_types_only is None:
            # Ask the Informix ODBC driver to report standard ODBC types when
            # possible. The converter registered in on_connect() remains as a
            # fallback for SQL_INFX_BIGINT (-114).
            need_odbc_types_only = "1"

        delimident = _pop_key_case_insensitive(
            keys,
            "DELIMIDENT",
        )

        auto_translate = _pop_key_case_insensitive(
            keys,
            "AutoTranslate",
        )

        odbc_auto_translate = _pop_key_case_insensitive(
            keys,
            "odbc_autotranslate",
        )

        if auto_translate is None:
            auto_translate = odbc_auto_translate

        user = _pop_key_case_insensitive(
            keys,
            "user",
        )

        password = _pop_key_case_insensitive(
            keys,
            "password",
            "",
        )

        uid = _pop_key_case_insensitive(
            keys,
            "UID",
        )

        pwd = _pop_key_case_insensitive(
            keys,
            "PWD",
        )

        if user is None and uid is not None:
            user = uid
            password = "" if pwd is None else pwd
        elif pwd is not None and password in (None, ""):
            password = pwd

        tctx = _pop_key_case_insensitive(
            keys,
            "TCTX",
        )

        trusted_context = _pop_key_case_insensitive(
            keys,
            "trusted_context",
        )

        trusted_context_enabled = (
            tctx is not None
            and util.asbool(tctx)
        ) or (
            trusted_context is not None
            and util.asbool(trusted_context)
        )

        dsn = _pop_key_case_insensitive(
            keys,
            "dsn",
        )

        # odbc_connect was handled above as a literal passthrough.
        # DSN mode uses DSN plus optional authentication and driver options.
        if dsn is not None:
            connectors = [
                "DSN=%s" % _quote_odbc_value(dsn)
            ]

        # Explicit Informix connection string.
        else:
            driver = _pop_key_case_insensitive(
                keys,
                "driver",
                self.pyodbc_driver_name,
            )

            host = _pop_key_case_insensitive(
                keys,
                "host",
                "",
            )

            database = _pop_key_case_insensitive(
                keys,
                "database",
                "",
            )

            server = _pop_key_case_insensitive(
                keys,
                "server",
                "",
            )

            protocol = _pop_key_case_insensitive(
                keys,
                "protocol",
                "",
            )

            service = _pop_key_case_insensitive(
                keys,
                "service",
                "",
            )

            # Backward compatibility: when only port is supplied,
            # use it as the Informix SERVICE value.
            if not service:
                port = _pop_key_case_insensitive(
                    keys,
                    "port",
                )

                if port is not None:
                    service = str(port)

            connectors = [
                "DRIVER=%s"
                % _quote_odbc_value(
                    driver,
                    force=True,
                )
            ]

            if host:
                connectors.append(
                    "HOST=%s"
                    % _quote_odbc_value(host)
                )

            if service:
                connectors.append(
                    "SERVICE=%s"
                    % _quote_odbc_value(service)
                )

            if server:
                connectors.append(
                    "SERVER=%s"
                    % _quote_odbc_value(server)
                )

            if database:
                connectors.append(
                    "DATABASE=%s"
                    % _quote_odbc_value(database)
                )

            if protocol:
                connectors.append(
                    "PROTOCOL=%s"
                    % _quote_odbc_value(protocol)
                )

        if user:
            connectors.append(
                "UID=%s"
                % _quote_odbc_value(user)
            )

            connectors.append(
                "PWD=%s"
                % _quote_odbc_value(password)
            )

        elif trusted_context_enabled:
            connectors.append("TCTX=1")

        connectors.append(
            "NeedODBCTypesOnly=%s"
            % _quote_odbc_value(
                need_odbc_types_only
            )
        )

        if delimident is not None:
            connectors.append(
                "DELIMIDENT=%s"
                % _quote_odbc_value(delimident)
            )

        if auto_translate is not None:
            connectors.append(
                "AutoTranslate=%s"
                % _quote_odbc_value(auto_translate)
            )

        # Append any remaining parameters without changing their names.
        for key, value in keys.items():
            if value is not None:
                connectors.append(
                    "%s=%s"
                    % (
                        key,
                        _quote_odbc_value(value),
                    )
                )

        return [
            [";".join(connectors)],
            connect_args,
        ]


# Alias expected by some SQLAlchemy dialect loaders.
dialect = IfxDialect_pyodbc
