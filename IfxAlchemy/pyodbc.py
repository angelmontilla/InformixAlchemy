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

import re
import threading
from collections.abc import Mapping
from urllib.parse import unquote

from sqlalchemy import types as sa_types
from sqlalchemy import util
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.engine import BindTyping
from sqlalchemy.exc import ArgumentError

from .complex import DISTINCT, LIST, MULTISET, ROW, SET

from .base import (
    BSON,
    DBCLOB,
    JSON,
    LONGVARGRAPHIC,
    LVARCHAR,
    XML,
    _SelectLastRowIDMixin,
    IfxDialect,
    IfxExecutionContext,
)


SQL_INFX_BIGINT = -114
INFX_BIGINT_BINARY_SIZE = 8


# SQLSTATE class 08 is reserved for connection exceptions. SQLSTATE 01002
# is the ODBC-standard "disconnect error" warning. pyodbc normally places
# the SQLSTATE in ``error.args[0]``, but driver-manager and platform
# combinations can embed it in a longer diagnostic string or expose it as
# an exception attribute.
_DISCONNECT_SQLSTATE_RE = re.compile(
    r"(?<![A-Z0-9])(?:08[A-Z0-9]{3}|01002)(?![A-Z0-9])"
)

# Native Informix / Informix ODBC diagnostics that identify a failed or
# unavailable physical connection even when the driver reports a generic
# SQLSTATE such as HY000 or S1000. Keep this list deliberately narrow:
# operational errors that leave the DBAPI connection usable must not
# invalidate the pool.
_DISCONNECT_NATIVE_ERROR_CODES = frozenset(
    {
        -908,  # Attempt to connect to database server failed.
        -930,  # Cannot connect to database server.
        -11020,  # Communication link failure.
        -25580,  # System error occurred in a network function.
        -25582,  # Network connection is broken.
    }
)
_DISCONNECT_NATIVE_ERROR_RE = re.compile(
    r"(?<!\d)(?:%s)(?!\d)"
    % "|".join(
        re.escape(str(code))
        for code in sorted(
            _DISCONNECT_NATIVE_ERROR_CODES,
            key=lambda code: (-len(str(code)), code),
        )
    )
)
_DISCONNECT_NATIVE_CONTEXT_RE = re.compile(
    r"\b(?:INFORMIX|SQLCODE|NATIVE(?: ERROR(?: CODE)?)?)\b"
)

# Explicit connection-loss messages used by Informix, pyodbc, unixODBC and
# common operating-system socket layers. Avoid generic tokens such as
# ``NETWORK`` or all timeout errors because those produce false pool
# invalidations for recoverable statement/configuration failures.
_DISCONNECT_MESSAGE_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"\b(?:DATABASE\s+)?CONNECTION "
        r"(?:IS |HAS BEEN |WAS )?"
        r"(?:CLOSED|LOST|BROKEN|RESET|ABORTED|TERMINATED)\b",
        r"\bCONNECTION (?:IS )?(?:NO LONGER|NOT) "
        r"(?:ACTIVE|OPEN|VALID|USABLE)\b",
        r"\bATTEMPT TO USE (?:A )?CLOSED CONNECTION\b",
        r"\bCANNOT (?:ROLLBACK|COMMIT|EXECUTE|OPERATE) "
        r"(?:ON|USING) (?:A )?CLOSED CONNECTION\b",
        r"\b(?:COMMUNICATION|CONNECTION) LINK FAILURE\b",
        r"\bCOMMUNICATION FAILURE\b",
        r"\bCONNECTION FAILURE\b",
        r"\bNETWORK CONNECTION "
        r"(?:IS |HAS BEEN |WAS )?"
        r"(?:BROKEN|CLOSED|LOST|RESET|ABORTED|TERMINATED)\b",
        r"\bGENERAL NETWORK ERROR\b",
        r"\bSYSTEM ERROR OCCURRED IN (?:A )?NETWORK FUNCTION\b",
        r"\b(?:DATABASE )?SERVER "
        r"(?:IS |WAS )?(?:NOT AVAILABLE|UNAVAILABLE|DOWN|OFFLINE)\b",
        r"\b(?:CANNOT|CAN'T|COULD NOT|FAILED TO) CONNECT TO "
        r"(?:THE )?(?:DATABASE )?SERVER\b",
        r"\bATTEMPT TO CONNECT TO "
        r"(?:THE )?(?:DATABASE )?SERVER FAILED\b",
        r"\bCONNECTION (?:WAS )?REFUSED\b",
        r"\bBROKEN PIPE\b",
        r"\bCONNECTION RESET BY PEER\b",
        r"\bCONNECTION ABORTED BY (?:HOST|PEER|SOFTWARE)\b",
        r"\bSOCKET "
        r"(?:IS |HAS BEEN |WAS )?"
        r"(?:CLOSED|RESET|NOT CONNECTED|DISCONNECTED)\b",
        r"\bTCP(?:/IP)? CONNECTION "
        r"(?:IS |HAS BEEN |WAS )?"
        r"(?:BROKEN|CLOSED|RESET|ABORTED|TERMINATED)\b",
        r"\bSERVER CLOSED THE CONNECTION\b",
        r"\bREMOTE HOST "
        r"(?:CLOSED|RESET|ABORTED) (?:THE )?CONNECTION\b",
    )
)

_DIAGNOSTIC_ATTRIBUTE_NAMES = (
    "sqlstate",
    "sql_state",
    "native_error",
    "native_code",
    "sqlcode",
    "driver_error",
    "message",
)

_EXCEPTION_LINK_ATTRIBUTE_NAMES = (
    "orig",
    "__cause__",
    "__context__",
)


def _safe_getattr(value, attribute_name, default=None):
    """Read a diagnostic attribute without trusting driver properties."""

    try:
        return getattr(value, attribute_name, default)
    except Exception:
        return default


def _flatten_exception_values(value, seen=None):
    """Yield scalar values from nested DBAPI diagnostic containers.

    Driver wrappers occasionally place pyodbc diagnostics in nested tuples,
    lists or mappings. Cycle detection keeps malformed wrapper payloads from
    recursing forever.
    """

    if value is None:
        return

    if isinstance(
        value,
        (str, bytes, bytearray, memoryview, int, float),
    ):
        yield value
        return

    if seen is None:
        seen = set()

    value_id = id(value)
    if value_id in seen:
        return

    if isinstance(value, Mapping):
        seen.add(value_id)
        for item in value.values():
            yield from _flatten_exception_values(item, seen)
        return

    if isinstance(value, (tuple, list, set, frozenset)):
        seen.add(value_id)
        for item in value:
            yield from _flatten_exception_values(item, seen)
        return

    yield value


def _iter_exception_chain(error):
    """Yield an exception and any wrapped DBAPI exceptions exactly once."""

    pending = [error]
    seen = set()

    while pending:
        current = pending.pop()
        if not isinstance(current, BaseException):
            continue

        current_id = id(current)
        if current_id in seen:
            continue

        seen.add(current_id)
        yield current

        for attribute_name in _EXCEPTION_LINK_ATTRIBUTE_NAMES:
            linked = _safe_getattr(current, attribute_name)
            if isinstance(linked, BaseException):
                pending.append(linked)


def _stringify_diagnostic_value(value):
    """Return text for a diagnostic value without propagating driver bugs."""

    if isinstance(value, memoryview):
        value = value.tobytes()
    elif isinstance(value, bytearray):
        value = bytes(value)

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    try:
        return str(value)
    except Exception:
        return ""


def _iter_diagnostic_values(error):
    """Yield diagnostic values while avoiding SQLAlchemy wrapper SQL text."""

    for current in _iter_exception_chain(error):
        # SQLAlchemy-style wrapper exceptions expose the real DBAPI exception
        # through ``orig`` and can include the SQL statement and parameter
        # values in their own string/args. Those values are not diagnostics
        # and can contain coincidental SQLSTATEs or Informix error numbers.
        has_orig = isinstance(_safe_getattr(current, "orig"), BaseException)

        if not has_orig:
            yield from _flatten_exception_values(
                _safe_getattr(current, "args", ())
            )

        for attribute_name in _DIAGNOSTIC_ATTRIBUTE_NAMES:
            attribute_value = _safe_getattr(current, attribute_name)
            if attribute_value is None or callable(attribute_value):
                continue
            yield from _flatten_exception_values(attribute_value)

        if not has_orig:
            # Some exception classes expose useful information only through
            # ``__str__`` rather than through args or diagnostic attributes.
            yield current


def _disconnect_diagnostic_text(error):
    """Return normalized diagnostics from an error and wrapped errors."""

    return " ".join(
        text
        for value in _iter_diagnostic_values(error)
        if (text := _stringify_diagnostic_value(value))
    ).upper()


def _has_disconnect_native_error(error):
    """Detect selected native codes without matching arbitrary SQL values."""

    native_code_texts = {
        str(code) for code in _DISCONNECT_NATIVE_ERROR_CODES
    }

    for value in _iter_diagnostic_values(error):
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and value in _DISCONNECT_NATIVE_ERROR_CODES
        ):
            return True

        text = _stringify_diagnostic_value(value).upper()
        if not text or not _DISCONNECT_NATIVE_ERROR_RE.search(text):
            continue

        if text.strip() in native_code_texts:
            return True

        if _DISCONNECT_NATIVE_CONTEXT_RE.search(text):
            return True

    return False


def _connection_is_closed(connection, cursor):
    """Return True when a supplied DBAPI connection explicitly says closed."""

    candidates = [connection]
    cursor_connection = _safe_getattr(cursor, "connection")
    if cursor_connection is not None:
        candidates.append(cursor_connection)

    seen = set()
    while candidates:
        candidate = candidates.pop()
        if candidate is None:
            continue

        candidate_id = id(candidate)
        if candidate_id in seen:
            continue
        seen.add(candidate_id)

        closed = _safe_getattr(candidate, "closed")
        if closed is True or (
            isinstance(closed, int)
            and not isinstance(closed, bool)
            and closed != 0
        ):
            return True

        for attribute_name in (
            "dbapi_connection",
            "driver_connection",
            "connection",
        ):
            nested = _safe_getattr(candidate, attribute_name)
            if nested is not None and nested is not candidate:
                candidates.append(nested)

    return False


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


class _IFXLVarchar_pyodbc(LVARCHAR):
    """Bind native Informix LVARCHAR as ODBC SQL_VARCHAR.

    IBM Informix ODBC documents LVARCHAR as SQL_VARCHAR.  SQL_LONGVARCHAR is
    reserved for TEXT and can make the driver attempt a TEXT-to-LVARCHAR cast.
    """

    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_VARCHAR


class IfxExecutionContext_pyodbc(
    _SelectLastRowIDMixin,
    IfxExecutionContext,
):
    pass


class IfxDialect_pyodbc(PyODBCConnector, IfxDialect):
    # ODBC transaction isolation constants are stable values defined by the
    # ODBC specification.  The runtime DBAPI constants are preferred when
    # available, while these values keep the dialect testable with lightweight
    # DBAPI doubles and older pyodbc builds.
    _odbc_sql_txn_read_uncommitted = 1
    _odbc_sql_txn_read_committed = 2
    _odbc_sql_txn_repeatable_read = 4
    _odbc_sql_txn_serializable = 8

    # Informix exposes four native session isolation modes through
    # ``SET ISOLATION``: DIRTY READ, COMMITTED READ, CURSOR STABILITY and
    # REPEATABLE READ.  The ODBC connection attribute only exposes three
    # generic levels and cannot distinguish COMMITTED READ from CURSOR
    # STABILITY, so SQLAlchemy isolation changes are emitted with Informix SQL
    # rather than SQL_ATTR_TXN_ISOLATION.
    #
    # READ STABILITY / RS is retained as a compatibility spelling from the
    # legacy IfxPy backend. Informix has no separate native READ STABILITY
    # mode, therefore it maps conservatively to strict REPEATABLE READ.
    _isolation_level_to_informix_sql = {
        "DIRTY READ": "DIRTY READ",
        "UNCOMMITTED READ": "DIRTY READ",
        "UR": "DIRTY READ",
        "READ UNCOMMITTED": "DIRTY READ",
        "COMMITTED READ": "COMMITTED READ",
        "READ COMMITTED": "COMMITTED READ",
        "CURSOR STABILITY": "CURSOR STABILITY",
        "CS": "CURSOR STABILITY",
        "READ STABILITY": "REPEATABLE READ",
        "RS": "REPEATABLE READ",
        "REPEATABLE READ": "REPEATABLE READ",
        "RR": "REPEATABLE READ",
        "SERIALIZABLE": "REPEATABLE READ",
    }

    _advertised_isolation_levels = (
        "AUTOCOMMIT",
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE",
        "DIRTY READ",
        "UNCOMMITTED READ",
        "UR",
        "COMMITTED READ",
        "CURSOR STABILITY",
        "CS",
        "READ STABILITY",
        "RS",
        "RR",
    )

    # Informix TEXT and LVARCHAR parameters need explicit, distinct ODBC
    # descriptors: TEXT uses SQL_LONGVARCHAR, while LVARCHAR uses SQL_VARCHAR.
    # Ordinary character parameters continue to use pyodbc inference.
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

            # NCHAR and NVARCHAR are locale-sensitive native Informix
            # datatypes, not aliases for CHAR/VARCHAR or for the dialect's
            # GRAPHIC mapping of generic Unicode.  Exact mappings preserve
            # their visit names and leave Python str handling to pyodbc.
            sa_types.NCHAR: sa_types.NCHAR,
            sa_types.NVARCHAR: sa_types.NVARCHAR,

            # LVARCHAR remains a native opaque Informix type.  Its pyodbc
            # implementation reports SQL_VARCHAR, which is the ODBC mapping
            # documented by IBM.  SQL_LONGVARCHAR is intentionally reserved
            # for Informix TEXT.
            LVARCHAR: _IFXLVarchar_pyodbc,
            DBCLOB: DBCLOB,
            LONGVARGRAPHIC: LONGVARGRAPHIC,
            XML: XML,
            JSON: JSON,
            BSON: BSON,
            LIST: LIST,
            SET: SET,
            MULTISET: MULTISET,
            ROW: ROW,
            DISTINCT: DISTINCT,
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

        # SQLAlchemy stores the create_engine(isolation_level=...) value on
        # the dialect and compares it with default_isolation_level when a
        # pooled connection is reset. Normalize the engine-level spelling at
        # construction time so aliases such as ``cursor_stability`` use the
        # same canonical form reported by get_isolation_level().
        if self._on_connect_isolation_level is not None:
            self._on_connect_isolation_level = self._normalize_isolation_level(
                self._on_connect_isolation_level
            )

        # pyodbc exposes SQLSetConnectAttr through set_attr(), but it does not
        # expose SQLGetConnectAttr.  Keep the last successfully requested
        # level per physical DBAPI connection so Connection.get_isolation_level
        # can report the effective setting and SQLAlchemy can restore pooled
        # connections deterministically.  Entries are removed when the DBAPI
        # connection is physically closed.
        self._isolation_level_by_connection_id = {}
        self._isolation_level_state_lock = threading.RLock()

        if self.dbapi is not None:
            selected_input_sizes = set()

            sql_longvarchar = getattr(
                self.dbapi,
                "SQL_LONGVARCHAR",
                None,
            )
            if sql_longvarchar is not None:
                selected_input_sizes.add(sql_longvarchar)

            sql_varchar = getattr(
                self.dbapi,
                "SQL_VARCHAR",
                None,
            )
            if sql_varchar is not None:
                selected_input_sizes.add(sql_varchar)

            sql_varbinary = getattr(
                self.dbapi,
                "SQL_VARBINARY",
                None,
            )
            if sql_varbinary is not None:
                selected_input_sizes.add(sql_varbinary)

            sql_longvarbinary = getattr(
                self.dbapi,
                "SQL_LONGVARBINARY",
                None,
            )
            if sql_longvarbinary is not None:
                selected_input_sizes.add(sql_longvarbinary)

            self.include_set_input_sizes = selected_input_sizes

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
        # an Informix TEXT or LVARCHAR parameter.  LVARCHAR must use
        # SQL_VARCHAR rather than SQL_LONGVARCHAR because the latter maps to
        # Informix TEXT.  Supply the declared byte maximum so pyodbc does not
        # reclassify long Python strings as a long-character parameter.
        prepared_input_sizes = []
        has_selected_type = False
        sql_varchar = (
            getattr(self.dbapi, "SQL_VARCHAR", None)
            if self.dbapi is not None
            else None
        )
        sql_varbinary = (
            getattr(self.dbapi, "SQL_VARBINARY", None)
            if self.dbapi is not None
            else None
        )

        for key, dbtype, sqltype in list_of_tuples:
            if (
                dbtype is not None
                and sql_varchar is not None
                and dbtype == sql_varchar
            ):
                if isinstance(sqltype, LVARCHAR):
                    length = (
                        sqltype.length if sqltype.length is not None else 2048
                    )
                    dbtype = (sql_varchar, length, 0)
                elif isinstance(sqltype, (LIST, SET, MULTISET, ROW, DISTINCT)):
                    # Complex types use their external text representation.
                    # Keep SQL_VARCHAR so the Informix driver does not promote
                    # long constructor strings to TEXT, which has no implicit
                    # assignment path to collection/ROW values.
                    dbtype = (sql_varchar, 0, 0)
                elif isinstance(sqltype, JSON) or (
                    isinstance(sqltype, BSON)
                    and getattr(sqltype, "transport", "json") == "json"
                ):
                    # pyodbc otherwise promotes long Python strings to
                    # SQL_LONGVARCHAR.  The IBM Informix ODBC driver maps that
                    # descriptor to TEXT, and Informix has no TEXT -> JSON
                    # cast.  Column-size 0 keeps SQL_VARCHAR while allowing
                    # data-at-execution for documents larger than the normal
                    # inline buffer.
                    dbtype = (sql_varchar, 0, 0)

            if (
                dbtype is not None
                and sql_varbinary is not None
                and dbtype == sql_varbinary
                and isinstance(sqltype, BSON)
                and getattr(sqltype, "transport", "json") == "binary"
            ):
                # Preserve variable-binary binding for diagnosed native BSON
                # codecs without forcing the value into a BLOB descriptor.
                dbtype = (sql_varbinary, 0, 0)

            if dbtype is not None:
                has_selected_type = True

            prepared_input_sizes.append((key, dbtype, sqltype))

        if not has_selected_type:
            return

        super().do_set_input_sizes(
            cursor,
            prepared_input_sizes,
            context,
        )

    @classmethod
    def import_dbapi(cls):
        return __import__("pyodbc")

    def is_disconnect(self, error, connection, cursor):
        """Identify pyodbc diagnostics that invalidate an Informix session.

        SQLAlchemy uses this result to discard the failed physical connection
        and, for pool pre-ping, to retry with a newly created connection.
        Informix ODBC diagnostics vary by driver version, driver manager and
        operating system, so classification is based on SQLSTATE class 08,
        narrowly selected native connectivity errors, explicit connection
        failure messages and an explicit DBAPI ``closed`` state. It is not
        based on the broad DBAPI exception class.
        """

        if super().is_disconnect(error, connection, cursor):
            return True

        if _connection_is_closed(connection, cursor):
            return True

        text = _disconnect_diagnostic_text(error)

        if _DISCONNECT_SQLSTATE_RE.search(text):
            return True

        if _has_disconnect_native_error(error):
            return True

        return any(
            pattern.search(text)
            for pattern in _DISCONNECT_MESSAGE_PATTERNS
        )

    @staticmethod
    def _normalize_isolation_level(level):
        if level is None:
            return ""

        normalized = str(level).strip().upper()
        normalized = normalized.replace("_", " ").replace("-", " ")
        return " ".join(normalized.split())

    def _dbapi_constant(self, name, fallback):
        if self.dbapi is None:
            return fallback
        return getattr(self.dbapi, name, fallback)

    def _remember_isolation_level(self, dbapi_connection, level):
        with self._isolation_level_state_lock:
            self._isolation_level_by_connection_id[id(dbapi_connection)] = level

    def _remembered_isolation_level(self, dbapi_connection):
        with self._isolation_level_state_lock:
            return self._isolation_level_by_connection_id.get(
                id(dbapi_connection)
            )

    def _forget_isolation_level(self, dbapi_connection):
        with self._isolation_level_state_lock:
            self._isolation_level_by_connection_id.pop(
                id(dbapi_connection),
                None,
            )

    def get_isolation_level_values(self, dbapi_connection):
        """Return every isolation-level spelling accepted by the dialect."""

        return list(self._advertised_isolation_levels)

    def get_default_isolation_level(self, dbapi_connection):
        """Return the SQLAlchemy reset baseline for this physical connection.

        Without an engine-level isolation setting, the baseline is the ODBC
        driver's ``SQL_DEFAULT_TXN_ISOLATION`` value.  When the Engine was
        created with ``isolation_level=...``, SQLAlchemy applies that setting
        through its built-in first-connect hook *before* dialect
        initialization calls this method.  In that case, the configured level
        is the initial connection level and therefore must become
        ``default_isolation_level`` for this Engine.

        Returning the raw driver default after an engine-level setting would
        make ``DefaultDialect.reset_isolation_level()`` compare two different
        baselines and fail during pool check-in.  AUTOCOMMIT is the exception:
        it is a DBAPI mode layered over an underlying transactional level, so
        the latter remains the reset baseline reported here.
        """

        default_value = self._odbc_sql_txn_read_committed
        info_code = None

        if self.dbapi is not None:
            info_code = getattr(
                self.dbapi,
                "SQL_DEFAULT_TXN_ISOLATION",
                None,
            )

        if info_code is not None and hasattr(dbapi_connection, "getinfo"):
            try:
                default_value = int(dbapi_connection.getinfo(info_code))
            except Exception:
                # SQLAlchemy requires this method not to leak driver-specific
                # exceptions during first-connect initialization.
                default_value = self._odbc_sql_txn_read_committed

        default_names = {
            self._dbapi_constant(
                "SQL_TXN_READ_UNCOMMITTED",
                self._odbc_sql_txn_read_uncommitted,
            ): "READ UNCOMMITTED",
            self._dbapi_constant(
                "SQL_TXN_READ_COMMITTED",
                self._odbc_sql_txn_read_committed,
            ): "READ COMMITTED",
            self._dbapi_constant(
                "SQL_TXN_REPEATABLE_READ",
                self._odbc_sql_txn_repeatable_read,
            ): "READ STABILITY",
            self._dbapi_constant(
                "SQL_TXN_SERIALIZABLE",
                self._odbc_sql_txn_serializable,
            ): "SERIALIZABLE",
        }
        driver_default_level = default_names.get(
            default_value,
            "READ COMMITTED",
        )

        engine_level = self._on_connect_isolation_level
        if engine_level and engine_level != "AUTOCOMMIT":
            # The built-in on-connect listener has already called
            # set_isolation_level(), so the remembered value is the effective
            # server setting.  Fall back to the normalized engine value only
            # for lightweight DBAPI doubles that bypass that listener.
            initial_level = (
                self._remembered_isolation_level(dbapi_connection)
                or engine_level
            )
            self._remember_isolation_level(
                dbapi_connection,
                initial_level,
            )
            return initial_level

        # No engine-wide override (or AUTOCOMMIT, which does not replace the
        # underlying transactional level): retain the physical driver default.
        if self._remembered_isolation_level(dbapi_connection) is None:
            self._remember_isolation_level(
                dbapi_connection,
                driver_default_level,
            )

        return driver_default_level

    def get_isolation_level(self, dbapi_connection):
        """Return the last effective non-autocommit level for a connection."""

        level = self._remembered_isolation_level(dbapi_connection)
        if level is not None:
            return level
        return self.get_default_isolation_level(dbapi_connection)

    def set_isolation_level(self, dbapi_connection, level):
        """Set the enduring Informix session isolation level.

        Informix 14.10+ supports ``SET ISOLATION`` as a complete-connection
        setting.  Using the native statement is required to preserve the
        distinction between COMMITTED READ and CURSOR STABILITY, which the
        generic ODBC isolation attribute cannot represent.

        SQLAlchemy invokes this hook only when no SQLAlchemy transaction is
        active.  If the connection was in DBAPI autocommit mode, leave that
        mode before applying a transactional isolation level.
        """

        normalized_level = self._normalize_isolation_level(level)

        if normalized_level == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
            return

        isolation_clause = self._isolation_level_to_informix_sql.get(
            normalized_level
        )
        if isolation_clause is None:
            raise ArgumentError(
                "Invalid value %r for isolation_level. "
                "Valid isolation levels for %r are %s"
                % (
                    normalized_level,
                    self.name,
                    ", ".join(self._advertised_isolation_levels),
                )
            )

        was_autocommit = bool(
            getattr(dbapi_connection, "autocommit", False)
        )
        if was_autocommit:
            dbapi_connection.autocommit = False

        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(
                "SET ISOLATION TO %s" % isolation_clause
            )
        except Exception:
            # Do not publish an isolation level that the server rejected. If
            # this call was leaving AUTOCOMMIT, restore the original DBAPI
            # mode so the failed operation has no hidden side effect.
            if was_autocommit:
                dbapi_connection.autocommit = True
            raise
        finally:
            cursor.close()

        self._remember_isolation_level(
            dbapi_connection,
            normalized_level,
        )

    def do_close(self, dbapi_connection):
        self._forget_isolation_level(dbapi_connection)
        super().do_close(dbapi_connection)

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
        """Check the connection without leaving an Informix transaction open.

        The Informix ODBC driver starts a transaction for the SELECT used by
        pool pre-ping when DBAPI autocommit is disabled.  SQLAlchemy performs
        pre-ping before it creates the public ``Connection`` transaction
        state, so an unclosed driver transaction is invisible to SQLAlchemy.
        A subsequent ``SQL_ATTR_TXN_ISOLATION`` change then fails with
        ``HY011 / -11119`` (attribute cannot be set now).

        In manual-commit mode, attempt a rollback after every ping attempt,
        including a failed SELECT.  A non-disconnect DBAPI error can leave the
        physical connection open with a transaction started or contaminated.
        Cursor-close and rollback failures must never replace the original
        ping exception; they are propagated only when the ping itself
        succeeded.
        """

        cursor = None
        ping_failed = False
        manual_commit = not getattr(
            dbapi_connection,
            "autocommit",
            False,
        )

        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(
                "SELECT FIRST 1 tabname "
                "FROM systables "
                "ORDER BY tabname"
            )
            cursor.fetchone()
        except BaseException:
            ping_failed = True
            raise
        finally:
            cleanup_error = None

            if cursor is not None:
                try:
                    cursor.close()
                except BaseException as error:
                    cleanup_error = error

            if manual_commit:
                try:
                    dbapi_connection.rollback()
                except BaseException as error:
                    if cleanup_error is None:
                        cleanup_error = error

            if not ping_failed and cleanup_error is not None:
                raise cleanup_error

        return True

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
