# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2008-2016 IBM Corporation
# Copyright (c) 2026 Angel Montilla
#
# Originally derived from IfxAlchemy / OpenInformix.
# Modified by Angel Montilla to adapt IfxAlchemy to SQLAlchemy 2.0.
#
# Original authors: Sathyanesh Krishnan, Shilpa S Jadhav
# Additional authors: Alex Pitigoi, Abhigyan Agrawal, Rahul Priyadarshi
# Contributors: Jaimy Azle, Mike Bayer, Angel Montilla
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
"""Support for Informix database

"""
import datetime
import re
from sqlalchemy import event, table
from sqlalchemy import exc
from sqlalchemy import schema as sa_schema
from sqlalchemy import sql
from sqlalchemy import types as sa_types
from sqlalchemy import util
from sqlalchemy.sql import compiler
from sqlalchemy.sql import operators
from sqlalchemy.sql import selectable
from sqlalchemy.sql import elements as sql_elements
from sqlalchemy.sql import functions as sql_functions
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors as sql_visitors
from sqlalchemy.engine import default

from . import reflection as ifx_reflection
from . import sqla_compat

from sqlalchemy.types import BLOB, CHAR, CLOB, DATE, DATETIME, INTEGER, \
    SMALLINT, BIGINT, DECIMAL, NUMERIC, REAL, TIME, TIMESTAMP, \
    VARCHAR, FLOAT

_IFX_SINGLE_ROW_FROM = " FROM sysmaster:informix.sysdual"
_IFX_LASTROWID_DBINFO_BY_TYPE = {
    "BIGSERIAL": "bigserial",
    "SERIAL8": "serial8",
}
_IFX_UNIQUE_CONSTRAINT_AS_INDEX = "informix_unique_constraint_as_index"


def _is_ifx_simple_self_referential_fk(constraint):
    """Return whether *constraint* is a simple one-column self reference."""
    if not isinstance(constraint, sa_schema.ForeignKeyConstraint):
        return False

    if len(constraint.elements) != 1:
        return False

    element = constraint.elements[0]
    try:
        referred_column = element.column
    except (
        exc.NoReferencedTableError,
        exc.NoReferencedColumnError,
    ):
        return False

    return (
        element.parent.table is constraint.table
        and referred_column.table is constraint.table
    )


def _create_ifx_deferred_self_referential_fks(
    table,
    connection,
    **kw,
):
    """Create simple self-referential FKs after the Informix table exists.

    Informix can reject the inline form used during ``CREATE TABLE`` for the
    SQLAlchemy reflection fixture.  Explicit ``use_alter=True`` constraints
    are already handled by SQLAlchemy and are therefore skipped here.
    """
    _ = kw

    if getattr(connection.dialect, "name", None) != "informix":
        return

    for constraint in table.foreign_key_constraints:
        if constraint.use_alter:
            continue

        if _is_ifx_simple_self_referential_fk(constraint):
            connection.execute(sa_schema.AddConstraint(constraint))


event.listen(
    sa_schema.Table,
    "after_create",
    _create_ifx_deferred_self_referential_fks,
    propagate=True,
)

# as documented from:
RESERVED_WORDS = {
    'activate', 'disallow', 'locale', 'result', 'add', 'disconnect', 'localtime',
    'result_set_locator', 'after', 'distinct', 'localtimestamp', 'return', 'alias',
    'do', 'locator', 'returns', 'all', 'double', 'locators', 'revoke', 'allocate', 'drop',
    'lock', 'right', 'allow', 'dssize', 'lockmax', 'rollback', 'alter', 'dynamic',
    'locksize', 'routine', 'and', 'each', 'long', 'row', 'any', 'editproc', 'loop',
    'row_number', 'as', 'else', 'maintained', 'rownumber', 'asensitive', 'elseif',
    'materialized', 'rows', 'associate', 'enable', 'maxvalue', 'rowset', 'asutime',
    'encoding', 'microsecond', 'rrn', 'at', 'encryption', 'microseconds', 'run',
    'attributes', 'end', 'minute', 'savepoint', 'audit', 'end-exec', 'minutes', 'schema',
    'authorization', 'ending', 'minvalue', 'scratchpad', 'aux', 'erase', 'mode', 'scroll',
    'auxiliary', 'escape', 'modifies', 'search', 'before', 'every', 'month', 'second',
    'begin', 'except', 'months', 'seconds', 'between', 'exception', 'new', 'secqty',
    'binary', 'excluding', 'new_table', 'security', 'bufferpool', 'exclusive',
    'nextval', 'select', 'by', 'execute', 'no', 'sensitive', 'cache', 'exists', 'nocache',
    'sequence', 'call', 'exit', 'nocycle', 'session', 'called', 'explain', 'nodename',
    'session_user', 'capture', 'external', 'nodenumber', 'set', 'cardinality',
    'extract', 'nomaxvalue', 'signal', 'cascaded', 'fenced', 'nominvalue', 'simple',
    'case', 'fetch', 'none', 'some', 'cast', 'fieldproc', 'noorder', 'source', 'ccsid',
    'file', 'normalized', 'specific', 'char', 'final', 'not', 'sql', 'character', 'for',
    'null', 'sqlid', 'check', 'foreign', 'nulls', 'stacked', 'close', 'free', 'numparts',
    'standard', 'cluster', 'from', 'obid', 'start', 'collection', 'full', 'of', 'starting',
    'collid', 'function', 'old', 'statement', 'column', 'general', 'old_table', 'static',
    'comment', 'generated', 'on', 'stay', 'commit', 'get', 'open', 'stogroup', 'concat',
    'global', 'optimization', 'stores', 'condition', 'go', 'optimize', 'style', 'connect',
    'goto', 'option', 'substring', 'connection', 'grant', 'or', 'summary', 'constraint',
    'graphic', 'order', 'synonym', 'contains', 'group', 'out', 'sysfun', 'continue',
    'handler', 'outer', 'sysibm', 'count', 'hash', 'over', 'sysproc', 'count_big',
    'hashed_value', 'overriding', 'system', 'create', 'having', 'package',
    'system_user', 'cross', 'hint', 'padded', 'table', 'current', 'hold', 'pagesize',
    'tablespace', 'current_date', 'hour', 'parameter', 'then', 'current_lc_ctype',
    'hours', 'part', 'time', 'current_path', 'identity', 'partition', 'timestamp',
    'current_schema', 'if', 'partitioned', 'to', 'current_server', 'immediate',
    'partitioning', 'transaction', 'current_time', 'in', 'partitions', 'trigger',
    'current_timestamp', 'including', 'password', 'trim', 'current_timezone',
    'inclusive', 'path', 'type', 'current_user', 'increment', 'piecesize', 'undo',
    'cursor', 'index', 'plan', 'union', 'cycle', 'indicator', 'position', 'unique', 'data',
    'inherit', 'precision', 'until', 'database', 'inner', 'prepare', 'update',
    'datapartitionname', 'inout', 'prevval', 'usage', 'datapartitionnum',
    'insensitive', 'primary', 'user', 'date', 'insert', 'priqty', 'using', 'day',
    'integrity', 'privileges', 'validproc', 'days', 'intersect', 'procedure', 'value',
    'into', 'program', 'values', 'is', 'psid', 'variable',
    'isobid', 'query', 'variant', 'dbinfo', 'isolation', 'queryno', 'vcat',
    'dbpartitionname', 'iterate', 'range', 'version', 'dbpartitionnum', 'jar', 'rank',
    'view', 'deallocate', 'java', 'read', 'volatile', 'declare', 'join', 'reads', 'volumes',
    'default', 'key', 'recovery', 'when', 'defaults', 'label', 'references', 'whenever',
    'definition', 'language', 'referencing', 'where', 'delete', 'lateral', 'refresh',
    'while', 'dense_rank', 'lc_ctype', 'release', 'with', 'denserank', 'leave', 'rename',
    'without', 'describe', 'left', 'repeat', 'wlm', 'descriptor', 'like', 'reset', 'write',
    'deterministic', 'linktype', 'resignal', 'xmlelement', 'diagnostics', 'local',
    'restart', 'year', 'disable', 'localdate', 'restrict', 'years', '', 'abs', 'grouping',
    'regr_intercept', 'are', 'int', 'regr_r2', 'array', 'integer', 'regr_slope',
    'asymmetric', 'intersection', 'regr_sxx', 'atomic', 'interval', 'regr_sxy', 'avg',
    'large', 'regr_syy', 'bigint', 'leading', 'rollup', 'blob', 'ln', 'scope', 'boolean',
    'lower', 'similar', 'both', 'match', 'smallint', 'ceil', 'max', 'specifictype',
    'ceiling', 'member', 'sqlexception', 'char_length', 'merge', 'sqlstate',
    'character_length', 'method', 'sqlwarning', 'clob', 'min', 'sqrt', 'coalesce', 'mod',
    'stddev_pop', 'collate', 'module', 'stddev_samp', 'collect', 'multiset',
    'submultiset', 'convert', 'national', 'sum', 'corr', 'natural', 'symmetric',
    'corresponding', 'nchar', 'tablesample', 'covar_pop', 'nclob', 'timezone_hour',
    'covar_samp', 'normalize', 'timezone_minute', 'cube', 'nullif', 'trailing',
    'cume_dist', 'numeric', 'translate', 'current_default_transform_group',
    'octet_length', 'translation', 'current_role', 'only', 'treat',
    'current_transform_group_for_type', 'overlaps', 'true', 'dec', 'overlay',
    'uescape', 'decimal', 'percent_rank', 'unknown', 'deref', 'percentile_cont',
    'unnest', 'element', 'percentile_disc', 'upper', 'exec', 'power', 'var_pop', 'exp',
    'real', 'var_samp', 'false', 'recursive', 'varchar', 'filter', 'ref', 'varying',
    'float', 'regr_avgx', 'width_bucket', 'floor', 'regr_avgy', 'window', 'fusion',
    'regr_count', 'within', 'asc'}


class BOOLEAN(sa_types.Boolean):
    """Native Informix BOOLEAN type.

    Informix exposes BOOLEAN as a built-in opaque type whose documented
    external values are ``t``, ``f`` and ``NULL``. The result processor also
    accepts the common DBAPI representations used for SQL bit values so the
    Python-facing value is consistently ``bool`` or ``None``.

    SQLAlchemy's generic :class:`~sqlalchemy.types.Boolean` is adapted to this
    class through ``colspecs`` so application code can continue to use either
    ``Boolean`` or the dialect-specific ``BOOLEAN`` type.
    """

    cache_ok = True

    _TRUE_TEXT_VALUES = frozenset({"1", "t", "true"})
    _FALSE_TEXT_VALUES = frozenset({"0", "f", "false"})

    def bind_processor(self, dialect):
        strict_as_bool = self._strict_as_bool

        def process(value):
            value = strict_as_bool(value)
            if value is None:
                return None
            return "t" if value else "f"

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None or isinstance(value, bool):
                return value

            if isinstance(value, memoryview):
                value = value.tobytes()
            elif isinstance(value, bytearray):
                value = bytes(value)

            if isinstance(value, bytes):
                if value == b"\x01":
                    return True
                if value == b"\x00":
                    return False
                value = value.strip(b"\x00 \t\r\n")
                try:
                    value = value.decode("ascii")
                except UnicodeDecodeError as exc:
                    raise ValueError(
                        "Informix BOOLEAN returned non-ASCII bytes: "
                        f"{value!r}"
                    ) from exc

            if isinstance(value, int):
                if value in (0, 1):
                    return bool(value)
                raise ValueError(
                    "Informix BOOLEAN returned an integer other than 0 or 1: "
                    f"{value!r}"
                )

            if isinstance(value, str):
                normalized = value.strip().casefold()
                if normalized in self._TRUE_TEXT_VALUES:
                    return True
                if normalized in self._FALSE_TEXT_VALUES:
                    return False

            raise ValueError(
                "Informix BOOLEAN returned an unsupported value: "
                f"type={type(value).__name__}, value={value!r}"
            )

        return process


# Backwards-compatible private name retained for code that imported it from
# older IfxAlchemy releases.
_IFXBoolean = BOOLEAN


class _IFXTime(sa_types.Time):
    cache_ok = True

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None

            if isinstance(value, datetime.datetime):
                return value

            if isinstance(value, datetime.time):
                return datetime.datetime.combine(
                    datetime.date(1900, 1, 1),
                    value.replace(tzinfo=None),
                )

            raise TypeError(
                "Informix TIME expects datetime.time or datetime.datetime, "
                f"not {type(value).__name__}"
            )

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None

            if isinstance(value, datetime.time):
                return value.replace(tzinfo=None)

            if isinstance(value, datetime.datetime):
                return value.time().replace(tzinfo=None)

            return datetime.time.fromisoformat(str(value).strip())

        return process


class _IFXNumeric(sa_types.Numeric):
    cache_ok = True

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            return super().result_processor(dialect, coltype)

        def process(value):
            return None if value is None else float(value)

        return process


class _IFXFloat(sa_types.Float):
    cache_ok = True

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            return super().result_processor(dialect, coltype)

        def process(value):
            return None if value is None else float(value)

        return process


class _IFXDate(sa_types.Date):

    cache_ok = True

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None

            # Some drivers may return a datetime value for a DATE column.
            if isinstance(value, datetime.datetime):
                return value.date()

            # pyodbc usually returns a `datetime.date`.
            if isinstance(value, datetime.date):
                return value

            raise TypeError(
                "Informix DATE returned an incompatible value: "
                f"{value!r} ({type(value).__name__})"
            )

        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None

            # DATE should not store the time.
            if isinstance(value, datetime.datetime):
                return value.date()

            if isinstance(value, datetime.date):
                return value

            raise TypeError(
                "Informix DATE columns require datetime.date or "
                f"datetime.datetime; received {type(value).__name__}"
            )

        return process


class DOUBLE(sa_types.Numeric):
    __visit_name__ = 'DOUBLE'


class LONGVARCHAR(sa_types.VARCHAR):
    __visit_name__ = 'LONGVARCHAR'


class LVARCHAR(sa_types.String):
    """Native Informix LVARCHAR opaque character type.

    Informix stores up to 32,739 bytes and applies a 2,048-byte maximum when
    no length is declared.  A missing ``length`` is intentionally preserved
    so DDL renders the native shorthand ``LVARCHAR`` and the server applies
    that default.
    """

    __visit_name__ = "LVARCHAR"
    cache_ok = True


class DBCLOB(sa_types.CLOB):
    __visit_name__ = "DBCLOB"


class GRAPHIC(sa_types.CHAR):
    __visit_name__ = "GRAPHIC"


class VARGRAPHIC(sa_types.Unicode):
    __visit_name__ = "VARGRAPHIC"


class LONGVARGRAPHIC(sa_types.UnicodeText):
    __visit_name__ = "LONGVARGRAPHIC"


class XML(sa_types.Text):
    __visit_name__ = "XML"


class SERIAL(sa_types.INTEGER):
    __visit_name__ = "SERIAL"


class SERIAL8(sa_types.BIGINT):
    __visit_name__ = "SERIAL8"


class BIGSERIAL(sa_types.BIGINT):
    __visit_name__ = "BIGSERIAL"


def _ifx_type_visit_name(type_):
    if type_ is None:
        return None
    if isinstance(type_, type):
        return getattr(type_, "__visit_name__", None)
    return getattr(type_, "__visit_name__", None)


def _is_ifx_serial_type(type_):
    return _ifx_type_visit_name(type_) in {"SERIAL", "SERIAL8", "BIGSERIAL"}


def _get_ifx_autoincrement_type_name(column):
    type_name = _ifx_type_visit_name(column.type)

    if type_name in {"SERIAL", "SERIAL8", "BIGSERIAL"}:
        return type_name

    table = getattr(column, "table", None)
    autoincrement_column = sqla_compat.get_table_autoincrement_column(table)
    if autoincrement_column is not column:
        return type_name

    if isinstance(column.type, sa_types.BigInteger):
        return "SERIAL8"
    if isinstance(column.type, sa_types.Integer):
        return "SERIAL"

    return type_name


def _get_ifx_lastrowid_query(column):
    type_name = _get_ifx_autoincrement_type_name(column)

    if type_name == "BIGSERIAL":
        expr = "CAST(DBINFO('bigserial') AS DECIMAL(20,0))"
    elif type_name == "SERIAL8":
        expr = "DBINFO('serial8')"
    else:
        expr = "DBINFO('sqlca.sqlerrd1')"

    return "SELECT %s%s" % (expr, _IFX_SINGLE_ROW_FROM)


_IFX_ARITHMETIC_DEFAULT_OPERATORS = frozenset(
    {
        operators.add,
        operators.sub,
        operators.mul,
        operators.truediv,
        operators.floordiv,
        operators.mod,
    }
)

_IFX_SIGNED_NUMERIC_DEFAULT_RE = re.compile(
    r"^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE][+-]?\d+)?$"
)

_IFX_QUOTED_STRING_DEFAULT_RE = re.compile(
    r"^'(?:''|[^'])*'$",
    re.DOTALL,
)

_IFX_ISO_TEMPORAL_DEFAULT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"
    r"(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?$"
)


def _strip_ifx_outer_parentheses(value):
    """Remove balanced parentheses enclosing the complete default."""

    text = str(value).strip()

    while len(text) >= 2 and text[0] == "(" and text[-1] == ")":
        depth = 0
        in_string = False
        encloses_whole_value = True
        index = 0

        while index < len(text):
            character = text[index]

            if in_string:
                if character == "'":
                    if index + 1 < len(text) and text[index + 1] == "'":
                        index += 2
                        continue

                    in_string = False

                index += 1
                continue

            if character == "'":
                in_string = True

            elif character == "(":
                depth += 1

            elif character == ")":
                depth -= 1

                if depth == 0 and index != len(text) - 1:
                    encloses_whole_value = False
                    break

            index += 1

        if (
            not encloses_whole_value
            or depth != 0
            or in_string
        ):
            break

        text = text[1:-1].strip()

    return text


def _is_ifx_scalar_default_literal(text):
    """Return whether *text* is a supported non-arithmetic literal."""
    return any(
        pattern.fullmatch(text)
        for pattern in (
            _IFX_SIGNED_NUMERIC_DEFAULT_RE,
            _IFX_QUOTED_STRING_DEFAULT_RE,
            _IFX_ISO_TEMPORAL_DEFAULT_RE,
        )
    )


def _mask_ifx_quoted_literals(text):
    """Replace quoted SQL literal contents with spaces.

    Keeping the original length allows operator positions to remain stable.
    Doubled single quotes are treated as escaped quotes inside a literal.
    """
    characters = list(text)
    in_string = False
    index = 0

    while index < len(characters):
        character = characters[index]

        if character != "'":
            if in_string:
                characters[index] = " "
            index += 1
            continue

        characters[index] = " "

        if in_string and index + 1 < len(characters):
            if characters[index + 1] == "'":
                characters[index + 1] = " "
                index += 2
                continue

        in_string = not in_string
        index += 1

    return "".join(characters)


def _nearest_ifx_non_space_character(text, start, step):
    """Return the nearest non-space character or ``None``."""
    index = start

    while 0 <= index < len(text) and text[index].isspace():
        index += step

    if 0 <= index < len(text):
        return text[index]

    return None


def _is_ifx_non_binary_sign(text, index):
    """Return whether ``+`` or ``-`` is unary or part of an exponent."""
    previous_character = _nearest_ifx_non_space_character(
        text,
        index - 1,
        -1,
    )
    next_character = _nearest_ifx_non_space_character(
        text,
        index + 1,
        1,
    )

    is_unary_sign = (
        previous_character is None
        or previous_character in "(,"
    )
    is_exponent_sign = (
        previous_character in {"e", "E"}
        and next_character is not None
        and next_character.isdigit()
    )

    return is_unary_sign or is_exponent_sign


def _contains_ifx_binary_arithmetic_operator(text):
    """Return whether unquoted SQL contains a binary arithmetic operator."""
    if any(operator in text for operator in "*/%"):
        return True

    for index, character in enumerate(text):
        if (
            character in "+-"
            and not _is_ifx_non_binary_sign(text, index)
        ):
            return True

    return False


def _contains_ifx_arithmetic_default(default_sql):
    """Return True for arithmetic operators outside quoted SQL literals."""
    text = _strip_ifx_outer_parentheses(str(default_sql))

    if not text or _is_ifx_scalar_default_literal(text):
        return False

    unquoted_text = _mask_ifx_quoted_literals(text)
    return _contains_ifx_binary_arithmetic_operator(unquoted_text)


def _validate_ifx_server_default(column, default_sql):
    """Reject arithmetic server defaults unsupported by Informix."""

    server_default = getattr(column, "server_default", None)
    default_arg = getattr(server_default, "arg", None)

    is_structured_arithmetic_expression = (
        isinstance(default_arg, sql.elements.BinaryExpression)
        and default_arg.operator
        in _IFX_ARITHMETIC_DEFAULT_OPERATORS
    )

    is_textual_arithmetic_expression = (
        _contains_ifx_arithmetic_default(default_sql)
    )

    if (
        is_structured_arithmetic_expression
        or is_textual_arithmetic_expression
    ):
        raise exc.CompileError(
            "Informix does not support arithmetic server-default "
            f"expressions for column {column.name!r}: "
            f"{default_sql!r}. "
            "Use a literal value or an Informix native default "
            "such as CURRENT or TODAY."
        )


_IFX_UNENCODED_DEFAULT_SQL_KEYWORDS = frozenset(
    {
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DBSERVERNAME",
        "FALSE",
        "NULL",
        "SYSDATE",
        "TODAY",
        "TRUE",
        "USER",
    }
)


_IFX_DEFAULT_ENCODING_CHARACTERS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/"
)


_IFX_NUMERIC_DEFAULT_RE = re.compile(
    r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"
)


def _is_ifx_numeric_type(type_):
    """Return whether *type_* has a numeric SQLAlchemy affinity."""
    affinity = getattr(type_, "_type_affinity", type(type_))

    try:
        return issubclass(
            affinity,
            (
                sa_types.Integer,
                sa_types.Numeric,
                sa_types.Float,
            ),
        )
    except TypeError:
        return isinstance(
            type_,
            (
                sa_types.Integer,
                sa_types.Numeric,
                sa_types.Float,
            ),
        )


def _is_ifx_current_datetime_default(default_arg):
    """Recognize SQLAlchemy's generic current-timestamp functions."""
    function_name = getattr(default_arg, "name", None)

    return (
        isinstance(function_name, str)
        and function_name.lower() in {"now", "current_timestamp"}
    )


def _ifx_current_default_for_column(type_compiler, column):
    """Render CURRENT with the DATETIME qualifier required by Informix."""
    rendered_type = type_compiler.process(
        column.type,
        type_expression=column,
    )

    normalized_type = " ".join(rendered_type.upper().split())

    if normalized_type.startswith("DATETIME "):
        qualifier = normalized_type[len("DATETIME "):]
        return f"CURRENT {qualifier}"

    return "CURRENT"


def _normalize_ifx_boolean_default(value):
    """Return SQLAlchemy's canonical spelling for an Informix BOOLEAN default."""
    candidate = _strip_ifx_outer_parentheses(value)

    if (
        len(candidate) >= 2
        and candidate[0] == candidate[-1]
        and candidate[0] in {"'", '"'}
    ):
        candidate = candidate[1:-1].strip()

    normalized = candidate.casefold()

    if normalized in {"t", "true", "1"}:
        return "true"

    if normalized in {"f", "false", "0"}:
        return "false"

    return None


def _normalize_ifx_reflected_default(raw_default, reflected_type):
    """Normalize a DEFAULT value obtained from Informix system catalogs.

    For most non-character types, Informix stores literal defaults in
    ``sysdefaults.default`` using this format::

        <encoded-6-bit-value> <SQL-literal>

    Examples::

        gAAAAAAAAAAA 0.000
        AAAAAA 0
        AACOrQ 2000-01-01

    Character-family defaults contain only their textual representation
    and must not have their first token removed.

    BOOLEAN defaults are normalized to SQLAlchemy's canonical ``true`` or
    ``false`` spelling.

    SQL expressions such as ``CURRENT YEAR TO SECOND`` must also remain
    unchanged.
    """
    if raw_default is None:
        return None

    value = str(raw_default).strip()

    if not value:
        return value

    # Informix stores native BOOLEAN defaults using values such as t/f,
    # quoted t/f, true/false, or 1/0. SQLAlchemy's reflection contract
    # expects the canonical true/false spelling.
    if isinstance(reflected_type, sa_types.Boolean):
        normalized_boolean = _normalize_ifx_boolean_default(value)

        if normalized_boolean is not None:
            return normalized_boolean

        # Preserve unknown BOOLEAN expressions rather than corrupting them.
        return value

    # Character defaults are stored directly as their textual value.
    # They do not contain Informix's encoded catalog prefix.
    if isinstance(reflected_type, sa_types.String):
        return value

    # Informix names the current DATETIME expression CURRENT or SYSDATE,
    # while SQLAlchemy's generic server-default contract uses the canonical
    # CURRENT_TIMESTAMP spelling. Canonicalizing here makes reflected metadata
    # stable and prevents false Alembic differences.
    if isinstance(reflected_type, sa_types.DateTime):
        normalized_expression = " ".join(value.upper().split())

        if normalized_expression in {"CURRENT", "SYSDATE"}:
            return "CURRENT_TIMESTAMP"

    encoded_prefix, separator, sql_literal = value.partition(" ")

    # A value without a separator cannot contain both the internal
    # representation and the textual SQL literal.
    if not separator:
        return value

    # Do not damage native SQL default expressions, for example:
    #
    #     CURRENT YEAR TO SECOND
    #
    # The first token of these expressions is not an encoded value.
    if encoded_prefix.upper() in _IFX_UNENCODED_DEFAULT_SQL_KEYWORDS:
        return value

    # Informix's internal representation uses a base64-like alphabet.
    # Requiring at least six characters avoids interpreting ordinary
    # short SQL tokens as encoded catalog data.
    is_encoded_prefix = (
        len(encoded_prefix) >= 6
        and all(
            character in _IFX_DEFAULT_ENCODING_CHARACTERS
            for character in encoded_prefix
        )
    )

    if not is_encoded_prefix:
        return value

    normalized = sql_literal.strip()

    # Defensive behavior: never replace a non-empty catalog value with
    # an empty result merely because it happened to contain a space.
    return normalized if normalized else value


def _normalize_ifx_reflected_column(column):
    """Return an independent reflected-column dictionary with a clean default.

    A copy is created because reflection results can be cached by SQLAlchemy
    or by the Informix reflector. Mutating the original dictionary could
    contaminate subsequent reflection calls.
    """
    normalized_column = dict(column)

    normalized_column["default"] = _normalize_ifx_reflected_default(
        normalized_column.get("default"),
        normalized_column.get("type"),
    )

    return normalized_column


colspecs = {
    sa_types.Boolean: BOOLEAN,
    sa_types.Date: _IFXDate,
    sa_types.Time: _IFXTime,
    sa_types.Numeric: _IFXNumeric,
    sa_types.DECIMAL: _IFXNumeric,
    sa_types.Float: _IFXFloat
}

ischema_names = {
    'BLOB': BLOB,
    'CHAR': CHAR,
    'CHARACTER': CHAR,
    'CLOB': CLOB,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'SERIAL': SERIAL,
    'SERIAL8': SERIAL8,
    'BIGSERIAL': BIGSERIAL,
    'INTEGER': INTEGER,
    'SMALLINT': SMALLINT,
    'BIGINT': BIGINT,
    'DECIMAL': DECIMAL,
    'NUMERIC': NUMERIC,
    'REAL': REAL,
    'DOUBLE': DOUBLE,
    'FLOAT': FLOAT,
    'TIME': TIME,
    'TIMESTAMP': TIMESTAMP,
    'VARCHAR': VARCHAR,
    'NCHAR': sa_types.NCHAR,
    'NVARCHAR': sa_types.NVARCHAR,
    'LONGVARCHAR': LONGVARCHAR,
    'XML': XML,
    'GRAPHIC': GRAPHIC,
    'VARGRAPHIC': VARGRAPHIC,
    'LONGVARGRAPHIC': LONGVARGRAPHIC,
    'DBCLOB': DBCLOB,
    'BOOLEAN': BOOLEAN,
    'BYTE': sa_types.LargeBinary,
    'TEXT': sa_types.Text,
    'LVARCHAR': LVARCHAR,
}


_IFX_TYPE_VISITOR_ALIASES = {
    "visit_TIMESTAMP": "visit_timestamp",
    "visit_DATE": "visit_date",
    "visit_TIME": "visit_time",
    "visit_DATETIME": "visit_datetime",
    "visit_SMALLINT": "visit_smallint",
    "visit_INT": "visit_int",
    "visit_BIGINT": "visit_bigint",
    "visit_SERIAL": "visit_serial",
    "visit_SERIAL8": "visit_serial8",
    "visit_BIGSERIAL": "visit_bigserial",
    "visit_FLOAT": "visit_float",
    "visit_DOUBLE": "visit_double",
    "visit_XML": "visit_xml",
    "visit_CLOB": "visit_clob",
    "visit_BLOB": "visit_blob",
    "visit_DBCLOB": "visit_dbclob",
    "visit_VARCHAR": "visit_varchar",
    "visit_LONGVARCHAR": "visit_longvarchar",
    "visit_VARGRAPHIC": "visit_vargraphic",
    "visit_LONGVARGRAPHIC": "visit_longvargraphic",
    "visit_CHAR": "visit_char",
    "visit_GRAPHIC": "visit_graphic",
    "visit_DECIMAL": "visit_decimal",
    "visit_TEXT": "visit_text",
}


class IfxTypeCompiler(compiler.GenericTypeCompiler):

    def __getattr__(self, name):
        alias = _IFX_TYPE_VISITOR_ALIASES.get(name)
        if alias is not None:
            return getattr(self, alias)
        raise AttributeError(name)

    def visit_timestamp(self, type_):
        return "TIMESTAMP"

    def visit_date(self, type_):
        return "DATE"

    def visit_time(self, type_, **kw):
        fraction_digits = getattr(
            type_,
            "fraction_digits",
            5,
        )

        if fraction_digits == 0:
            return "DATETIME HOUR TO SECOND"

        return (
            "DATETIME HOUR TO "
            f"FRACTION({fraction_digits})"
        )

    def visit_datetime(self, type_, **kw):
        fraction_digits = getattr(
            type_,
            "fraction_digits",
            5,
        )

        if fraction_digits == 0:
            return "DATETIME YEAR TO SECOND"

        return (
            "DATETIME YEAR TO "
            f"FRACTION({fraction_digits})"
        )

    def visit_smallint(self, type_):
        return "SMALLINT"

    def visit_int(self, type_):
        return "INTEGER"

    def visit_bigint(self, type_):
        return "BIGINT"

    def visit_serial(self, type_):
        return "SERIAL"

    def visit_serial8(self, type_):
        return "SERIAL8"

    def visit_bigserial(self, type_):
        return "BIGSERIAL"

    def visit_float(self, type_):
        return "FLOAT" if type_.precision is None else \
                "FLOAT(%(precision)s)" % {'precision': type_.precision}

    def visit_double(self, type_):
        return "DOUBLE"

    def visit_xml(self, type_):
        return "XML"

    def visit_clob(self, type_):
        return "CLOB"

    def visit_blob(self, type_):
        # Informix accepts BLOB in DDL, while the legacy BLOB(1M) form
        # raises -201 on the target server used by the SQLAlchemy suite.
        return "BLOB"

    def visit_dbclob(self, type_):
        return "DBCLOB"

    def _require_length(self, type_, type_name):
        if type_.length in (None, 0):
            raise exc.CompileError(
                "Informix %s requires an explicit length" % type_name
            )
        return type_.length

    def visit_varchar(self, type_):
        length = self._require_length(type_, "VARCHAR")
        return "VARCHAR(%(length)s)" % {"length": length}

    def visit_longvarchar(self, type_):
        return "LONG VARCHAR"

    def visit_LVARCHAR(self, type_, **kw):
        _ = kw
        length = type_.length

        if length is None:
            return "LVARCHAR"

        if isinstance(length, bool) or not isinstance(length, int):
            raise exc.CompileError(
                "Informix LVARCHAR length must be an integer number of bytes"
            )

        if not 1 <= length <= 32739:
            raise exc.CompileError(
                "Informix LVARCHAR length must be between 1 and 32739 bytes"
            )

        return f"LVARCHAR({length})"

    def visit_vargraphic(self, type_):
        length = self._require_length(type_, "VARGRAPHIC")
        return "VARGRAPHIC(%(length)s)" % {"length": length}

    def visit_longvargraphic(self, type_):
        return "LONG VARGRAPHIC"

    def visit_char(self, type_):
        return "CHAR" if type_.length in (None, 0) else \
                "CHAR(%(length)s)" % {'length': type_.length}

    def _render_national_character_type(
        self,
        type_name,
        type_,
        maximum_length,
    ):
        """Render an Informix locale-sensitive character type.

        Informix interprets NCHAR and NVARCHAR lengths as bytes in the
        database locale.  Keep these types distinct from CHAR/VARCHAR and
        reject lengths that the server cannot represent before emitting DDL.
        A missing length is valid Informix syntax and uses the server default
        of one byte.
        """
        length = type_.length

        if length is None:
            return type_name

        if isinstance(length, bool) or not isinstance(length, int):
            raise exc.CompileError(
                f"Informix {type_name} length must be an integer number "
                "of bytes"
            )

        if not 1 <= length <= maximum_length:
            raise exc.CompileError(
                f"Informix {type_name} length must be between 1 and "
                f"{maximum_length} bytes"
            )

        # NCHAR/NVARCHAR collation is selected by DB_LOCALE rather than by
        # degrading the type to CHAR/VARCHAR.  GenericTypeCompiler's helper
        # preserves the exact native type name and an explicitly supplied
        # SQLAlchemy collation clause.
        return self._render_string_type(
            type_name,
            length,
            type_.collation,
        )

    def visit_NCHAR(self, type_, **kw):
        _ = kw
        return self._render_national_character_type(
            "NCHAR",
            type_,
            32767,
        )

    def visit_NVARCHAR(self, type_, **kw):
        _ = kw
        return self._render_national_character_type(
            "NVARCHAR",
            type_,
            255,
        )

    def visit_graphic(self, type_):
        return "GRAPHIC" if type_.length in (None, 0) else \
                "GRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_decimal(self, type_):
        if not type_.precision:
            return "DECIMAL(31, 0)"
        elif not type_.scale:
            return "DECIMAL(%(precision)s, 0)" % {'precision': type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                            'precision': type_.precision, 'scale': type_.scale}

    def visit_numeric(self, type_):
        return self.visit_decimal(type_)

    def visit_integer(self, type_):
        return self.visit_int(type_)

    def visit_boolean(self, type_):
        return "BOOLEAN"

    def visit_unicode(self, type_):
        return self.visit_vargraphic(type_)

    def visit_unicode_text(self, type_):
        return self.visit_longvargraphic(type_)

    def visit_string(self, type_):
        return self.visit_varchar(type_)

    def visit_text(self, type_):
        return "TEXT"

    def visit_large_binary(self, type_):
        return "BYTE"


class IfxCompiler(compiler.SQLCompiler):
    ansi_bind_rules = True

    def _ifx_merge_column(self, column, **kw):
        """Render a target MERGE column with its table or alias qualifier."""

        render_kw = dict(kw)
        render_kw["include_table"] = True
        return self.process(column, **render_kw)

    def _ifx_render_merge_values_source(self, source, **kw):
        """Render a named SQLAlchemy ``Values`` as an Informix source query.

        Informix 14.10 does not accept the generic ``VALUES (...), (...)``
        table expression emitted by SQLAlchemy in the USING clause.  Reuse the
        dialect's typed single-row SELECT strategy and combine rows with
        ``UNION ALL``.  Aliases on the first branch define the derived-table
        column names used by the ON and action clauses.
        """

        if not source.named_with_column:
            raise exc.CompileError(
                "Informix MERGE requires a named Values source; pass "
                "values(..., name='source_name') or call .alias('source_name')"
            )

        if sqla_compat.values_is_lateral(source):
            raise exc.CompileError(
                "Informix MERGE does not support a LATERAL Values source"
            )

        rows = sqla_compat.get_values_rows(source)
        if not rows:
            raise exc.CompileError(
                "Informix MERGE Values source requires at least one row"
            )

        columns = tuple(source.columns)
        column_types = sqla_compat.get_values_column_types(source)
        if len(columns) != len(column_types):
            raise exc.CompileError(
                "Informix MERGE Values source column/type metadata is "
                "inconsistent"
            )

        render_kw = dict(kw)
        render_kw.setdefault(
            "literal_binds",
            sqla_compat.get_values_literal_binds(source),
        )

        branches = []
        for row_index, row in enumerate(rows):
            row = tuple(row)
            if len(row) != len(columns):
                raise exc.CompileError(
                    "Informix MERGE Values source row has "
                    f"{len(row)} values for {len(columns)} columns"
                )

            typed_row = sql.elements.Tuple(*row, types=column_types)
            rendered_expressions = []
            for expression, column in zip(typed_row.clauses, columns):
                rendered = self._ifx_render_values_cte_expression(
                    expression,
                    **render_kw,
                )
                if row_index == 0:
                    rendered += " AS " + self.preparer.quote(column.name)
                rendered_expressions.append(rendered)

            branches.append(
                "SELECT "
                + ", ".join(rendered_expressions)
                + self.default_from()
            )

        alias_name = self.preparer.quote(source.name)
        return (
            "("
            + " UNION ALL ".join(branches)
            + ")"
            + self.get_render_as_alias_suffix(alias_name)
        )

    def _ifx_render_merge_source(self, source, **kw):
        if isinstance(source, selectable.Values):
            return self._ifx_render_merge_values_source(source, **kw)

        render_kw = dict(kw)
        render_kw["asfrom"] = True
        return self.process(source, **render_kw)

    def visit_informix_merge(self, merge, **kw):
        """Compile the safe native Informix MERGE construct."""

        if (
            merge._matched_update is None
            and not merge._matched_delete
            and merge._not_matched_insert is None
        ):
            raise exc.CompileError(
                "Informix MERGE requires UPDATE, DELETE, and/or INSERT action"
            )

        target_kw = dict(kw)
        target_kw["asfrom"] = True
        target_sql = self.process(merge.target, **target_kw)
        source_sql = self._ifx_render_merge_source(merge.source, **kw)
        on_sql = self.process(merge.onclause, **kw)

        clauses = [
            f"MERGE INTO {target_sql}",
            f"USING {source_sql}",
            f"ON {on_sql}",
        ]

        if merge._matched_update is not None:
            assignments = ", ".join(
                f"{self._ifx_merge_column(column, **kw)} = "
                f"{self.process(value, **kw)}"
                for column, value in merge._matched_update
            )
            clauses.append(
                "WHEN MATCHED THEN UPDATE SET " + assignments
            )
        elif merge._matched_delete:
            clauses.append("WHEN MATCHED THEN DELETE")

        if merge._not_matched_insert is not None:
            insert_kw = dict(kw)
            insert_kw["include_table"] = False
            columns = ", ".join(
                self.process(column, **insert_kw)
                for column, _ in merge._not_matched_insert
            )
            values = ", ".join(
                self.process(value, **kw)
                for _, value in merge._not_matched_insert
            )
            clauses.append(
                "WHEN NOT MATCHED THEN INSERT ("
                + columns
                + ") VALUES ("
                + values
                + ")"
            )

        return " ".join(clauses)

    def visit_column(
        self,
        column,
        add_to_result_map=None,
        include_table=True,
        result_map_targets=(),
        ambiguous_table_name_map=None,
        **kw,
    ):
        """Render owner-qualified tables without three-part columns.

        Informix accepts ``owner.table`` in the FROM clause, but column
        references must use ``table.column`` (or an alias), not
        ``owner.table.column``.
        """
        rendered = super().visit_column(
            column,
            add_to_result_map=add_to_result_map,
            include_table=include_table,
            result_map_targets=result_map_targets,
            ambiguous_table_name_map=ambiguous_table_name_map,
            **kw,
        )

        table = column.table
        if (
            table is None
            or not include_table
            or not table.named_with_column
        ):
            return rendered

        effective_schema = self.preparer.schema_for_object(table)
        if not effective_schema:
            return rendered

        schema_prefix = (
            self.preparer.quote_schema(effective_schema) + "."
        )
        if rendered.startswith(schema_prefix):
            return rendered[len(schema_prefix):]

        return rendered

    def visit_insert(
        self,
        insert_stmt,
        visited_bindparam=None,
        visiting_cte=None,
        **kw,
    ):
        """Render an empty INSERT through an Informix SERIAL column.

        Informix does not support SQLAlchemy's generic ``DEFAULT VALUES``
        or ``() VALUES ()`` forms.  For an empty INSERT into a table whose
        autoincrement column is compiled as SERIAL/SERIAL8/BIGSERIAL, an
        explicit zero asks Informix to generate the next serial value.

        The rewrite is deliberately limited to true execution-time empty
        parameter sets and to tables that expose an autoincrement column.
        Other empty INSERT shapes keep SQLAlchemy's normal CompileError.
        """
        is_empty_parameter_set = (
            self.column_keys == []
            and not insert_stmt._values
            and not insert_stmt._multi_values
            and insert_stmt.select is None
        )

        if is_empty_parameter_set:
            autoincrement_column = insert_stmt.table._autoincrement_column

            if autoincrement_column is not None:
                insert_stmt = insert_stmt.values(
                    {autoincrement_column: 0}
                )

        return super().visit_insert(
            insert_stmt,
            visited_bindparam=visited_bindparam,
            visiting_cte=visiting_cte,
            **kw,
        )

    @staticmethod
    def _ifx_is_boolean_projection_expression(column):
        """Return whether a projected Boolean needs predicate emulation.

        Informix requires a search condition in ``CASE WHEN``.  SQLAlchemy
        Boolean values such as ``true()``, ``false()``, Boolean bind
        parameters and an existing ``CASE`` expression are already scalar
        values; wrapping them again would produce an invalid nested predicate.

        ``_is_implicitly_boolean`` identifies expressions that are actual SQL
        predicates, including comparisons, ``IN`` and Boolean clause lists.
        Only those expressions need to be converted to a native BOOLEAN
        scalar when projected in the SELECT list. ``EXISTS`` is handled by
        :meth:`visit_unary` and therefore is intentionally not wrapped here.
        """
        if not isinstance(column.type, sa_types.Boolean):
            return False

        element = (
            column.element
            if isinstance(column, sql.elements.Label)
            else column
        )

        return bool(getattr(element, "_is_implicitly_boolean", False))

    def _ifx_boolean_projection_expression(self, column):
        if not self._ifx_is_boolean_projection_expression(column):
            return column

        label_name = (
            column.name
            if isinstance(column, sql.elements.Label)
            else None
        )
        element = (
            column.element
            if isinstance(column, sql.elements.Label)
            else column
        )

        rendered = sql.type_coerce(
            sql.case(
                (element, sql.true()),
                else_=sql.false(),
            ),
            BOOLEAN(),
        )

        if label_name is not None:
            rendered = rendered.label(label_name)

        return rendered

    def _label_select_column(
        self,
        select,
        column,
        populate_result_map,
        asfrom,
        column_clause_args,
        **kw,
    ):
        column = self._ifx_boolean_projection_expression(column)
        return super()._label_select_column(
            select,
            column,
            populate_result_map,
            asfrom,
            column_clause_args,
            **kw,
        )

    @staticmethod
    def _ifx_projected_label_names(select):
        return frozenset(
            column.name
            for column in select.selected_columns
            if isinstance(column, sql.elements.Label)
            and column.name is not None
        )

    def order_by_clause(self, select, **kw):
        order_by_clauses = sqla_compat.get_order_by_clauses(select)
        if not order_by_clauses:
            return ""

        rendered = self._generate_delimited_list(
            order_by_clauses,
            compiler.OPERATORS[operators.comma_op],
            ifx_order_by_label_names=self._ifx_projected_label_names(select),
            **kw,
        )
        return " ORDER BY " + rendered if rendered else ""

    def visit_label(
        self,
        label,
        add_to_result_map=None,
        within_label_clause=False,
        within_columns_clause=False,
        render_label_as_label=None,
        result_map_targets=(),
        **kw,
    ):
        projected_names = kw.pop("ifx_order_by_label_names", frozenset())
        if (
            not within_columns_clause
            and label.name in projected_names
        ):
            return self.preparer.format_label(label)

        return super().visit_label(
            label,
            add_to_result_map=add_to_result_map,
            within_label_clause=within_label_clause,
            within_columns_clause=within_columns_clause,
            render_label_as_label=render_label_as_label,
            result_map_targets=result_map_targets,
            **kw,
        )

    def _ifx_rewrite_multitable_dml_as_exists(self, statement):
        extra_froms = sqla_compat.get_dml_extra_froms(statement)
        if not extra_froms:
            return statement

        criteria = sqla_compat.get_dml_where_criteria(statement)
        if not criteria:
            return statement

        predicate = sql.and_(*criteria)
        exists_predicate = sql.exists(
            sql.select(sql.literal_column("1"))
            .select_from(*extra_froms)
            .where(predicate)
        )

        rewritten = sqla_compat.clone_dml_without_where(statement)
        return rewritten.where(exists_predicate)

    def visit_bindparam(
        self,
        bindparam,
        within_columns_clause=False,
        literal_binds=False,
        skip_bind_expression=False,
        literal_execute=False,
        render_postcompile=False,
        is_upsert_set=False,
        **kw,
    ):
        """Protect parameterized CTEs that precede UPDATE or DELETE.

        IBM Informix ODBC/server combinations can execute a WITH + DML
        statement without reporting an error yet affect zero rows when host
        variables occur inside the CTE body.  SQLAlchemy's official CTE suite
        uses ordinary bound values in precisely that position.

        Mark only bind parameters compiled *inside* a CTE attached to an
        UPDATE or DELETE as ``literal_execute``.  SQLAlchemy renders their
        typed values immediately before execution, while SET values and
        predicates outside the CTE remain normal DBAPI parameters.  This
        keeps statement caching and user-supplied execution parameters intact.
        """
        if (
            (self.isupdate or self.isdelete)
            and kw.get("visiting_cte") is not None
            and not literal_binds
            and not literal_execute
            and not bindparam.literal_execute
        ):
            bindparam = bindparam.render_literal_execute()

        return super().visit_bindparam(
            bindparam,
            within_columns_clause=within_columns_clause,
            literal_binds=literal_binds,
            skip_bind_expression=skip_bind_expression,
            literal_execute=literal_execute,
            render_postcompile=render_postcompile,
            is_upsert_set=is_upsert_set,
            **kw,
        )

    def visit_update(self, update_stmt, visiting_cte=None, **kw):
        return super().visit_update(
            self._ifx_rewrite_multitable_dml_as_exists(update_stmt),
            visiting_cte=visiting_cte,
            **kw,
        )

    def visit_delete(self, delete_stmt, visiting_cte=None, **kw):
        return super().visit_delete(
            self._ifx_rewrite_multitable_dml_as_exists(delete_stmt),
            visiting_cte=visiting_cte,
            **kw,
        )

    def visit_select_statement_grouping(self, grouping, **kw):
        element = grouping.element
        parent = self.stack[-1].get("selectable") if self.stack else None

        if (
            isinstance(parent, selectable.CompoundSelect)
            and isinstance(element, selectable.Select)
            and (
                self._ifx_limit_fetch_clause(element) is not None
                or sqla_compat.get_offset_clause(element) is not None
                or sqla_compat.get_order_by_clauses(element)
            )
        ):
            # Informix rejects parenthesized SELECT branches in combined
            # queries.  A derived-table SELECT preserves each branch's
            # ORDER BY / FIRST / SKIP semantics without those parentheses.
            return self.process(element.alias().select(), **kw)

        return super().visit_select_statement_grouping(grouping, **kw)

    def group_by_clause(self, select, **kw):
        """Render projected labels by alias in ``GROUP BY``.

        Informix accepts a select-list alias in ``GROUP BY`` but rejects
        some composed expressions when SQLAlchemy expands the label back
        to its underlying expression.  Only labels that are actually part
        of the SELECT list are rendered by alias; non-projected labels keep
        SQLAlchemy's normal expression rendering.
        """
        selected_columns = tuple(select.selected_columns)
        rendered = []

        for clause in select._group_by_clauses:
            if (
                isinstance(clause, sql.elements.Label)
                and any(
                    selected is clause
                    for selected in selected_columns
                )
            ):
                rendered.append(
                    self.process(
                        clause,
                        render_label_as_label=clause,
                        **kw,
                    )
                )
            else:
                rendered.append(self.process(clause, **kw))

        if rendered:
            return " GROUP BY " + ", ".join(rendered)

        return ""

    def default_from(self):
        return _IFX_SINGLE_ROW_FROM

    def visit_false(self, expr, **kw):
        # Informix's native FALSE literal is the quoted external value 'f'.
        return "'f'"

    def visit_true(self, expr, **kw):
        # Informix's native TRUE literal is the quoted external value 't'.
        return "'t'"

    def visit_is_true_unary_operator(self, element, operator, **kw):
        operand = element.element
        if getattr(operand, "_is_implicitly_boolean", False):
            return self.process(operand, **kw)
        return (
            f"{self.process(operand, **kw)} = "
            f"{self.visit_true(None)}"
        )

    def visit_is_false_unary_operator(self, element, operator, **kw):
        operand = element.element
        if getattr(operand, "_is_implicitly_boolean", False):
            return f"NOT {self.process(operand, **kw)}"
        return (
            f"{self.process(operand, **kw)} = "
            f"{self.visit_false(None)}"
        )

    def get_cte_preamble(self, recursive):
        return "WITH"

    def _ifx_values_cte_cast_type(self, type_):
        """Return an Informix type that can type a SELECT-list marker.

        Informix accepts parameter markers in this context when the marker is
        the operand of a native CAST expression.  Generic SQLAlchemy String
        has no length, while Informix VARCHAR normally requires one in the
        dialect type compiler, so use native LVARCHAR for that specific case.
        """

        if isinstance(type_, sa_types.NullType):
            raise exc.CompileError(
                "Informix parameterized VALUES-backed CTE columns require "
                "an explicit SQLAlchemy type"
            )

        adapted_type = self.dialect.type_descriptor(type_)

        if (
            isinstance(adapted_type, sa_types.String)
            and not isinstance(adapted_type, sa_types.CHAR)
            and getattr(adapted_type, "length", None) in (None, 0)
        ):
            return self.dialect.type_compiler_instance.process(LVARCHAR())

        try:
            return self.dialect.type_compiler_instance.process(adapted_type)
        except exc.CompileError as error:
            raise exc.CompileError(
                "Informix cannot derive a CAST target for a parameterized "
                "VALUES-backed CTE column of type "
                f"{type_!r}"
            ) from error

    def _ifx_render_values_cte_expression(self, expression, **kw):
        """Render one typed VALUES expression for an Informix SELECT list."""

        rendered = self.process(expression, **kw)

        if (
            kw.get("literal_binds")
            or not isinstance(expression, sql.elements.BindParameter)
            or expression.literal_execute
        ):
            return rendered

        cast_type = self._ifx_values_cte_cast_type(expression.type)
        return f"CAST({rendered} AS {cast_type})"

    def _ifx_render_values_cte_row(
        self,
        row,
        column_types,
        **kw,
    ):
        """Render one ``Values`` row as a single-row Informix SELECT."""

        row = tuple(row)
        if len(row) != len(column_types):
            raise exc.CompileError(
                "Informix VALUES-backed CTE row has "
                f"{len(row)} values for {len(column_types)} columns"
            )

        typed_row = sql.elements.Tuple(
            *row,
            types=column_types,
        )
        expressions = ", ".join(
            self._ifx_render_values_cte_expression(expression, **kw)
            for expression in typed_row.clauses
        )
        return f"SELECT {expressions}{self.default_from()}"

    def _ifx_render_values_cte(self, element, **kw):
        """Emulate a VALUES-backed CTE using SELECT branches and UNION ALL."""

        rows = sqla_compat.get_values_rows(element)
        if not rows:
            raise exc.CompileError(
                "Informix VALUES-backed CTE requires at least one row"
            )

        column_types = sqla_compat.get_values_column_types(element)
        render_kw = dict(kw)
        render_kw.setdefault(
            "literal_binds",
            sqla_compat.get_values_literal_binds(element),
        )

        return " UNION ALL ".join(
            self._ifx_render_values_cte_row(
                row,
                column_types,
                **render_kw,
            )
            for row in rows
        )

    def visit_values(
        self,
        element,
        asfrom=False,
        from_linter=None,
        visiting_cte=None,
        **kw,
    ):
        """Compile a direct ``Values.cte()`` body for Informix.

        Informix does not accept SQLAlchemy's native ``VALUES (...)`` body in
        this position.  Restrict the emulation to the direct body of a CTE;
        every other ``Values`` usage keeps SQLAlchemy's standard compilation
        and remains outside the advertised capability.
        """

        if visiting_cte is not None and visiting_cte.element is element:
            if sqla_compat.get_values_independent_ctes(element):
                self._dispatch_independent_ctes(element, kw)

            if sqla_compat.values_is_lateral(element):
                raise exc.CompileError(
                    "Can't use a LATERAL VALUES expression inside of a CTE"
                )
            return self._ifx_render_values_cte(
                element,
                visiting_cte=visiting_cte,
                **kw,
            )

        return super().visit_values(
            element,
            asfrom=asfrom,
            from_linter=from_linter,
            visiting_cte=visiting_cte,
            **kw,
        )

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_grouping(self, grouping, asfrom=False, **kw):
        if isinstance(grouping, sql.selectable.ScalarSelect):
            scalar_select = grouping.element
            has_row_limit = (
                self._ifx_limit_fetch_clause(scalar_select) is not None
                or sqla_compat.get_offset_clause(scalar_select) is not None
            )

            if has_row_limit and sqla_compat.get_order_by_clauses(
                scalar_select
            ):
                ordered_rows = scalar_select.alias()
                grouping = sql.select(
                    *ordered_rows.c
                ).select_from(ordered_rows).scalar_subquery()

        return super().visit_grouping(
            grouping,
            asfrom=asfrom,
            **kw,
        )

    def for_update_clause(self, select, **kw):
        for_update = sqla_compat.get_select_for_update(select)

        if for_update is None:
            return ''

        if for_update.nowait:
            raise exc.CompileError(
                "Informix dialect does not support FOR UPDATE NOWAIT"
            )

        if for_update.skip_locked:
            raise exc.CompileError(
                "Informix dialect does not support FOR UPDATE SKIP LOCKED"
            )

        if for_update.of:
            raise exc.CompileError(
                "Informix dialect does not support FOR UPDATE OF"
            )

        if for_update.key_share:
            raise exc.CompileError(
                "Informix dialect does not support KEY SHARE locks"
            )

        if for_update.read:
            return ' WITH RS USE AND KEEP SHARE LOCKS'

        return ' WITH RS USE AND KEEP UPDATE LOCKS'

    def visit_mod_binary(self, binary, operator, **kw):
        return "mod(%s, %s)" % (self.process(binary.left),
                                self.process(binary.right))

    def _ifx_visit_bitwise_binary(
        self,
        binary,
        function_name,
        **kw,
    ):
        """Render a SQLAlchemy bitwise binary operator as an Informix function.

        Informix exposes bitwise arithmetic through named functions rather
        than the portable ``&``, ``|``, ``^``, ``<<`` and ``>>`` tokens.
        Preserve the compiler keyword arguments for aliases, literal binds,
        post-compile parameters and nested expression context.
        """
        left = self.process(binary.left, **kw)
        right = self.process(binary.right, **kw)
        return f"{function_name}({left}, {right})"

    def visit_bitwise_and_op_binary(self, binary, operator, **kw):
        return self._ifx_visit_bitwise_binary(binary, "BITAND", **kw)

    def visit_bitwise_or_op_binary(self, binary, operator, **kw):
        return self._ifx_visit_bitwise_binary(binary, "BITOR", **kw)

    def visit_bitwise_xor_op_binary(self, binary, operator, **kw):
        return self._ifx_visit_bitwise_binary(binary, "BITXOR", **kw)

    def visit_bitwise_lshift_op_binary(self, binary, operator, **kw):
        return self._ifx_visit_bitwise_binary(
            binary,
            "IFX_BIT_LEFTSHIFT",
            **kw,
        )

    def visit_bitwise_rshift_op_binary(self, binary, operator, **kw):
        return self._ifx_visit_bitwise_binary(
            binary,
            "IFX_BIT_RIGHTSHIFT",
            **kw,
        )

    def visit_bitwise_not_op_unary_operator(
        self,
        element,
        operator,
        **kw,
    ):
        operand = self.process(element.element, **kw)
        return f"BITNOT({operand})"

    def _ifx_fetch_options(self, select):
        return sqla_compat.get_fetch_clause_options(select)

    def _ifx_limit_fetch_clause(self, select):
        fetch_clause = sqla_compat.get_fetch_clause(select)
        if fetch_clause is not None:
            fetch_options = self._ifx_fetch_options(select)

            if fetch_options.get("with_ties"):
                raise exc.CompileError(
                    "Informix dialect does not support FETCH WITH TIES"
                )

            if fetch_options.get("percent"):
                raise exc.CompileError(
                    "Informix dialect does not support FETCH PERCENT"
                )

            return fetch_clause

        return sqla_compat.get_limit_clause(select)

    def _row_limit_expression(self, select, clause):
        if clause is None:
            return None

        if sqla_compat.simple_int_clause(select, clause):
            return clause.render_literal_execute()

        if isinstance(clause.type, sa_types.NullType):
            return sql.type_coerce(
                clause,
                sa_types.Integer(),
            )

        return clause

    def _ifx_native_row_count_clause(self, select, clause):
        """Return whether *clause* is safe in Informix SKIP/FIRST.

        Informix accepts integer literals and integer host variables in the
        SELECT pre-column SKIP/FIRST syntax.  SQLAlchemy represents ordinary
        ``limit(5)`` / ``offset(2)`` values as special post-compile bind
        parameters, while explicit ``bindparam()`` values remain regular host
        variables.  Arbitrary SQL expressions are intentionally excluded and
        continue through the ROW_NUMBER emulation.
        """
        if clause is None:
            return True

        if sqla_compat.simple_int_clause(select, clause):
            return True

        if not isinstance(clause, sql.elements.BindParameter):
            return False

        return isinstance(
            clause.type,
            (sa_types.Integer, sa_types.NullType),
        )

    def _ifx_uses_native_skip_first(self, select):
        limit_clause = self._ifx_limit_fetch_clause(select)
        offset_clause = sqla_compat.get_offset_clause(select)

        if limit_clause is None and offset_clause is None:
            return False

        if offset_clause is not None and sqla_compat.get_distinct(select):
            # Keep the established two-level DISTINCT rewrite.  Adding
            # ROW_NUMBER only after DISTINCT is important for both result
            # cardinality and ORDER BY adaptation.
            return False

        return (
            self._ifx_native_row_count_clause(select, limit_clause)
            and self._ifx_native_row_count_clause(select, offset_clause)
        )

    def _row_limit_upper_bound(self, select, limit_clause, offset_clause):
        limit_expression = self._row_limit_expression(select, limit_clause)
        offset_expression = self._row_limit_expression(select, offset_clause)

        if offset_expression is None:
            return limit_expression

        return limit_expression + offset_expression

    def _translate_distinct_offset_select(self, select, order_by_clauses):
        translated = (
            sqla_compat.clone_select(select)
            .limit(None)
            .offset(None)
            .order_by(None)
        )
        translated = translated.alias()

        adapter = sql_util.ClauseAdapter(translated)
        translated_order_by = [
            elem
            for elem in (
                adapter.traverse(elem) for elem in order_by_clauses
            )
            if elem is not None
        ]

        return sql.select(
            *[
                column
                for column in translated.c
                if select.selected_columns.corresponding_column(column)
                is not None
            ],
            sql.func.ROW_NUMBER()
            .over(order_by=translated_order_by or None)
            .label("ifx_rn")
        ).select_from(translated).alias()

    def _ifx_requires_row_number_rewrite(
        self,
        select,
        limit_clause,
        offset_clause,
    ):
        if limit_clause is None and offset_clause is None:
            return False

        return not self._ifx_uses_native_skip_first(select)

    def _translate_offset_select(self, select):
        limit_clause = self._ifx_limit_fetch_clause(select)
        offset_clause = sqla_compat.get_offset_clause(select)

        if not self._ifx_requires_row_number_rewrite(
            select,
            limit_clause,
            offset_clause,
        ):
            return select

        order_by_clauses = [
            sql_util.unwrap_label_reference(elem)
            for elem in sqla_compat.get_order_by_clauses(select)
        ]

        if sqla_compat.get_distinct(select):
            translated = self._translate_distinct_offset_select(
                select, order_by_clauses
            )
        else:
            translated = (
                sqla_compat.clone_select(select)
                .limit(None)
                .offset(None)
                .add_columns(
                    sql.func.ROW_NUMBER()
                    .over(order_by=order_by_clauses or None)
                    .label("ifx_rn")
                )
                .order_by(None)
                .alias()
            )

        row_number_col = translated.c.ifx_rn
        paged = (
            sql.select(
                *[column for column in translated.c if column.key != "ifx_rn"]
            )
            .select_from(translated)
            .order_by(row_number_col)
        )

        if offset_clause is not None:
            paged = paged.where(
                row_number_col
                > self._row_limit_expression(select, offset_clause)
            )

        if limit_clause is not None:
            paged = paged.where(
                row_number_col
                <= self._row_limit_upper_bound(
                    select, limit_clause, offset_clause
                )
            )

        return paged

    def limit_clause(self, select, **kwargs):
        return ""

    def fetch_clause(
        self,
        select,
        fetch_clause=None,
        require_offset=False,
        use_literal_execute_for_simple_int=False,
        **kw
    ):
        if sqla_compat.get_fetch_clause(select) is not None:
            self._ifx_limit_fetch_clause(select)

        return ""

    def translate_select_structure(self, select_stmt, **kwargs):
        return self._translate_offset_select(select_stmt)

    def visit_compound_select(
        self,
        cs,
        asfrom=False,
        compound_index=None,
        **kwargs,
    ):
        """Apply compound-query pagination through an Informix table expression.

        SQLAlchemy normally renders a compound LIMIT/OFFSET after the UNION.
        Informix places SKIP/FIRST immediately after SELECT, so a paginated
        compound is exposed as a derived table and paginated by an outer
        SELECT.  Branch-local limits continue to use the existing grouping
        rewrite and can therefore also use native FIRST/SKIP.
        """
        limit_clause = self._ifx_limit_fetch_clause(cs)
        offset_clause = sqla_compat.get_offset_clause(cs)

        if limit_clause is None and offset_clause is None:
            return super().visit_compound_select(
                cs,
                asfrom=asfrom,
                compound_index=compound_index,
                **kwargs,
            )

        order_by_clauses = list(sqla_compat.get_order_by_clauses(cs))

        inner = cs.order_by(None).limit(None).offset(None)
        inner_alias = inner.alias()
        # ORDER BY expressions can reference columns exported by the original
        # CompoundSelect. Once the compound is cloned without pagination and
        # wrapped in a new derived table, those expressions must be adapted to
        # the current outer alias.
        #
        # Name-based fallback is safe here because ORDER BY belongs to the
        # compound result and therefore resolves against the exported column
        # namespace of ``inner_alias``.
        adapter = sql_util.ClauseAdapter(
            inner_alias,
            adapt_on_names=True,
        )
        adapted_order_by = [
            adapted
            for adapted in (
                adapter.traverse(elem) for elem in order_by_clauses
            )
            if adapted is not None
        ]

        paged = sql.select(*inner_alias.c).select_from(inner_alias)
        if adapted_order_by:
            paged = paged.order_by(*adapted_order_by)

        fetch_clause = sqla_compat.get_fetch_clause(cs)
        if fetch_clause is not None:
            fetch_options = self._ifx_fetch_options(cs)
            paged = paged.fetch(
                fetch_clause,
                percent=fetch_options.get("percent", False),
                with_ties=fetch_options.get("with_ties", False),
            )
        elif limit_clause is not None:
            paged = paged.limit(limit_clause)

        if offset_clause is not None:
            paged = paged.offset(offset_clause)

        return self.process(paged, asfrom=asfrom, **kwargs)

    def visit_sequence(self, sequence, **kw):
        return "%s.NEXTVAL" % self.preparer.format_sequence(sequence)

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        if add_to_result_map is not None:
            add_to_result_map(func.name, func.name, (func.name,), func.type)

        if func.name.upper() == "AVG":
            return "AVG(DOUBLE(%s))" % (self.function_argspec(func, **kwargs))
        elif func.name.upper() == "CHAR_LENGTH":
            return "CHAR_LENGTH(%s, %s)" % (self.function_argspec(func, **kwargs), 'OCTETS')
        else:
            return compiler.SQLCompiler.visit_function(
                self,
                func,
                add_to_result_map=add_to_result_map,
                **kwargs
            )

    def visit_cast(self, cast, **kw):
        type_ = cast.typeclause.type

        if isinstance(type_, (
                    sa_types.DateTime,
                    sa_types.Date,
                    sa_types.Time,
                    sa_types.DECIMAL,
                    sa_types.Numeric,
                    sa_types.Integer,
                    sa_types.BigInteger,
                    sa_types.SmallInteger,
                    sa_types.Float,
                    sa_types.String,
                    sa_types.Text,
                    sa_types.Unicode,
                    sa_types.UnicodeText,
                    sa_types.Boolean)):
            return super(IfxCompiler, self).visit_cast(cast, **kw)
        else:
            return self.process(cast.clause, **kw)

    def get_select_precolumns(self, select, **kwargs):
        text = ""
        limit_clause = self._ifx_limit_fetch_clause(select)
        offset_clause = sqla_compat.get_offset_clause(select)
        native_skip_first = self._ifx_uses_native_skip_first(select)

        if native_skip_first and offset_clause is not None:
            offset_expression = self._row_limit_expression(
                select,
                offset_clause,
            )
            text += "SKIP %s " % self.process(
                offset_expression,
                **kwargs,
            )

        # Informix: SELECT FIRST n DISTINCT ...
        if native_skip_first and limit_clause is not None:
            limit_expression = self._row_limit_expression(select, limit_clause)
            if limit_expression is not None:
                text += "FIRST %s " % self.process(
                    limit_expression, **kwargs
                )

        # SQLAlchemy retains its official DISTINCT ON warning here.
        text += super().get_select_precolumns(select, **kwargs)
        return text

    def visit_savepoint(self, savepoint_stmt, **kw):
        # Informix uses ANSI savepoint syntax here; the DB2-specific
        # "ON ROLLBACK RETAIN CURSORS" suffix raises -201 on the target
        # backend and breaks Session.begin_nested().
        return "SAVEPOINT %(sid)s" % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)
        }

    def visit_rollback_to_savepoint(self, savepoint_stmt, **kw):
        return 'ROLLBACK TO SAVEPOINT %(sid)s' % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)
        }

    def visit_release_savepoint(self, savepoint_stmt, **kw):
        return 'RELEASE SAVEPOINT %(sid)s' % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)
        }

    def visit_unary(
        self, unary, add_to_result_map=None, result_map_targets=(), **kw
    ):
        usql = super(IfxCompiler, self).visit_unary(
            unary,
            add_to_result_map=add_to_result_map,
            result_map_targets=result_map_targets,
            **kw
        )

        if (
            unary.operator == operators.exists
            and kw.get('within_columns_clause', False)
        ):
            return (
                "CASE WHEN " + usql
                + f" THEN {self.visit_true(None)}"
                + f" ELSE {self.visit_false(None)} END"
            )

        return usql

    def visit_binary(
        self,
        binary,
        override_operator=None,
        eager_grouping=False,
        from_linter=None,
        lateral_from_linter=None,
        **kw,
    ):
        """Compile Informix-specific binary-expression semantics."""
        operator_ = override_operator or binary.operator

        if operator_ in (
            operators.in_op,
            operators.not_in_op,
        ):
            binary = self._coerce_untyped_null_in_left(binary)

        right_is_boolean_literal = isinstance(
            binary.right,
            (sql.elements.True_, sql.elements.False_),
        )
        if operator_ in (operators.is_, operators.is_not) and (
            right_is_boolean_literal
        ):
            left = self.process(binary.left, **kw)
            right = self.process(binary.right, **kw)

            if operator_ is operators.is_:
                return f"{left} = {right}"

            # Informix documents equality against 't'/'f', not ``IS TRUE``.
            # Preserve SQL ``IS NOT`` semantics by including NULL explicitly.
            return f"({left} != {right} OR {left} IS NULL)"

        return super().visit_binary(
            binary,
            override_operator=override_operator,
            eager_grouping=eager_grouping,
            from_linter=from_linter,
            lateral_from_linter=lateral_from_linter,
            **kw,
        )

    def visit_is_distinct_from_binary(self, binary, operator, **kw):
        left = self.process(binary.left, **kw)
        right = self.process(binary.right, **kw)

        return (
            "(CASE "
            f"WHEN {left} IS NULL AND {right} IS NULL THEN 0 "
            f"WHEN {left} IS NULL OR {right} IS NULL THEN 1 "
            f"WHEN {left} = {right} THEN 0 "
            "ELSE 1 END = 1)"
        )

    def visit_is_not_distinct_from_binary(self, binary, operator, **kw):
        left = self.process(binary.left, **kw)
        right = self.process(binary.right, **kw)

        return (
            "(CASE "
            f"WHEN {left} IS NULL AND {right} IS NULL THEN 1 "
            f"WHEN {left} IS NULL OR {right} IS NULL THEN 0 "
            f"WHEN {left} = {right} THEN 1 "
            "ELSE 0 END = 1)"
        )

    def visit_empty_set_expr(self, element_types, **kw):
        if len(element_types) != 1:
            raise exc.CompileError(
                "Informix dialect does not support tuple-valued empty sets"
            )

        return (
            "SELECT 1 FROM sysmaster:informix.sysdual "
            "WHERE 1 = 0"
        )

    def visit_empty_set_op_expr(self, type_, expand_op, **kw):
        """Compile scalar empty IN/NOT IN expressions without a subquery."""
        if len(type_) != 1:
            raise exc.CompileError(
                "Informix dialect does not support tuple-valued empty sets"
            )

        typed_null = self._render_empty_set_typed_null(type_[0])

        if expand_op is operators.in_op:
            return f"{typed_null}) AND (1 = 0"

        if expand_op is operators.not_in_op:
            return f"{typed_null}) OR (1 = 1"

        return super().visit_empty_set_op_expr(
            type_,
            expand_op,
            **kw,
        )

    def _render_empty_set_typed_null(self, type_):
        """Render a typed NULL for an Informix empty IN value list.

        SQLAlchemy can provide NullType when neither the left-hand expression
        nor the empty expanding parameter contains type information. SMALLINT
        is used only as a neutral fallback for that indeterminate case.
        """
        dialect_type = type_._unwrapped_dialect_impl(self.dialect)

        if isinstance(dialect_type, sa_types.NullType):
            rendered_type = "SMALLINT"
        else:
            rendered_type = self.dialect.type_compiler_instance.process(
                dialect_type
            )

        return f"CAST(NULL AS {rendered_type})"

    def _coerce_untyped_null_in_left(self, binary):
        """Type an untyped NULL used as the left operand of IN/NOT IN.

        Informix rejects:

            NULL IN (...)

        but accepts:

            CAST(NULL AS SMALLINT) IN (...)

        Whenever SQLAlchemy can infer a type from the right-hand expanding
        parameter, that type is used. SMALLINT is the fallback for a completely
        untyped empty collection.
        """
        if not isinstance(binary.left.type, sa_types.NullType):
            return binary

        right_type = binary.right.type._unwrapped_dialect_impl(
            self.dialect
        )

        if isinstance(right_type, sa_types.NullType):
            right_type = sa_types.SmallInteger()

        coerced = binary._clone()
        coerced.left = sql.cast(binary.left, right_type)

        return coerced

    def visit_in_op_binary(self, binary, operator, **kw):
        binary = self._coerce_untyped_null_in_left(binary)

        return self._generate_generic_binary(
            binary,
            compiler.OPERATORS[operator],
            **kw,
        )


class IfxDDLCompiler(compiler.DDLCompiler):

    _WRITABLE_TABLE_LOCK_LEVELS = frozenset({"PAGE", "ROW"})

    def _format_fragment_identifier(self, value):
        """Quote one validated Informix fragment/dbspace identifier."""
        return self.preparer.quote(value)

    def _fragment_expression_sql(
        self,
        value,
        subject,
        *,
        role,
        require_column=False,
    ):
        """Compile one structured fragmentation expression safely.

        User-authored expressions must be SQLAlchemy expression objects and
        may reference only columns of the table being fragmented.  Reflected
        catalog expressions are trusted only because they originate in
        SYSFRAGMENTS and are represented by an internal immutable marker.
        """
        from .fragmentation import _ReflectedFragmentExpression

        if isinstance(value, _ReflectedFragmentExpression):
            return value.sql

        table = subject.table if isinstance(subject, sa_schema.Index) else subject
        if any(
            isinstance(element, selectable.SelectBase)
            for element in sql_visitors.iterate(value)
        ) or isinstance(value, selectable.ScalarSelect):
            raise exc.CompileError(
                f"{role} cannot contain a subquery"
            )
        columns = {
            element
            for element in sql_visitors.iterate(value)
            if isinstance(element, sql_elements.ColumnClause)
        }
        for column in columns:
            if getattr(column, "table", None) is not table:
                raise exc.CompileError(
                    f"{role} may reference only columns of the fragmented table"
                )
        if require_column and not columns:
            raise exc.CompileError(
                f"{role} must reference at least one column of the fragmented table"
            )

        return self.sql_compiler.process(
            value,
            literal_binds=True,
            include_table=False,
        )

    def _range_interval_key_columns(self, value, subject):
        """Return the single native column used by RANGE INTERVAL.

        Informix RANGE INTERVAL fragmentation is intentionally narrower than
        general expression fragmentation: its key must depend on exactly one
        numeric, DATE, or DATETIME column.  Reflected catalog expressions are
        already validated by the server and therefore remain round-trippable
        without attempting to parse catalog SQL back into a SQLAlchemy tree.
        """
        from .fragmentation import _ReflectedFragmentExpression

        if isinstance(value, _ReflectedFragmentExpression):
            return ()

        table = subject.table if isinstance(subject, sa_schema.Index) else subject
        columns = tuple(
            {
                element
                for element in sql_visitors.iterate(value)
                if isinstance(element, sql_elements.ColumnClause)
            }
        )
        if len(columns) != 1:
            raise exc.CompileError(
                "range-interval fragmentation key must reference exactly "
                "one column of the fragmented table"
            )

        column = columns[0]
        if getattr(column, "table", None) is not table:
            raise exc.CompileError(
                "range-interval fragmentation key may reference only a "
                "column of the fragmented table"
            )

        if not isinstance(
            column.type,
            (
                sa_types.Integer,
                sa_types.Numeric,
                sa_types.Date,
                sa_types.DateTime,
            ),
        ):
            raise exc.CompileError(
                "range-interval fragmentation key must use a numeric, DATE, "
                "or DATETIME column"
            )
        return columns

    def _fragment_literal_sql(self, value, subject, *, role):
        """Compile a fragment boundary/list value through SQLAlchemy."""
        from .fragmentation import _ReflectedFragmentExpression

        if isinstance(value, _ReflectedFragmentExpression):
            return value.sql
        expression = (
            value
            if isinstance(value, sql_elements.ClauseElement)
            else sql.literal(value)
        )
        if isinstance(expression, sql_elements.TextClause):
            raise exc.CompileError(
                f"{role} must be a structured SQLAlchemy constant expression"
            )
        if any(
            isinstance(element, selectable.SelectBase)
            for element in sql_visitors.iterate(expression)
        ) or isinstance(expression, selectable.ScalarSelect):
            raise exc.CompileError(f"{role} cannot contain a subquery")
        columns = {
            element
            for element in sql_visitors.iterate(expression)
            if isinstance(element, sql_elements.ColumnClause)
        }
        if columns:
            raise exc.CompileError(f"{role} must be a constant expression")
        return self.sql_compiler.process(
            expression,
            literal_binds=True,
            include_table=False,
        )

    def _fragment_prefix(self, fragmentation):
        return "PARTITION BY" if fragmentation.partition_by else "FRAGMENT BY"

    def _format_fragment_name(self, fragment):
        if fragment.name is None:
            return ""
        return f"PARTITION {self._format_fragment_identifier(fragment.name)} "

    def _compile_expression_fragment(self, fragment, subject):
        prefix = self._format_fragment_name(fragment)
        if fragment._catalog_selector is not None:
            selector = fragment._catalog_selector.sql
        elif fragment.remainder:
            selector = "REMAINDER"
        elif fragment.is_null:
            selector = "VALUES (NULL)"
        else:
            expression = self._fragment_expression_sql(
                fragment.expression,
                subject,
                role="fragment expression",
            )
            stripped = expression.strip()
            selector = (
                stripped
                if stripped.startswith("(") and stripped.endswith(")")
                else f"({stripped})"
            )
        return (
            f"{prefix}{selector} IN "
            f"{self._format_fragment_identifier(fragment.dbspace)}"
        )

    def _compile_list_fragment(self, fragment, subject):
        prefix = self._format_fragment_name(fragment)
        if fragment._catalog_selector is not None:
            selector = fragment._catalog_selector.sql
        elif fragment.remainder:
            selector = "REMAINDER"
        elif fragment.is_null:
            selector = "VALUES (NULL)"
        else:
            values = ", ".join(
                self._fragment_literal_sql(
                    value,
                    subject,
                    role="list fragmentation value",
                )
                for value in fragment.values
            )
            selector = f"VALUES ({values})"
        return (
            f"{prefix}{selector} IN "
            f"{self._format_fragment_identifier(fragment.dbspace)}"
        )

    def _compile_range_interval_fragment(self, fragment, subject):
        prefix = self._format_fragment_name(fragment)
        if fragment._catalog_selector is not None:
            selector = fragment._catalog_selector.sql
        elif fragment.is_null:
            selector = "VALUES IS NULL"
        else:
            upper_bound = self._fragment_literal_sql(
                fragment.upper_bound,
                subject,
                role="range-interval upper bound",
            )
            selector = f"VALUES < {upper_bound}"
        return (
            f"{prefix}{selector} IN "
            f"{self._format_fragment_identifier(fragment.dbspace)}"
        )

    def _compile_fragmentation(self, fragmentation, subject):
        """Render one typed table/index fragmentation strategy."""
        from .fragmentation import (
            AttachedIndexFragmentation,
            ExpressionFragmentation,
            ListFragmentation,
            RangeFragmentation,
            RangeIntervalFragmentation,
            RoundRobinFragmentation,
        )

        if isinstance(fragmentation, AttachedIndexFragmentation):
            if not isinstance(subject, sa_schema.Index):
                raise exc.CompileError(
                    "AttachedIndexFragmentation is valid only for reflected indexes"
                )
            return ""

        prefix = self._fragment_prefix(fragmentation)

        if isinstance(fragmentation, RoundRobinFragmentation):
            if isinstance(subject, sa_schema.Index):
                raise exc.CompileError(
                    "Informix does not support ROUND ROBIN fragmentation for indexes"
                )
            if fragmentation.fragments:
                fragments = ", ".join(
                    self._format_fragment_name(fragment)
                    + "IN "
                    + self._format_fragment_identifier(fragment.dbspace)
                    for fragment in fragmentation.fragments
                )
                return f"{prefix} ROUND ROBIN {fragments}"
            else:
                fragments = ", ".join(
                    self._format_fragment_identifier(dbspace)
                    for dbspace in fragmentation.dbspaces
                )
                return f"{prefix} ROUND ROBIN IN {fragments}"

        if isinstance(fragmentation, (ExpressionFragmentation, RangeFragmentation)):
            fragments = ", ".join(
                self._compile_expression_fragment(fragment, subject)
                for fragment in fragmentation.fragments
            )
            return f"{prefix} EXPRESSION {fragments}"

        if isinstance(fragmentation, ListFragmentation):
            key = self._fragment_expression_sql(
                fragmentation.key,
                subject,
                role="list fragmentation key",
                require_column=True,
            )
            fragments = ", ".join(
                self._compile_list_fragment(fragment, subject)
                for fragment in fragmentation.fragments
            )
            return f"{prefix} LIST ({key}) {fragments}"

        if isinstance(fragmentation, RangeIntervalFragmentation):
            self._range_interval_key_columns(fragmentation.key, subject)
            key = self._fragment_expression_sql(
                fragmentation.key,
                subject,
                role="range-interval key",
                require_column=True,
            )
            interval = self._fragment_literal_sql(
                fragmentation.interval,
                subject,
                role="range-interval interval",
            )
            text = f"{prefix} RANGE ({key}) INTERVAL ({interval})"
            if fragmentation.store_in:
                dbspaces = ", ".join(
                    self._format_fragment_identifier(dbspace)
                    for dbspace in fragmentation.store_in
                )
                text += f" STORE IN ({dbspaces})"
            fragments = ", ".join(
                self._compile_range_interval_fragment(fragment, subject)
                for fragment in fragmentation.fragments
            )
            return f"{text} {fragments}"

        raise exc.CompileError(
            "informix_fragment_by must be a typed Informix fragmentation object"
        )

    def _fragment_storage_clauses(self, subject, options):
        """Return mutually exclusive dbspace/fragmentation CREATE clauses."""
        dbspace = options.get("dbspace")
        fragment_by = options.get("fragment_by")
        if dbspace is not None and fragment_by is not None:
            raise exc.CompileError(
                "informix_dbspace and informix_fragment_by are mutually exclusive"
            )
        if dbspace is not None:
            from .fragmentation import _identifier

            _identifier(dbspace, "dbspace")
            return [f"IN {self._format_fragment_identifier(dbspace)}"]
        if fragment_by is not None:
            clause = self._compile_fragmentation(fragment_by, subject)
            return [clause] if clause else []
        return []

    def _format_fragment_subject(self, subject):
        if isinstance(subject, sa_schema.Table):
            return "TABLE", self.preparer.format_table(subject)
        if isinstance(subject, sa_schema.Index):
            return "INDEX", self.preparer.format_index(subject)
        raise exc.CompileError("ALTER FRAGMENT requires a Table or Index")

    def _alter_fragment_prefix(self, alter):
        kind, name = self._format_fragment_subject(alter.subject)
        text = "ALTER FRAGMENT"
        if alter.online:
            if not isinstance(alter.subject, sa_schema.Table):
                raise exc.CompileError(
                    "ALTER FRAGMENT ONLINE requires a table"
                )
            from .fragmentation import RangeIntervalFragmentation

            fragment_by = alter.subject.dialect_options["informix"].get(
                "fragment_by"
            )
            if fragment_by is not None and not isinstance(
                fragment_by,
                RangeIntervalFragmentation,
            ):
                raise exc.CompileError(
                    "ALTER FRAGMENT ONLINE requires a range-interval table"
                )
            text += " ONLINE"
        return f"{text} ON {kind} {name}"

    def _alter_fragment_position(self, before, after):
        if before is not None:
            return f" BEFORE {self._format_fragment_identifier(before)}"
        if after is not None:
            return f" AFTER {self._format_fragment_identifier(after)}"
        return ""

    def _compile_alter_fragment_selector(self, fragment, subject):
        if fragment.expression is not None or fragment.remainder:
            return self._compile_expression_fragment(fragment, subject)
        if fragment.values or fragment.is_null:
            return self._compile_list_fragment(fragment, subject)
        if fragment.has_upper_bound:
            return self._compile_range_interval_fragment(fragment, subject)
        if fragment.dbspace is not None:
            return (
                self._format_fragment_name(fragment)
                + "IN "
                + self._format_fragment_identifier(fragment.dbspace)
            )
        raise exc.CompileError("ALTER FRAGMENT requires a complete fragment definition")

    def visit_init_fragmentation(self, alter, **kw):
        _ = kw
        if alter.online:
            raise exc.CompileError("ALTER FRAGMENT INIT does not support ONLINE")
        prefix = self._alter_fragment_prefix(alter)
        if alter.fragment_by is not None:
            clause = self._compile_fragmentation(alter.fragment_by, alter.subject)
        else:
            clause = ""
            if alter.fragment_name is not None:
                clause += (
                    "PARTITION "
                    + self._format_fragment_identifier(alter.fragment_name)
                    + " "
                )
            clause += "IN " + self._format_fragment_identifier(alter.dbspace)
        return f"{prefix} INIT {clause}"

    def visit_add_fragment(self, alter, **kw):
        _ = kw
        if alter.online:
            raise exc.CompileError("ALTER FRAGMENT ADD does not support ONLINE")
        prefix = self._alter_fragment_prefix(alter)
        if alter.interval_dbspaces:
            spaces = ", ".join(
                self._format_fragment_identifier(value)
                for value in alter.interval_dbspaces
            )
            clause = f"INTERVAL STORE IN ({spaces})"
        else:
            clause = self._compile_alter_fragment_selector(
                alter.fragment, alter.subject
            )
            clause += self._alter_fragment_position(alter.before, alter.after)
        return f"{prefix} ADD {clause}"

    def visit_drop_fragment(self, alter, **kw):
        _ = kw
        if alter.online:
            raise exc.CompileError("ALTER FRAGMENT DROP does not support ONLINE")
        prefix = self._alter_fragment_prefix(alter)
        if alter.interval_dbspaces:
            spaces = ", ".join(
                self._format_fragment_identifier(value)
                for value in alter.interval_dbspaces
            )
            clause = f"INTERVAL STORE IN ({spaces})"
        else:
            noun = "PARTITION " if alter.partition else ""
            clause = noun + self._format_fragment_identifier(alter.fragment_name)
        return f"{prefix} DROP {clause}"

    def visit_modify_fragment(self, alter, **kw):
        _ = kw
        if alter.online and not isinstance(alter.subject, sa_schema.Table):
            raise exc.CompileError("ALTER FRAGMENT ONLINE MODIFY requires a table")
        prefix = self._alter_fragment_prefix(alter)
        old_noun = "PARTITION " if alter.old_partition else ""
        old_name = old_noun + self._format_fragment_identifier(alter.old_name)
        replacement = self._compile_alter_fragment_selector(
            alter.fragment, alter.subject
        )
        return f"{prefix} MODIFY {old_name} TO {replacement}"

    def _compile_attach_fragment_selector(self, fragment, subject):
        """Compile the ATTACH ``AS`` clause without an ``IN dbspace``.

        Informix stores the attached fragment where the consumed table already
        resides.  The AS clause can name the new partition and define its
        expression/list/range selector, but it has no storage-location clause.
        """
        prefix = self._format_fragment_name(fragment)
        if fragment._catalog_selector is not None:
            selector = fragment._catalog_selector.sql
        elif fragment.remainder:
            selector = "REMAINDER"
        elif fragment.is_null:
            selector = "VALUES (NULL)"
        elif fragment.expression is not None:
            expression = self._fragment_expression_sql(
                fragment.expression,
                subject,
                role="ATTACH fragment expression",
            )
            stripped = expression.strip()
            selector = (
                stripped
                if stripped.startswith("(") and stripped.endswith(")")
                else f"({stripped})"
            )
        elif fragment.values:
            values = ", ".join(
                self._fragment_literal_sql(
                    value,
                    subject,
                    role="ATTACH list value",
                )
                for value in fragment.values
            )
            selector = f"VALUES ({values})"
        elif fragment.has_upper_bound:
            upper_bound = self._fragment_literal_sql(
                fragment.upper_bound,
                subject,
                role="ATTACH range upper bound",
            )
            selector = f"VALUES < {upper_bound}"
        else:
            selector = ""

        return (prefix + selector).rstrip()

    def visit_attach_fragment(self, alter, **kw):
        _ = kw
        prefix = self._alter_fragment_prefix(alter)
        consumed = self.preparer.format_table(alter.consumed_table)
        text = f"{prefix} ATTACH {consumed}"
        if alter.fragment is not None:
            text += " AS " + self._compile_attach_fragment_selector(
                alter.fragment, alter.subject
            )
        text += self._alter_fragment_position(alter.before, alter.after)
        return text

    def visit_detach_fragment(self, alter, **kw):
        _ = kw
        prefix = self._alter_fragment_prefix(alter)
        noun = "PARTITION " if alter.partition else ""
        fragment = noun + self._format_fragment_identifier(alter.fragment_name)
        new_table = self.preparer.format_table(alter.new_table)
        return f"{prefix} DETACH {fragment} {new_table}"

    @staticmethod
    def _positive_table_storage_value(
        option_name,
        value,
        *,
        error_name=None,
    ):
        """Validate one native Informix table storage value.

        Informix expresses EXTENT SIZE and NEXT SIZE in kilobytes.  SQLAlchemy
        table dialect options and executable DDL parameters are deliberately
        restricted to positive Python integers so generated SQL cannot contain
        arbitrary expressions.
        """
        if value is None:
            return None

        display_name = error_name or f"informix_{option_name}"

        if isinstance(value, bool) or not isinstance(value, int):
            raise exc.CompileError(
                f"{display_name} must be a positive integer "
                "expressed in kilobytes"
            )

        if value <= 0:
            raise exc.CompileError(
                f"{display_name} must be greater than zero"
            )

        return value

    def _normalize_writable_table_lock_level(
        self,
        lock_level,
        *,
        option_name,
        allow_reflected_catalog_state=False,
    ):
        """Validate a lock level accepted by native Informix DDL.

        ``PAGE_AND_ROW`` is a catalog state, not a writable CREATE/ALTER
        clause.  It is ignored only when recompiling metadata that the
        Informix reflector marked explicitly; user-authored values and ALTER
        operations remain strictly limited to PAGE and ROW.
        """
        if not isinstance(lock_level, str):
            raise exc.CompileError(
                f"{option_name} must be either 'PAGE' or 'ROW'"
            )

        normalized = lock_level.strip().upper()
        if normalized in self._WRITABLE_TABLE_LOCK_LEVELS:
            return normalized

        if (
            allow_reflected_catalog_state
            and normalized == "PAGE_AND_ROW"
            and getattr(
                lock_level,
                "_informix_reflected_lock_level",
                False,
            )
        ):
            return None

        raise exc.CompileError(
            f"{option_name} must be either 'PAGE' or 'ROW'; "
            f"received {lock_level!r}"
        )

    def _table_lock_mode_clause(self, table_options):
        lock_level = table_options.get("lock_level")
        if lock_level is None:
            return None

        normalized = self._normalize_writable_table_lock_level(
            lock_level,
            option_name="informix_lock_level",
            allow_reflected_catalog_state=True,
        )
        if normalized is None:
            # Informix can report the catalog-only B state, but CREATE TABLE
            # accepts only PAGE or ROW. Preserve the reflected metadata
            # without inventing a lossy DDL translation.
            return None

        return f"LOCK MODE {normalized}"

    def visit_set_table_lock_mode(self, alter, **kw):
        """Render ``ALTER TABLE ... LOCK MODE (...)`` for Informix."""
        lock_level = self._normalize_writable_table_lock_level(
            alter.lock_mode,
            option_name="lock_mode",
        )
        table_name = self.preparer.format_table(alter.table)
        return f"ALTER TABLE {table_name} LOCK MODE ({lock_level})"

    def _format_synonym_name(self, synonym_name):
        """Render a structured synonym identifier for the current database."""
        name = self.preparer.quote(synonym_name.name)
        if synonym_name.owner is None:
            return name
        return f"{self.preparer.quote(synonym_name.owner)}.{name}"

    def _format_synonym_target(self, target):
        """Render local and remote Informix object qualification safely."""
        object_name = self.preparer.quote(target.name)
        if target.owner is not None:
            object_name = (
                f"{self.preparer.quote(target.owner)}.{object_name}"
            )

        if target.database is None:
            return object_name

        database = self.preparer.quote(target.database)
        if target.server is not None:
            database += f"@{self.preparer.quote(target.server)}"
        return f"{database}:{object_name}"

    def visit_create_synonym(self, create, **kw):
        """Render native Informix CREATE SYNONYM DDL.

        PUBLIC and PRIVATE are legal only in non-ANSI databases.  Every name
        component has already been validated and is quoted independently, so
        no caller-provided string is treated as executable SQL.
        """
        _ = kw
        if self.dialect.is_ansi_database and create.public is not None:
            raise exc.CompileError(
                "Informix ANSI databases do not accept PUBLIC or PRIVATE "
                "in CREATE SYNONYM"
            )
        text = "CREATE "
        if create.public is True:
            text += "PUBLIC "
        elif create.public is False:
            text += "PRIVATE "
        text += "SYNONYM "
        if create.if_not_exists:
            text += "IF NOT EXISTS "
        text += self._format_synonym_name(create.name)
        text += " FOR "
        text += self._format_synonym_target(create.target)
        return text

    def visit_drop_synonym(self, drop, **kw):
        """Render native, idempotent Informix DROP SYNONYM DDL."""
        _ = kw
        text = "DROP SYNONYM "
        if drop.if_exists:
            text += "IF EXISTS "
        return text + self._format_synonym_name(drop.name)

    def visit_modify_table_extents(self, alter, **kw):
        """Render native, validated Informix extent modifications."""
        first_extent = self._positive_table_storage_value(
            "first_extent",
            alter.first_extent,
            error_name="first_extent",
        )
        next_extent = self._positive_table_storage_value(
            "next_extent",
            alter.next_extent,
            error_name="next_extent",
        )

        clauses = []
        if first_extent is not None:
            clauses.append(f"EXTENT SIZE {first_extent}")
        if next_extent is not None:
            clauses.append(f"NEXT SIZE {next_extent}")

        # The construct rejects an empty operation in __init__.  Keep this
        # defensive guard because executable DDL objects remain mutable.
        if not clauses:
            raise exc.CompileError(
                "ModifyTableExtents requires first_extent and/or "
                "next_extent"
            )

        table_name = self.preparer.format_table(alter.table)
        return f"ALTER TABLE {table_name} MODIFY {' '.join(clauses)}"

    @staticmethod
    def _validate_table_page_size(table_options):
        """Reject user-authored page sizes while accepting reflected metadata.

        Informix page size belongs to the dbspace, not to an individual table.
        SYSTABLES exposes the effective page size, so reflection retains it as
        ``informix_page_size``.  The reflector marks that integer internally;
        only such reflected values may pass through table re-compilation.
        """
        page_size = table_options.get("page_size")
        if page_size is None:
            return

        if getattr(page_size, "_informix_reflected_page_size", False):
            return

        raise exc.CompileError(
            "informix_page_size is reflection-only because Informix page "
            "size is defined by the dbspace, not by CREATE TABLE. Place the "
            "table in a dbspace with the required page size instead."
        )

    def post_create_table(self, table):
        """Render native Informix physical table options.

        The order is intentionally stable: extents, physical location or
        fragmentation, smart-LOB options (when introduced), compression, and
        finally LOCK MODE.  Page size is not emitted because it is inherited
        from the selected dbspace.
        """
        table_options = table.dialect_options["informix"]
        self._validate_table_page_size(table_options)

        clauses = []
        first_extent = self._positive_table_storage_value(
            "first_extent",
            table_options.get("first_extent"),
        )
        next_extent = self._positive_table_storage_value(
            "next_extent",
            table_options.get("next_extent"),
        )

        if first_extent is not None:
            clauses.append(f"EXTENT SIZE {first_extent}")
        if next_extent is not None:
            clauses.append(f"NEXT SIZE {next_extent}")

        clauses.extend(self._fragment_storage_clauses(table, table_options))

        compressed = table_options.get("compressed")
        if compressed is not None:
            if not isinstance(compressed, bool):
                raise exc.CompileError("informix_compressed must be a boolean")
            if compressed:
                clauses.append("COMPRESSED")

        lock_clause = self._table_lock_mode_clause(table_options)
        if lock_clause is not None:
            clauses.append(lock_clause)

        if not clauses:
            return ""

        return " " + " ".join(clauses)

    def visit_create_sequence(self, create, prefix=None, **kw):
        """Render native Informix CREATE SEQUENCE DDL.

        Informix 14.10 and later support ``IF NOT EXISTS`` immediately after
        the ``CREATE SEQUENCE`` keywords.  Use SQLAlchemy's
        :class:`~sqlalchemy.schema.CreateSequence` flag directly instead of
        emulating idempotency with a catalog lookup, which would be vulnerable
        to a check-then-create race.
        """
        sequence = create.element
        text = "CREATE SEQUENCE "
        if create.if_not_exists:
            text += "IF NOT EXISTS "
        text += self.preparer.format_sequence(sequence)
        options = []

        if sequence.start is not None:
            options.append("START WITH %d" % sequence.start)
        if sequence.increment is not None:
            options.append("INCREMENT BY %d" % sequence.increment)
        if sequence.minvalue is not None:
            options.append("MINVALUE %d" % sequence.minvalue)
        if sequence.maxvalue is not None:
            options.append("MAXVALUE %d" % sequence.maxvalue)
        if sequence.cache is not None:
            options.append("CACHE %d" % sequence.cache)
        if sequence.cycle is True:
            options.append("CYCLE")

        if options:
            text += " " + " ".join(options)

        return text

    def visit_drop_sequence(self, drop, **kw):
        """Render native Informix DROP SEQUENCE DDL.

        Informix 14.10 and later support ``IF EXISTS`` immediately after the
        ``DROP SEQUENCE`` keywords.
        """
        text = "DROP SEQUENCE "
        if drop.if_exists:
            text += "IF EXISTS "
        return text + self.preparer.format_sequence(drop.element)

    def get_server_version_info(self, dialect):
        """Returns the Informix server major and minor version as a list of ints."""
        if hasattr(dialect, 'dbms_ver'):
            return [int(ver_token) for ver_token in dialect.dbms_ver.split('.')[0:2]]
        else:
            return []

    def _is_nullable_unique_constraint_supported(self, dialect):
        """Checks to see if the Informix version is at least 10.5.
        This is needed for checking if unique constraints with null columns are supported.
        """

        dbms_name = getattr(dialect, 'dbms_name', None)
        if hasattr(dialect, 'dbms_name'):
            if dbms_name is not None and (dbms_name.find('Informix/') != -1):
                return self.get_server_version_info(dialect) >= [10, 5]
        else:
            return False

    def get_column_default_string(self, column):
        """Render and validate an Informix column server default.

        Informix requires CURRENT to carry the same DATETIME qualifier as the
        target column. It also rejects quoted numeric defaults for numeric
        columns and arbitrary arithmetic expressions in DEFAULT clauses.

        BOOLEAN defaults reflected as canonical true/false values are rendered
        again using Informix's native t/f representation.
        """
        server_default = column.server_default

        if not isinstance(server_default, sa_schema.DefaultClause):
            return None

        default_arg = server_default.arg

        if (
            isinstance(column.type, sa_types.DateTime)
            and _is_ifx_current_datetime_default(default_arg)
        ):
            default_sql = _ifx_current_default_for_column(
                self.dialect.type_compiler,
                column,
            )
        elif (
            isinstance(default_arg, str)
            and _is_ifx_numeric_type(column.type)
            and _IFX_NUMERIC_DEFAULT_RE.fullmatch(default_arg.strip())
        ):
            default_sql = default_arg.strip()
        else:
            default_sql = super().get_column_default_string(column)

        if (
            default_sql is not None
            and isinstance(column.type, sa_types.Boolean)
        ):
            normalized_boolean = _normalize_ifx_boolean_default(default_sql)

            if normalized_boolean == "true":
                default_sql = "'t'"
            elif normalized_boolean == "false":
                default_sql = "'f'"

        if default_sql is not None:
            _validate_ifx_server_default(column, default_sql)

        return default_sql

    def get_column_specification(self, column, **kw):
        col_spec = [self.preparer.format_column(column)]

        rendered_type = self.dialect.type_compiler.process(
            column.type,
            type_expression=column,
        )

        autoincrement_type_name = _get_ifx_autoincrement_type_name(column)

        if autoincrement_type_name in {"SERIAL", "SERIAL8", "BIGSERIAL"}:
            rendered_type = autoincrement_type_name

        col_spec.append(rendered_type)

        default = self.get_column_default_string(column)
        if default is not None:
            col_spec.extend(("DEFAULT", default))

        if not column.nullable or column.primary_key:
            col_spec.append("NOT NULL")

        return " ".join(col_spec)

    def _is_inline_self_referential_fk(self, constraint):
        """Compatibility wrapper for the shared self-reference detector."""
        return _is_ifx_simple_self_referential_fk(constraint)

    @staticmethod
    def _normalize_ondelete_action(action):
        """Validate and normalize the referential action supported by Informix."""
        if action is None:
            return None

        if not isinstance(action, str):
            raise exc.CompileError(
                "Informix foreign-key ON DELETE action must be a string or None"
            )

        normalized_action = " ".join(action.upper().split())

        if normalized_action != "CASCADE":
            raise exc.CompileError(
                "Informix supports only ON DELETE CASCADE for "
                "foreign-key constraints; received %r" % action
            )

        return normalized_action

    def define_constraint_cascades(self, constraint):
        text = ""
        ondelete = self._normalize_ondelete_action(constraint.ondelete)

        if ondelete is not None:
            text += " ON DELETE %s" % ondelete

        if constraint.onupdate is not None:
            util.warn(
                "Informix does not support UPDATE CASCADE for foreign keys."
            )

        return text

    def _constraint_table(self, constraint):
        """Return the table associated with a constraint.

        Table-level constraints expose ``constraint.table`` directly.
        Column-level CHECK constraints are attached to a Column and their
        ``table`` property raises InvalidRequestError, so the table must be
        obtained through the parent column.
        """
        try:
            return constraint.table
        except exc.InvalidRequestError:
            parent = getattr(constraint, "parent", None)
            return getattr(parent, "table", None)

    def _physical_constraint_name(self, constraint):
        """Return the Informix catalog name used for a constraint.

        Informix can enforce constraint names globally across owners. Prefix
        schema-owned objects physically while keeping the SQLAlchemy object's
        logical name unchanged.

        Naming-convention names must retain SQLAlchemy's truncation marker so
        that IdentifierPreparer can shorten them deterministically when they
        exceed Informix's maximum identifier length.
        """
        if constraint.name is None:
            return None

        logical_name = constraint.name
        table = self._constraint_table(constraint)
        schema = getattr(table, "schema", None)

        if schema:
            physical_name = f"{schema}__{logical_name}"

            if isinstance(
                logical_name,
                sql.elements._truncated_label,
            ):
                return type(logical_name)(
                    physical_name,
                    getattr(logical_name, "quote", None),
                )

            return physical_name

        return logical_name

    def _format_physical_constraint_name(self, constraint):
        physical_name = self._physical_constraint_name(constraint)
        if physical_name is None:
            return None

        return self.preparer.truncate_and_render_constraint_name(
            physical_name
        )

    def _define_constraint_name_postfix(self, constraint):
        formatted_name = self._format_physical_constraint_name(constraint)
        if formatted_name is None:
            return ""

        return " CONSTRAINT %s" % formatted_name

    def visit_primary_key_constraint(self, constraint, **kw):
        if len(constraint) == 0:
            return ""

        text = self.define_primary_key_body(constraint, **kw)
        text += self._define_constraint_name_postfix(constraint)
        text += self.define_constraint_deferrability(constraint)
        return text

    def _mark_unique_constraint_as_index(self, schema_item):
        schema_item.info[_IFX_UNIQUE_CONSTRAINT_AS_INDEX] = True

    def _is_unique_constraint_as_index(self, schema_item):
        return bool(
            getattr(schema_item, "info", {}).get(
                _IFX_UNIQUE_CONSTRAINT_AS_INDEX
            )
        )

    def _has_nullable_column(self, constraint):
        return any(column.nullable for column in constraint)

    def _should_use_nullable_unique_index(self, constraint):
        return (
            self._is_nullable_unique_constraint_supported(self.dialect)
            and isinstance(constraint, sa_schema.UniqueConstraint)
            and self._has_nullable_column(constraint)
        )

    def _unique_index_name(self, constraint, prefix):
        if isinstance(constraint.name, str) and constraint.name:
            return constraint.name

        return "%s_%s_%s" % (
            prefix,
            self.preparer.format_table(constraint.table),
            "_".join(column.name for column in constraint),
        )

    def _create_unique_index_for_constraint(self, constraint, prefix):
        index = sa_schema.Index(
            self._unique_index_name(constraint, prefix),
            *constraint
        )
        index.unique = True
        self._mark_unique_constraint_as_index(index)
        return index

    def _defer_unique_constraint_to_index(self, constraint, prefix):
        setattr(constraint, "use_alter", True)
        self._mark_unique_constraint_as_index(constraint)
        return self._create_unique_index_for_constraint(constraint, prefix)

    def visit_unique_constraint(self, constraint, **kw):
        if len(constraint) == 0:
            return ""

        text = self.define_unique_body(constraint, **kw)
        text += self._define_constraint_name_postfix(constraint)
        text += self.define_constraint_deferrability(constraint)
        return text

    def visit_foreign_key_constraint(self, constraint, **kw):
        text = self.define_foreign_key_body(constraint, **kw)
        text += self._define_constraint_name_postfix(constraint)
        text += self.define_constraint_match(constraint)
        text += self.define_constraint_cascades(constraint)
        text += self.define_constraint_deferrability(constraint)
        return text

    def visit_check_constraint(self, constraint, **kw):
        text = self.define_check_body(constraint, **kw)
        text += self._define_constraint_name_postfix(constraint)
        text += self.define_constraint_deferrability(constraint)
        return text

    def visit_column_check_constraint(self, constraint, **kw):
        """Compile a column CHECK using Informix postfix naming syntax."""
        return self.visit_check_constraint(constraint, **kw)

    def visit_drop_constraint(self, drop, **kw):
        """Compile constraint removal using Informix ALTER TABLE syntax.

        Informix removes named CHECK, UNIQUE and FOREIGN KEY constraints with
        ``DROP CONSTRAINT <name>``.  ``DROP FOREIGN KEY`` is not valid
        Informix SQL and produced error -201 during ``MetaData.drop_all()``.
        A nullable UNIQUE constraint implemented by this dialect as an index
        remains the only case that is removed with a standalone ``DROP INDEX``.
        """
        constraint = drop.element

        if self._is_unique_constraint_as_index(constraint):
            const = self._format_physical_constraint_name(constraint)
            return f"DROP INDEX {const}"

        table_name = self.preparer.format_table(constraint.table)

        if isinstance(constraint, sa_schema.PrimaryKeyConstraint):
            # Informix accepts the structural form for an unnamed or named PK.
            return f"ALTER TABLE {table_name} DROP PRIMARY KEY"

        const = self._format_physical_constraint_name(constraint)
        if not const:
            raise exc.CompileError(
                "Informix requires a name when dropping this constraint"
            )

        return f"ALTER TABLE {table_name} DROP CONSTRAINT {const}"

    def create_table_constraints(
        self,
        table,
        _include_foreign_key_constraints=None,
        **kw,
    ):
        for constraint in sqla_compat.get_table_sorted_constraints(table):
            if self._should_use_nullable_unique_index(constraint):
                self._defer_unique_constraint_to_index(constraint, "ukey")

        inline_self_fks = {
            constraint
            for constraint in table.foreign_key_constraints
            if self._is_inline_self_referential_fk(constraint)
        }

        if _include_foreign_key_constraints is None:
            included_fks = (
                table.foreign_key_constraints - inline_self_fks
            )
        else:
            included_fks = (
                set(_include_foreign_key_constraints)
                - inline_self_fks
            )

        return super().create_table_constraints(
            table,
            _include_foreign_key_constraints=included_fks,
            **kw,
        )

    def _physical_index_name(self, index):
        """Return the Informix catalog name used for an index.

        Preserve SQLAlchemy's truncation marker for names generated from a
        naming convention. This allows ``IdentifierPreparer`` to shorten long
        index names deterministically instead of validating them as ordinary
        strings.
        """
        if index.name is None:
            raise exc.CompileError(
                "CREATE INDEX requires that the index have a name"
            )

        logical_name = index.name
        table = getattr(index, "table", None)
        schema = getattr(table, "schema", None)

        if schema:
            physical_name = f"{schema}__{logical_name}"

            if isinstance(
                logical_name,
                sql.elements._truncated_label,
            ):
                return type(logical_name)(
                    physical_name,
                    getattr(logical_name, "quote", None),
                )

            return physical_name

        return logical_name

    def _prepared_index_name(self, index, include_schema=False):
        _ = include_schema
        return self.preparer.truncate_and_render_index_name(
            self._physical_index_name(index)
        )

    def _informix_index_options(self, index):
        return index.dialect_options["informix"]

    def _unwrap_functional_index_expression(self, expression):
        """Return the function and sort direction for a safe index key.

        The first functional-index implementation deliberately supports one
        function call whose arguments are direct columns of the indexed table.
        Arbitrary SQL expressions remain unsupported because Informix accepts
        functional indexes only on nonvariant UDRs, not on arbitrary built-in
        expressions.
        """
        descending = False

        if isinstance(expression, sql_elements.UnaryExpression):
            if expression.modifier is operators.desc_op:
                descending = True
            elif expression.modifier is not operators.asc_op:
                raise exc.CompileError(
                    "Informix functional indexes support only ASC or DESC "
                    "ordering around the function call"
                )
            expression = expression.element

        if not isinstance(expression, sql_functions.FunctionElement):
            raise exc.CompileError(
                "informix_functional=True requires a SQLAlchemy function "
                "call over columns of the indexed table"
            )

        return expression, descending

    def _validate_functional_index(self, index):
        options = self._informix_index_options(index)
        explicitly_functional = bool(options.get("functional"))
        reflected_procedure = options.get("procedure")

        def is_plain_column_expression(expression):
            if isinstance(expression, sa_schema.Column):
                return True
            if isinstance(expression, sql_elements.UnaryExpression):
                return (
                    expression.modifier in (operators.asc_op, operators.desc_op)
                    and isinstance(expression.element, sa_schema.Column)
                )
            return False

        has_non_column_expression = any(
            not is_plain_column_expression(expression)
            for expression in index.expressions
        )

        if not explicitly_functional and not reflected_procedure:
            if has_non_column_expression:
                raise exc.CompileError(
                    "Informix expression indexes must opt in with "
                    "informix_functional=True. Only a nonvariant UDR call "
                    "over columns of the indexed table is supported."
                )
            return

        # Reflected functional indexes are represented by TextClause entries
        # plus catalog procedure metadata. They were already validated by the
        # server when created, so mixed and multi-key indexes can be emitted
        # again without widening the contract for newly declared indexes.
        if reflected_procedure:
            has_reflected_expression = False
            for expression in index.expressions:
                if isinstance(expression, sql_elements.TextClause):
                    has_reflected_expression = True
                    continue
                if not is_plain_column_expression(expression):
                    raise exc.CompileError(
                        "Reflected Informix functional indexes may contain "
                        "only table columns and reflected SQL text"
                    )
            if not has_reflected_expression:
                raise exc.CompileError(
                    "informix_procedure metadata requires at least one "
                    "reflected functional expression"
                )
            return

        if len(index.expressions) != 1:
            raise exc.CompileError(
                "The initial Informix functional-index implementation "
                "supports exactly one function key"
            )

        expression = index.expressions[0]
        function, _descending = self._unwrap_functional_index_expression(
            expression
        )

        arguments = list(function.clauses)
        if not arguments:
            raise exc.CompileError(
                "Informix functional indexes require at least one column "
                "argument"
            )

        for argument in arguments:
            if isinstance(argument, sql_elements.Grouping):
                argument = argument.element

            if not isinstance(argument, sa_schema.Column):
                raise exc.CompileError(
                    "Informix functional-index arguments must be direct "
                    "table columns; literals and nested expressions are not "
                    "supported"
                )

            if argument.table is not index.table:
                raise exc.CompileError(
                    "Every Informix functional-index argument must belong "
                    "to the indexed table"
                )

    def visit_create_index(
        self, create, include_schema=False, include_table_schema=True, **kw
    ):
        self._validate_functional_index(create.element)

        sql = super(IfxDDLCompiler, self).visit_create_index(
            create,
            include_schema=include_schema,
            include_table_schema=include_table_schema,
            **kw
        )
        index_options = create.element.dialect_options["informix"]
        storage_clauses = self._fragment_storage_clauses(
            create.element,
            index_options,
        )
        if storage_clauses:
            sql += " " + " ".join(storage_clauses)
        if self._is_unique_constraint_as_index(create.element):
            sql += ' EXCLUDE NULL KEYS'
        return sql

    def visit_add_constraint(self, create, **kw):
        if self._should_use_nullable_unique_index(create.element):
            index = self._defer_unique_constraint_to_index(
                create.element, "uk_index"
            )
            return self.visit_create_index(sa_schema.CreateIndex(index))

        sql = "ALTER TABLE %s ADD CONSTRAINT %s" % (
            self.preparer.format_table(create.element.table),
            self.process(create.element),
        )
        return sql


class IfxIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = RESERVED_WORDS
    illegal_initial_characters = set("0123456789_$")


class IfxExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        sequence_name = self.identifier_preparer.format_sequence(seq)
        return self._execute_scalar(
            "SELECT FIRST 1 "
            + sequence_name
            + ".NEXTVAL FROM systables",
            type_,
        )


class _SelectLastRowIDMixin(object):
    _select_lastrowid = False
    _lastrowid = None
    _lastrowid_query = None

    def get_lastrowid(self):
        return self._lastrowid

    def _get_lastrowid_dml_table(self):
        compiled = getattr(self, "compiled", None)
        if compiled is None:
            return None

        dml_compile_state = sqla_compat.get_dml_compile_state(compiled)
        table = getattr(dml_compile_state, "dml_table", None)
        if table is not None:
            return table

        statement = getattr(compiled, "statement", None)
        return getattr(statement, "table", None)

    def _ifx_dml_returns_rows(self):
        compiled = getattr(self, "compiled", None)
        if compiled is None:
            return False

        if sqla_compat.compiled_returns_rows(compiled):
            return True

        statement = getattr(compiled, "statement", None)
        return bool(sqla_compat.get_statement_returning(statement))

    def pre_exec(self):
        self._lastrowid = None
        self._select_lastrowid = False
        self._lastrowid_query = None

        if getattr(self, "isinsert", False):
            tbl = self._get_lastrowid_dml_table()
            if tbl is None:
                return

            seq_column = sqla_compat.get_table_autoincrement_column(tbl)
            insert_has_sequence = seq_column is not None
            compiled_parameters = getattr(self, "compiled_parameters", None)
            compiled_params = (
                compiled_parameters[0] if compiled_parameters else {}
            )
            explicit_pk_value = (
                seq_column is not None
                and compiled_params.get(seq_column.key) is not None
            )
            compiled = getattr(self, "compiled", None)

            self._select_lastrowid = insert_has_sequence and \
                not explicit_pk_value and \
                not self._ifx_dml_returns_rows() and \
                not getattr(compiled, "inline", False) and \
                not getattr(self, "executemany", False)
            if self._select_lastrowid:
                self._lastrowid_query = _get_ifx_lastrowid_query(seq_column)

    def post_exec(self):
        if self._select_lastrowid and self._lastrowid_query:
            root_connection = getattr(self, "root_connection", None)
            fairy = getattr(root_connection, "connection", None)
            dbapi_connection = getattr(fairy, "dbapi_connection", None)

            if dbapi_connection is None:
                return

            lastrowid_cursor = dbapi_connection.cursor()
            try:
                lastrowid_cursor.execute(self._lastrowid_query)
                row = lastrowid_cursor.fetchone()

                if row is not None and row[0] is not None:
                    self._lastrowid = int(row[0])
            finally:
                lastrowid_cursor.close()


class IfxDialect(default.DefaultDialect):
    div_is_floordiv = False

    name = 'informix'
    max_identifier_length = 128
    encoding = 'utf-8'
    default_paramstyle = 'qmark'
    colspecs = colspecs
    ischema_names = ischema_names
    construct_arguments = [
        (
            sa_schema.Table,
            {
                "lock_level": None,
                "first_extent": None,
                "next_extent": None,
                "page_size": None,
                "fragment_by": None,
                "dbspace": None,
                "compressed": None,
                "resolve_synonyms": False,
            },
        ),
        (
            sa_schema.Index,
            {
                "functional": False,
                "procedure": None,
                "access_method": None,
                "opclass": None,
                "fragment_by": None,
                "dbspace": None,
            },
        ),
    ]
    reflection_options = ("informix_resolve_synonyms",)
    is_ansi_database = False
    supports_char_length = False
    supports_unicode_statements = False
    supports_unicode_binds = False
    returns_unicode_strings = False
    postfetch_lastrowid = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_decimal = False
    supports_native_boolean = True
    insert_returning = False
    update_returning = False
    delete_returning = False
    supports_multivalues_insert = False
    use_insertmanyvalues = False
    use_insertmanyvalues_wo_returning = False
    supports_identity_columns = False
    supports_schemas = False
    preexecute_sequences = False
    supports_alter = True
    supports_sequences = True
    sequences_optional = True

    requires_name_normalize = True

    supports_default_values = False
    supports_empty_insert = False
    supports_statement_cache = True

    two_phase_transactions = False
    savepoints = True

    statement_compiler = IfxCompiler
    ddl_compiler = IfxDDLCompiler
    type_compiler = IfxTypeCompiler
    preparer = IfxIdentifierPreparer
    execution_ctx_cls = IfxExecutionContext

    inspector = ifx_reflection.IfxInspector
    _reflector_cls = ifx_reflection.IfxReflector

    def __init__(self, **kw):
        super(IfxDialect, self).__init__(**kw)

        self._reflector = self._reflector_cls(self)

    def _detect_database_mode(self, connection):
        """Return the current database name and its ANSI-mode flag."""
        database_name = connection.exec_driver_sql(
            """
            SELECT DBINFO('dbname')
            FROM systables
            WHERE tabid = 1
            """
        ).scalar()

        if database_name is None:
            raise exc.InvalidRequestError(
                "Informix no pudo determinar la base actual mediante "
                "DBINFO('dbname')."
            )

        database_name = str(database_name).strip()

        database_row = connection.exec_driver_sql(
            """
            SELECT FIRST 1 d.is_ansi
            FROM sysmaster:sysdatabases d
            WHERE LOWER(d.name) = LOWER(?)
            """,
            (database_name,),
        ).first()

        if database_row is None or database_row[0] is None:
            raise exc.InvalidRequestError(
                "Informix no encontró la base conectada en "
                "sysmaster:sysdatabases. "
                f"Base informada por DBINFO: {database_name!r}."
            )

        ansi_value = database_row[0]

        try:
            is_ansi_database = bool(int(ansi_value))
        except (TypeError, ValueError) as exc_value:
            raise exc.InvalidRequestError(
                "Informix devolvió un valor is_ansi no reconocido para "
                f"la base {database_name!r}: {ansi_value!r}."
            ) from exc_value

        return database_name, is_ansi_database

    def initialize(self, connection):
        super(IfxDialect, self).initialize(connection)

        self.dbms_ver = getattr(
            connection.connection,
            "dbms_ver",
            None,
        )
        self.dbms_name = getattr(
            connection.connection,
            "dbms_name",
            None,
        )

        (
            self.database_name,
            self.is_ansi_database,
        ) = self._detect_database_mode(connection)

        # SQLAlchemy's public capability must describe the connected
        # database, not the Informix server version in the abstract.
        self.supports_schemas = self.is_ansi_database

    def normalize_name(self, name):
        return self._reflector.normalize_name(name)

    def denormalize_name(self, name):
        return self._reflector.denormalize_name(name)

    def _get_default_schema_name(self, connection):
        return self._reflector._get_default_schema_name(connection)

    def has_table(self, connection, table_name, schema=None, **kw):
        return self._reflector.has_table(connection, table_name, schema=schema, **kw)

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        return self._reflector.has_sequence(
            connection, sequence_name, schema=schema, **kw
        )

    def get_sequence_names(self, connection, schema=None, **kw):
        return self._reflector.get_sequence_names(
            connection, schema=schema, **kw
        )

    def get_synonym_names(self, connection, schema=None, **kw):
        return self._reflector.get_synonym_names(
            connection,
            schema=schema,
            **kw,
        )

    def get_synonyms(self, connection, schema=None, **kw):
        return self._reflector.get_synonyms(
            connection,
            schema=schema,
            **kw,
        )

    def has_synonym(self, connection, synonym_name, schema=None, **kw):
        return self._reflector.has_synonym(
            connection,
            synonym_name,
            schema=schema,
            **kw,
        )

    def get_schema_names(self, connection, **kw):
        return self._reflector.get_schema_names(connection, **kw)

    def get_table_names(self, connection, schema=None, **kw):
        return self._reflector.get_table_names(connection, schema=schema, **kw)

    def get_temp_table_names(self, connection, schema=None, **kw):
        return self._reflector.get_temp_table_names(
                                connection, schema=schema, **kw)

    def get_view_names(self, connection, schema=None, **kw):
        return self._reflector.get_view_names(connection, schema=schema, **kw)

    def get_materialized_view_names(self, connection, schema=None, **kw):
        return self._reflector.get_materialized_view_names(
            connection, schema=schema, **kw
        )

    def get_temp_view_names(self, connection, schema=None, **kw):
        return self._reflector.get_temp_view_names(
                                connection, schema=schema, **kw)

    def get_view_definition(self, connection, viewname, schema=None, **kw):
        return self._reflector.get_view_definition(
                                connection, viewname, schema=schema, **kw)

    def get_columns(self, connection, table_name, schema=None, **kw,):
        """Reflect columns and normalize Informix catalog defaults."""
        reflected_columns = self._reflector.get_columns(connection, table_name, schema=schema, **kw,)

        return [_normalize_ifx_reflected_column(column) for column in reflected_columns]

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_pk_constraint(
                                connection, table_name, schema=schema, **kw)

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_primary_keys(
                                connection, table_name, schema=schema, **kw)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_foreign_keys(
                                connection, table_name, schema=schema, **kw)

    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_incoming_foreign_keys(
                                connection, table_name, schema=schema, **kw)

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_indexes(
                                connection, table_name, schema=schema, **kw)

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_unique_constraints(
                                connection, table_name, schema=schema, **kw)

    def get_multi_columns(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ifx_reflection.ObjectKind.TABLE,
        scope=ifx_reflection.ObjectScope.DEFAULT,
        **kw,
    ):
        """Reflect multiple tables and normalize every column default."""
        reflected = self._reflector.get_multi_columns(
            connection,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

        def normalize_columns(columns):
            return [
                _normalize_ifx_reflected_column(column)
                for column in columns
            ]

        # Defensive compatibility: custom reflectors can return a mapping,
        # although SQLAlchemy normally expects an iterable of key/value pairs.
        if isinstance(reflected, dict):
            return {
                table_key: normalize_columns(columns)
                for table_key, columns in reflected.items()
            }

        return [
            (
                table_key,
                normalize_columns(columns),
            )
            for table_key, columns in reflected
        ]

    def get_multi_pk_constraint(
        self,
        connection,
        *,
        schema=None,
        filter_names=None,
        kind=ifx_reflection.ObjectKind.TABLE,
        scope=ifx_reflection.ObjectScope.DEFAULT,
        **kw,
    ):
        return self._reflector.get_multi_pk_constraint(
            connection,
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
        kind=ifx_reflection.ObjectKind.TABLE,
        scope=ifx_reflection.ObjectScope.DEFAULT,
        **kw,
    ):
        return self._reflector.get_multi_foreign_keys(
            connection,
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
        kind=ifx_reflection.ObjectKind.TABLE,
        scope=ifx_reflection.ObjectScope.DEFAULT,
        **kw,
    ):
        return self._reflector.get_multi_indexes(
            connection,
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
        kind=ifx_reflection.ObjectKind.TABLE,
        scope=ifx_reflection.ObjectScope.DEFAULT,
        **kw,
    ):
        return self._reflector.get_multi_unique_constraints(
            connection,
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
        kind=ifx_reflection.ObjectKind.TABLE,
        scope=ifx_reflection.ObjectScope.DEFAULT,
        **kw,
    ):
        return self._reflector.get_multi_check_constraints(
            connection,
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
        kind=ifx_reflection.ObjectKind.TABLE,
        scope=ifx_reflection.ObjectScope.DEFAULT,
        **kw,
    ):
        return self._reflector.get_multi_table_comment(
            connection,
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
        kind=ifx_reflection.ObjectKind.TABLE,
        scope=ifx_reflection.ObjectScope.DEFAULT,
        **kw,
    ):
        return self._reflector.get_multi_table_options(
            connection,
            schema=schema,
            filter_names=filter_names,
            kind=kind,
            scope=scope,
            **kw,
        )

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_check_constraints(
                                connection, table_name, schema=schema, **kw)

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_table_comment(
                                connection, table_name, schema=schema, **kw)

    def get_table_options(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_table_options(
                                connection, table_name, schema=schema, **kw)


# legacy naming
IFX_DBCompiler = IfxCompiler
IFX_DBDDLCompiler = IfxDDLCompiler
IFX_DBIdentifierPreparer = IfxIdentifierPreparer
IFX_DBExecutionContext = IfxExecutionContext
IFX_DBDialect = IfxDialect

dialect = IfxDialect
