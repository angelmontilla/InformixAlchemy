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
from sqlalchemy import exc
from sqlalchemy import types as sa_types
from sqlalchemy import sql
from sqlalchemy import schema as sa_schema
from sqlalchemy import util
from sqlalchemy.sql import compiler
from sqlalchemy.sql import operators
from sqlalchemy.sql import util as sql_util
from sqlalchemy.engine import default
from . import reflection as ifx_reflection
from . import sqla_compat

from sqlalchemy.types import BLOB, CHAR, CLOB, DATE, DATETIME, INTEGER,\
    SMALLINT, BIGINT, DECIMAL, NUMERIC, REAL, TIME, TIMESTAMP,\
    VARCHAR, FLOAT

_IFX_SINGLE_ROW_FROM = " FROM systables WHERE tabid = 1"
_IFX_LASTROWID_DBINFO_BY_TYPE = {
    "BIGSERIAL": "bigserial",
    "SERIAL8": "serial8",
}

# as documented from:
RESERVED_WORDS = set(
   ['activate', 'disallow', 'locale', 'result', 'add', 'disconnect', 'localtime',
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
    'regr_count', 'within', 'asc'])


class _IFX_Boolean(sa_types.Boolean):

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            else:
                return bool(value)
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            elif bool(value):
                return '1'
            else:
                return '0'
        return process


class _IFX_Date(sa_types.Date):

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return value
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return str(value)
        return process


class DOUBLE(sa_types.Numeric):
    __visit_name__ = 'DOUBLE'


class LONGVARCHAR(sa_types.VARCHAR):
    __visit_name__ = 'LONGVARCHAR'


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


colspecs = {
    sa_types.Boolean: _IFX_Boolean,
    sa_types.Date: _IFX_Date
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
    'LONGVARCHAR': LONGVARCHAR,
    'XML': XML,
    'GRAPHIC': GRAPHIC,
    'VARGRAPHIC': VARGRAPHIC,
    'LONGVARGRAPHIC': LONGVARGRAPHIC,
    'DBCLOB': DBCLOB,
    'BOOLEAN': _IFX_Boolean,
    'BYTE': sa_types.LargeBinary,
    'TEXT': sa_types.Text,
    'LVARCHAR': VARCHAR,
}


class IfxTypeCompiler(compiler.GenericTypeCompiler):

    def visit_TIMESTAMP(self, type_):
        return "TIMESTAMP"

    def visit_DATE(self, type_):
        return "DATE"

    def visit_TIME(self, type_):
        return "TIME"

    def visit_DATETIME(self, type_):
        return "DATETIME YEAR TO SECOND"

    def visit_SMALLINT(self, type_):
        return "SMALLINT"

    def visit_INT(self, type_):
        return "INTEGER"

    def visit_BIGINT(self, type_):
        return "BIGINT"

    def visit_SERIAL(self, type_):
        return "SERIAL"

    def visit_SERIAL8(self, type_):
        return "SERIAL8"

    def visit_BIGSERIAL(self, type_):
        return "BIGSERIAL"

    def visit_FLOAT(self, type_):
        return "FLOAT" if type_.precision is None else \
                "FLOAT(%(precision)s)" % {'precision': type_.precision}

    def visit_DOUBLE(self, type_):
        return "DOUBLE"

    def visit_XML(self, type_):
        return "XML"

    def visit_CLOB(self, type_):
        return "CLOB"

    def visit_BLOB(self, type_):
        # Informix accepts BLOB in DDL, while the legacy BLOB(1M) form
        # raises -201 on the target server used by the SQLAlchemy suite.
        return "BLOB"

    def visit_DBCLOB(self, type_):
        return "DBCLOB"

    def _require_length(self, type_, type_name):
        if type_.length in (None, 0):
            raise exc.CompileError(
                "Informix %s requires an explicit length" % type_name
            )
        return type_.length

    def visit_VARCHAR(self, type_):
        length = self._require_length(type_, "VARCHAR")
        return "VARCHAR(%(length)s)" % {"length": length}

    def visit_LONGVARCHAR(self, type_):
        return "LONG VARCHAR"

    def visit_VARGRAPHIC(self, type_):
        length = self._require_length(type_, "VARGRAPHIC")
        return "VARGRAPHIC(%(length)s)" % {"length": length}

    def visit_LONGVARGRAPHIC(self, type_):
        return "LONG VARGRAPHIC"

    def visit_CHAR(self, type_):
        return "CHAR" if type_.length in (None, 0) else \
                "CHAR(%(length)s)" % {'length': type_.length}

    def visit_GRAPHIC(self, type_):
        return "GRAPHIC" if type_.length in (None, 0) else \
                "GRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_DECIMAL(self, type_):
        if not type_.precision:
            return "DECIMAL(31, 0)"
        elif not type_.scale:
            return "DECIMAL(%(precision)s, 0)" % {'precision': type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                            'precision': type_.precision, 'scale': type_.scale}

    def visit_numeric(self, type_):
        return self.visit_DECIMAL(type_)

    def visit_datetime(self, type_):
        return self.visit_DATETIME(type_)

    def visit_date(self, type_):
        return self.visit_DATE(type_)

    def visit_time(self, type_):
        return self.visit_TIME(type_)

    def visit_integer(self, type_):
        return self.visit_INT(type_)

    def visit_boolean(self, type_):
        return self.visit_SMALLINT(type_)

    def visit_float(self, type_):
        return self.visit_FLOAT(type_)

    def visit_unicode(self, type_):
        return self.visit_VARGRAPHIC(type_)

    def visit_unicode_text(self, type_):
        return self.visit_LONGVARGRAPHIC(type_)

    def visit_string(self, type_):
        return self.visit_VARCHAR(type_)

    def visit_TEXT(self, type_):
        return self.visit_CLOB(type_)

    def visit_large_binary(self, type_):
        return "BYTE"


class IfxCompiler(compiler.SQLCompiler):
    def visit_false(self, expr, **kw):
        return '0'

    def visit_true(self, expr, **kw):
        return '1'

    def get_cte_preamble(self, recursive):
        return "WITH"

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

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

    def _ifx_limit_fetch_value(self, select, clause):
        if clause is None:
            return None

        if sqla_compat.get_fetch_clause(select) is clause:
            return sqla_compat.offset_or_limit_clause_asint(
                select, clause, "fetch"
            )

        return sqla_compat.get_limit_value(select)

    def _row_limit_expression(self, select, clause, value):
        if clause is None:
            return None

        if sqla_compat.simple_int_clause(select, clause):
            return sql.literal_column(str(value))

        return clause

    def _row_limit_upper_bound(self, select, limit_clause, offset_clause):
        limit_value = self._ifx_limit_fetch_value(select, limit_clause)
        offset_value = sqla_compat.get_offset_value(select)

        if (
            sqla_compat.simple_int_clause(select, limit_clause)
            and sqla_compat.simple_int_clause(select, offset_clause)
        ):
            return sql.literal_column(str(limit_value + offset_value))

        return (
            self._row_limit_expression(select, limit_clause, limit_value)
            + self._row_limit_expression(select, offset_clause, offset_value)
        )

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
            adapter.traverse(elem) for elem in order_by_clauses
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

    def _translate_offset_select(self, select):
        limit_clause = self._ifx_limit_fetch_clause(select)
        offset_clause = sqla_compat.get_offset_clause(select)

        if offset_clause is None:
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

        if not (
            sqla_compat.simple_int_clause(select, offset_clause)
            and sqla_compat.get_offset_value(select) == 0
        ):
            paged = paged.where(
                row_number_col
                > self._row_limit_expression(
                    select,
                    offset_clause,
                    sqla_compat.get_offset_value(select),
                )
            )

        if limit_clause is not None:
            paged = paged.where(
                row_number_col
                <= self._row_limit_upper_bound(
                    select, limit_clause, offset_clause
                )
            )

        return paged

    def limit_clause(self, select,**kwargs):
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

    def visit_sequence(self, sequence, **kw):
        return "%s.NEXTVAL" % self.preparer.format_sequence(sequence)

    def default_from(self):
        return _IFX_SINGLE_ROW_FROM

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
    # TODO: this is wrong but need to know what Informix is expecting here
    #    if func.name.upper() == "LENGTH":
    #        return "LENGTH('%s')" % func.compile().params[func.name + '_1']
    #    else:
    #        return compiler.SQLCompiler.visit_function(self, func, **kwargs)


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
        limit_value = self._ifx_limit_fetch_value(select, limit_clause)

        # Informix: SELECT FIRST n DISTINCT ...
        if (limit_clause is not None) and (
            sqla_compat.get_offset_clause(select) is None
        ):
            if sqla_compat.simple_int_clause(select, limit_clause):
                text += "FIRST %s " % limit_value
            else:
                text += "FIRST %s " % self.process(limit_clause, **kwargs)

        distinct = sqla_compat.get_distinct(select)
        if isinstance(distinct, str):
            text += distinct.upper() + " "
        elif distinct:
            text += "DISTINCT "

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
            return "CASE WHEN " + usql + " THEN 1 ELSE 0 END"

        return usql

class IfxDDLCompiler(compiler.DDLCompiler):

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
           if dbms_name != None and (dbms_name.find('Informix/') != -1):
                return self.get_server_version_info(dialect) >= [10, 5]
        else:
            return False

    def get_column_specification(self, column, **kw):
        col_spec = [self.preparer.format_column(column)]
        rendered_type = self.dialect.type_compiler.process(
            column.type,
            type_expression=column
        )

        autoincrement_type_name = _get_ifx_autoincrement_type_name(column)
        if autoincrement_type_name == "SERIAL":
            rendered_type = "SERIAL"
        elif autoincrement_type_name == "SERIAL8":
            rendered_type = "SERIAL8"
        elif autoincrement_type_name == "BIGSERIAL":
            rendered_type = "BIGSERIAL"

        col_spec.append(rendered_type)


        # column-options: "NOT NULL"
        if not column.nullable or column.primary_key:
            col_spec.append('NOT NULL')

        # default-clause:
        default = self.get_column_default_string(column)
        if default is not None:
            col_spec.append('WITH DEFAULT')
            col_spec.append(default)

        column_spec = ' '.join(col_spec)
        return column_spec

    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        if constraint.onupdate is not None:
            util.warn(
                "Informix does not support UPDATE CASCADE for foreign keys.")

        return text

    def _define_constraint_name_postfix(self, constraint):
        if constraint.name is None:
            return ""

        formatted_name = self.preparer.format_constraint(constraint)
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

    def visit_drop_constraint(self, drop, **kw):
        constraint = drop.element
        if isinstance(constraint, sa_schema.ForeignKeyConstraint):
            qual = "FOREIGN KEY "
            const = self.preparer.format_constraint(constraint)
        elif isinstance(constraint, sa_schema.PrimaryKeyConstraint):
            qual = "PRIMARY KEY "
            const = ""
        elif isinstance(constraint, sa_schema.UniqueConstraint):
            qual = "UNIQUE "
            if self._is_nullable_unique_constraint_supported(self.dialect):
                for column in constraint:
                    if column.nullable:
                        constraint.uConstraint_as_index = True
                if getattr(constraint, 'uConstraint_as_index', None):
                    qual = "INDEX "
            const = self.preparer.format_constraint(constraint)
        else:
            qual = ""
            const = self.preparer.format_constraint(constraint)

        if hasattr(constraint, 'uConstraint_as_index') and constraint.uConstraint_as_index:
            return "DROP %s%s" % \
                                (qual, const)
        return "ALTER TABLE %s DROP %s%s" % \
                                (self.preparer.format_table(constraint.table),
                                qual, const)

    def create_table_constraints(self, table, **kw):
        if self._is_nullable_unique_constraint_supported(self.dialect):
            for constraint in sqla_compat.get_table_sorted_constraints(table):
                if isinstance(constraint, sa_schema.UniqueConstraint):
                    for column in constraint:
                        if column.nullable:
                            constraint.use_alter = True
                            constraint.uConstraint_as_index = True
                            break
                    if getattr(constraint, 'uConstraint_as_index', None):
                        if not constraint.name:
                            index_name = "%s_%s_%s" % ('ukey', self.preparer.format_table(constraint.table), '_'.join(column.name for column in constraint))
                        else:
                            index_name = constraint.name
                        index = sa_schema.Index(index_name, *(column for column in constraint))
                        index.unique = True
                        index.uConstraint_as_index = True
        result = super( IfxDDLCompiler, self ).create_table_constraints(table, **kw)
        return result

    def visit_create_index(
        self, create, include_schema=False, include_table_schema=True, **kw
    ):
        sql = super( IfxDDLCompiler, self ).visit_create_index(
            create,
            include_schema=include_schema,
            include_table_schema=include_table_schema,
            **kw
        )
        if getattr(create.element, 'uConstraint_as_index', None):
            sql += ' EXCLUDE NULL KEYS'
        return sql

    def visit_add_constraint(self, create, **kw):
        if self._is_nullable_unique_constraint_supported(self.dialect):
            if isinstance(create.element, sa_schema.UniqueConstraint):
                for column in create.element:
                    if column.nullable:
                        create.element.uConstraint_as_index = True
                        break
                if getattr(create.element, 'uConstraint_as_index', None):
                    if not create.element.name:
                        index_name = "%s_%s_%s" % ('uk_index', self.preparer.format_table(create.element.table), '_'.join(column.name for column in create.element))
                    else:
                        index_name = create.element.name
                    index = sa_schema.Index(index_name, *(column for column in create.element))
                    index.unique = True
                    index.uConstraint_as_index = True
                    sql = self.visit_create_index(sa_schema.CreateIndex(index))
                    return sql
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
        return self._execute_scalar(
            "SELECT "
            + self.dialect.identifier_preparer.format_sequence(seq)
            + ".NEXTVAL"
            + _IFX_SINGLE_ROW_FROM,
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

        if self.isinsert:
            tbl = self._get_lastrowid_dml_table()
            if tbl is None:
                return

            seq_column = sqla_compat.get_table_autoincrement_column(tbl)
            insert_has_sequence = seq_column is not None
            compiled_params = (
                self.compiled_parameters[0] if self.compiled_parameters else {}
            )
            explicit_pk_value = (
                seq_column is not None
                and compiled_params.get(seq_column.key) is not None
            )

            self._select_lastrowid = insert_has_sequence and \
                                        not explicit_pk_value and \
                                        not self._ifx_dml_returns_rows() and \
                                        not self.compiled.inline and \
                                        not self.executemany
            if self._select_lastrowid:
                self._lastrowid_query = _get_ifx_lastrowid_query(seq_column)

    def post_exec(self):
        if self._select_lastrowid and self._lastrowid_query:
            self.cursor.execute(self._lastrowid_query)
            row = self.cursor.fetchone()
            if row is None:
                return
            if row[0] is not None:
                self._lastrowid = int(row[0])


class IfxDialect(default.DefaultDialect):

    name = 'informix'
    max_identifier_length = 128
    encoding = 'utf-8'
    default_paramstyle = 'qmark'
    colspecs = colspecs
    ischema_names = ischema_names
    supports_char_length = False
    supports_unicode_statements = False
    supports_unicode_binds = False
    returns_unicode_strings = False
    postfetch_lastrowid = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_decimal = False
    supports_native_boolean = False
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
    # Keep disabled until the compiler's LIMIT/OFFSET/FETCH rewrites and
    # Informix-specific DDL paths are validated with SQLAlchemy's statement
    # cache test coverage.
    supports_statement_cache = False

    two_phase_transactions = False
    savepoints = True

    statement_compiler = IfxCompiler
    ddl_compiler = IfxDDLCompiler
    type_compiler = IfxTypeCompiler
    preparer = IfxIdentifierPreparer
    execution_ctx_cls = IfxExecutionContext

    _reflector_cls = ifx_reflection.IfxReflector

    def __init__(self, **kw):
        super(IfxDialect, self).__init__(**kw)

        self._reflector = self._reflector_cls(self)

    # reflection: these all defer to an BaseIfxReflector
    # object which selects between Informix and AS/400 schemas
    def initialize(self, connection):
        super(IfxDialect, self).initialize(connection)
        self.dbms_ver = getattr(connection.connection, 'dbms_ver', None)
        self.dbms_name = getattr(connection.connection, 'dbms_name', None)

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

    def get_columns(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_columns(
                                connection, table_name, schema=schema, **kw)

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
        return self._reflector.get_multi_columns(
            connection,
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
