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

import re
from .base import IfxExecutionContext, IfxDialect
from sqlalchemy import types as sa_types, util
from sqlalchemy.engine import processors
from sqlalchemy.exc import ArgumentError
SQL_TXN_READ_UNCOMMITTED = 1
SQL_TXN_READ_COMMITTED = 2
SQL_TXN_REPEATABLE_READ = 4
SQL_TXN_SERIALIZABLE = 8
SQL_ATTR_TXN_ISOLATION = 108

VERSION_RE = re.compile(r'(\d+)\.(\d+)(.+\d+)')

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

    driver = 'IfxAlchemy'
    supports_unicode_statements = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_native_decimal = False
    supports_char_length = True
    supports_default_values = False
    supports_multivalues_insert = True
    supports_statement_cache = False
    execution_ctx_cls = IfxExecutionContext_IfxPy

    colspecs = util.update_copy(
        IfxDialect.colspecs,
        {
            sa_types.Numeric: _IFX_Numeric_IfxPy
        }
    )

    @classmethod
    def import_dbapi(cls):
        """Return the underlying DBAPI driver module."""
        import IfxPyDbi as module
        return module

    dbapi = import_dbapi

    def do_execute(self, cursor, statement, parameters, context=None):
        if context and context._out_parameters:
            statement = statement.split('(', 1)[0].split()[1]
            context._callproc_result = cursor.callproc(statement, parameters)
        else:
            cursor.execute(statement, parameters)


    _isolation_lookup = set(['READ STABILITY','RS', 'UNCOMMITTED READ','UR',
                             'CURSOR STABILITY','CS', 'REPEATABLE READ','RR'])

    _isolation_levels_cli = {'RR': SQL_TXN_SERIALIZABLE, 'REPEATABLE READ': SQL_TXN_SERIALIZABLE,
                            'UR': SQL_TXN_READ_UNCOMMITTED, 'UNCOMMITTED READ': SQL_TXN_READ_UNCOMMITTED,
                             'RS': SQL_TXN_REPEATABLE_READ, 'READ STABILITY': SQL_TXN_REPEATABLE_READ,
                             'CS': SQL_TXN_READ_COMMITTED, 'CURSOR STABILITY': SQL_TXN_READ_COMMITTED }

    _isolation_levels_returned = { value : key for key, value in _isolation_levels_cli.items()}

    def _get_cli_isolation_levels(self, level):
        return _isolation_levels_cli[level]

    def set_isolation_level(self, connection, level):
        if level is  None:
         level ='CS'
        else :
          if len(level.strip()) < 1:
            level ='CS'
        level.upper().replace("-", " ")
        if level not in self._isolation_lookup:
            raise ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s" %
                (level, self.name, ", ".join(self._isolation_lookup))
            )
        attrib = {SQL_ATTR_TXN_ISOLATION:_get_cli_isolation_levels(self,level)}
        res = connection.set_option(attrib)

    def reset_isolation_level(self, connection):
        self.set_isolation_level(connection,'CS')

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='uid', password='pwd',
                host='server', port='service') # Are these safe renames?
        connstr = ";".join(['%s=%s' % (k.upper(), v) for k, v in opts.items()])
        opt = {}

        return ([connstr], opt)

        #check for SSL arguments
        ssl_keys = ['Security', 'SSLClientKeystoredb', 'SSLClientKeystash','SSLServerCertificate']
        query_keys = url.query.keys()
        for key in ssl_keys:
             for query_key in query_keys:
                 if query_key.lower() == key.lower():
                     dsn_param.append('%(ssl_key)s=%(value)s' % {'ssl_key': key, 'value': url.query[query_key]})
                     del url.query[query_key]
                     break

        dsn = ';'.join(dsn_param)
        dsn += ';'
        return ((dsn, url.username, '', '', ''), {})

    # Retrieves current schema for the specified connection object
    def _get_default_schema_name(self, connection):
        return self.normalize_name(connection.connection.get_current_schema())

    def _get_server_version_info(self, connection):
        v = VERSION_RE.split(connection.connection.dbms_ver)
        return (int(v[1]), int(v[2]), v[3])


    # Checks if the DB_API driver error indicates an invalid connection
    def is_disconnect(self, ex, connection, cursor):
        if isinstance(ex, (self.dbapi.ProgrammingError,
                                             self.dbapi.OperationalError)):
            connection_errors = ('Connection is not active', 'connection is no longer active',
                                    'Connection Resource cannot be found', 'SQL30081N'
                                    'CLI0108E', 'CLI0106E', 'SQL1224N')
            for err_msg in connection_errors:
                if err_msg in str(ex):
                    return True
        else:
            return False

dialect = IfxDialect_IfxPy
