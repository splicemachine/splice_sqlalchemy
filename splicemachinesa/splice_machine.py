from .base import SpliceMachineExecutionContext, SpliceMachineDialect
from sqlalchemy import processors, types as sa_types, util
from sqlalchemy import __version__ as SA_Version
from sqlalchemy.exc import ArgumentError

"""
This file is part of Splice Machine.
Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
GNU Affero General Public License as published by the Free Software Foundation, either
version 3, or (at your option) any later version.
Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License along with Splice Machine.
If not, see <http://www.gnu.org/licenses/>.

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
and are licensed to you under the GNU Affero General Public License.
"""



SA_Version = [int(ver_token) for ver_token in SA_Version.split('.')[0:2]]
SQL_TXN_READ_UNCOMMITTED = 1
SQL_TXN_READ_COMMITTED = 2
SQL_TXN_REPEATABLE_READ = 4
SQL_TXN_SERIALIZABLE = 8
SQL_ATTR_TXN_ISOLATION = 108

if SA_Version < [0, 8]:
    from sqlalchemy.engine import base
else:
    from sqlalchemy.engine import result as _result


class _SM_Numeric(sa_types.Numeric):
    """
    Splice Machine Numeric Type
    that overrides original type
    """

    def result_processor(self, dialect, coltype):
        """
        Processing queried numeric types
        :param dialect: currentdialect:
        :param coltype: the column type to extract
        :returns: float value of decimal in db
        """
        if self.asdecimal:
            return None
        else:
            return processors.to_float


class SpliceMachineExecutionContext_sm(SpliceMachineExecutionContext):
    _callproc_result = None
    _out_parameters = None

    def get_lastrowid(self):
        """
        Get the last value from a sequence
        """
        return self.cursor.last_identity_val

    def pre_exec(self):
        """
        ff a single execute, check for outparams
        """
        if len(self.compiled_parameters) == 1:
            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    self._out_parameters = True
                    break

    def get_result_proxy(self):
        """
        Get results from a SQL Query
        :returns: the result from the query
        """
        if self._callproc_result and self._out_parameters:
            if SA_Version < [0, 8]:
                result = base.ResultProxy(self)
            else:
                result = _result.ResultProxy(self)
            result.out_parameters = {}

            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    name = self.compiled.bind_names[bindparam]
                    result.out_parameters[name] = self._callproc_result[self.compiled.positiontup.index(name)]

            return result
        else:
            if SA_Version < [0, 8]:
                result = base.ResultProxy(self)
            else:
                result = _result.ResultProxy(self)
            return result


class SpliceMachineDialect_sm(SpliceMachineDialect):
    driver = 'splicemachinesa'
    supports_unicode_statements = False
    supports_unicode_binds = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_native_decimal = False
    supports_native_enum = False
    supports_char_length = True
    supports_default_values = False
    supports_multivalues_insert = True
    execution_ctx_cls = SpliceMachineExecutionContext_sm

    colspecs = util.update_copy(
        SpliceMachineDialect.colspecs,
        {
            sa_types.Numeric: _SM_Numeric
        }
    )

    def do_execute(self, cursor, statement, parameters, context=None):
        """
        Execute a given statement in SQL on our cursor
        :param cursor: ODBC cursor object (pyODBC)
        :param statement: SQL Statement to execute
        :param parameters: parameters for the pyODBC cursor
        :param context: additional info for query resolving
        """
        if context and context._out_parameters:
            statement = statement.split('(', 1)[0].split()[1]
            context._callproc_result = cursor.callproc(statement, parameters)
        else:
            cursor.execute(statement, parameters)

    def _get_server_version_info(self, connection):
        """
        Get information about the db server (Splice Machine)
        from ODBC connection
        :param connection: ODBC cnxn
        """
        return connection.connection.server_info()

    _isolation_lookup = {'READ STABILITY', 'RS', 'UNCOMMITTED READ', 'UR', 'CURSOR STABILITY', 'CS', 'REPEATABLE READ',
                         'RR'}

    _isolation_levels_cli = {'RR': SQL_TXN_SERIALIZABLE, 'REPEATABLE READ': SQL_TXN_SERIALIZABLE,
                             'UR': SQL_TXN_READ_UNCOMMITTED, 'UNCOMMITTED READ': SQL_TXN_READ_UNCOMMITTED,
                             'RS': SQL_TXN_REPEATABLE_READ, 'READ STABILITY': SQL_TXN_REPEATABLE_READ,
                             'CS': SQL_TXN_READ_COMMITTED, 'CURSOR STABILITY': SQL_TXN_READ_COMMITTED}

    _isolation_levels_returned = {value: key for key, value in _isolation_levels_cli.items()}

    def _get_cli_isolation_levels(self, level):
        """
        Get the isolation level
        :param level: the isolation level to retrieve
        :returns: the isolation level cli data
        """
        return _isolation_levels_cli[level]

    def set_isolation_level(self, connection, level):
        """
        Set the connection to a given isolation level
        :param connection: pyODBC connection
        :param level: the new level to set isolation to
        """
        if level is None:
            level = 'CS'
        else:
            if len(level.strip()) < 1:
                level = 'CS'
        level.upper().replace("-", " ")
        if level not in self._isolation_lookup:
            raise ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s" %
                (level, self.name, ", ".join(self._isolation_lookup))
            )
        attrib = {SQL_ATTR_TXN_ISOLATION: _get_cli_isolation_levels(self, level)}
        res = connection.set_option(attrib)

    def get_isolation_level(self, connection):
        """
        Get the current isolation level
        :param connection: pyODBC cnxn to database
        """
        attrib = SQL_ATTR_TXN_ISOLATION
        res = connection.get_option(attrib)

        val = self._isolation_levels_returned[res]
        return val

    def reset_isolation_level(self, connection):
        """
        Set the isolation level to default 'CS'
        :param connection: pyODBC connection
        """
        self.set_isolation_level(connection, 'CS')

    def _get_default_schema_name(self, connection):
        """
        Retrieves current schema for the specified connection object
        :param connection: ODBC cnxn
        :returns: current schema (default)
        """
        return self.normalize_name(connection.connection.get_current_schema())

    def is_disconnect(self, ex, connection, cursor):
        """
        Checks if the DB_API driver error indicates an invalid connection
        :param ex: the error message
        :param connection: active ODBC cnxn
        :param cursor: active ODBC cursor
        :returns: whether or not a given exception is a disconnect
        """

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


dialect = SpliceMachineDialect_sm
