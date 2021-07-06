from __future__ import unicode_literals

import datetime
import re
import sys

from sqlalchemy import schema as sa_schema
from sqlalchemy import types as sa_types
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.sql import operators, compiler
from sqlalchemy.types import BLOB, CHAR, CLOB, DATE, DATETIME, INTEGER, \
    SMALLINT, BIGINT, DECIMAL, NUMERIC, REAL, TIME, TIMESTAMP, \
    VARCHAR, FLOAT, TEXT, INT
from sqlalchemy.sql.elements import TextClause
from enum import Enum as PyEnum
from . import constants
from . import reflection as sm_reflection

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



########################################
#                                      #
#    Find if Python is Version 3       #
#                                      #
########################################

IS_PYTHON_3 = sys.version_info[0] >= 3


########################################
#                                      #
#   Custom Splice Machine Data Types   #
#                                      #
########################################

class _SM_Integer(sa_types.Integer):
    """
    Overrided integer data type
    that makes sure that types passed
    in are integers before being binded
    """

    def bind_processor(self, dialect):
        """
        Returns a conversion function
        that converts input values to
        Python ints before binding
        :param dialect: the current dialect
            in use
        :returns: conversion function
        """

        def process(value):
            return None if value is None else int(value)

        return process

class _SM_Boolean(sa_types.Boolean):
    """
    An overrided boolean class specifically
    for Splice Machine that converts Boolean
    SQLAlchemy types into SMALLINT (0/1) values. This is because SpliceDB doesn't support using 0 and 1
    as true/false, which sqlalchemy generates for boolean types.
    """

    def result_processor(self, dialect, coltype):
        """
        Return a conversion function for
        processing row values
        :param dialect: the current dialect in use
        :param coltype: the column type in DB Schema
            from pyODBC.connection.cursor.description
        :returns: func for parsing boolean value of integer
        """

        def process(value):
            if value is None:  # null
                return None
            else:  # bool(0) -> false, bool(1) -> true
                return bool(value)

        return process

    def bind_processor(self, dialect):
        """
        Return a conversion function for processing bind values
        (when sending, rather than receiving)
        :param dialect: the current dialect in use
        :returns: func for getting
            integer value from boolean specified
        """

        def process(value):
            return None if value is None else int(bool(value))

        return process

class _SM_String(sa_types.String):
    """
    Overrided String class for
    unicode + posix conversions (MLFlow
    puts these types into SQLAlchemy
    frequently and they don't render in
    VARCHAR types properly)
    """

    def __init__(self, *args, **kwargs):
        if '_enums' in kwargs:
            kwargs.pop('_enums')
            kwargs['_expect_unicode'] = False
        super().__init__(*args, **kwargs)

    def bind_processor(self, dialect):
        """
        Return a conversion function``
        that transforms into
        Strings
        :param dialect: the current dialect
            in use
        :returns: conversion function
        """

        def process(value):
            if isinstance(value, PyEnum):
                value = value._name_
            return None if value is None else bytes(value.encode('utf-8'))

        # we use bytes type for python3 backwards compatibility
        return process


class _SM_Date(sa_types.Date):
    """
    An overrided date class specifically
    for Splice Machine that converts
    datetimes into date objects
    """

    def result_processor(self, dialect, coltype):
        """
        Return a conversion function for
        processing row values
        :param dialect: the current dialect in use
        :param coltype: the column type in DB Schema
            from pyODBC.connection.cursor.description

        :returns: func for parsing datetime value of integer
        """

        def process(value):
            if value is None:
                return None  # null
            if isinstance(value, datetime.datetime):  # convert string
                # datetime to date
                value = datetime.date(value.year, value.month, value.day)
            return value

        return process

    def bind_processor(self, dialect):
        """
        Return a conversion function for processing bind values
        (when sending, rather than receiving)
        :param dialect: the current dialect in use
        :returns: func for getting
            string value from datetime when we insert
        """

        def process(value):
            if value is None:
                return None  # null
            if isinstance(value, datetime.datetime):  # can be parsed?
                value = datetime.date(value.year, value.month, value.day)
            out = bytes(value.encode('utf-8'))  # stringify
            return out

        return process


# Mapping for our overrided functions to the original ones

colspecs = {
    sa_types.Date: _SM_Date,
    sa_types.Integer: _SM_Integer,
    sa_types.String: _SM_String,
    sa_types.Boolean: _SM_Boolean
}


########################################
#                                      #
#     Data Types Specific To Splice    #
#                                      #
########################################

class LONGVARCHAR(sa_types.VARCHAR):
    """
    Long varchar data type -- same
    as a varchar, but without max length;
    always 32,700 characters
    """
    __visit_name_ = 'LONGVARCHAR'


# supported types on Splice Machine
ischema_names = {
    'BLOB': BLOB,
    'CHAR': CHAR,
    'CHARACTER': CHAR,
    'CLOB': CLOB,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'INTEGER': INTEGER,
    'SMALLINT': SMALLINT,
    'BIGINT': BIGINT,
    'DECIMAL': DECIMAL,
    'NUMERIC': NUMERIC,
    'REAL': REAL,
    'DOUBLE': FLOAT,
    'FLOAT': FLOAT,
    'TIME': TIME,
    'TIMESTAMP': TIMESTAMP,
    'VARCHAR': VARCHAR,
    'LONGVARCHAR': LONGVARCHAR,
    'TEXT': TEXT,
    'TINYINT':SMALLINT
}


class TypeRegexes:
    """
    Regexes for matching
    default type converters
    """
    # numeric types
    NUM_RX = re.compile('|'.join(['INT', 'DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE', 'FLOAT']))
    # string types
    STR_RX = re.compile('|'.join(['BLOB', 'CLOB', 'CHAR', 'CHARACTER', 'DATE', 'DATETIME',
                                  'TIME', 'TIMESTAMP', 'VARCHAR', 'LONGVARCHAR']))


class QuotationUtilities:
    """
    Utilities for quotation
    """

    @staticmethod
    def quote(identifier):
        """
        Utility function that takes in
        a string identifier and returns
        the string wrapped in quotes

        :param identifier: string to surround
        :returns: string surrounded with quotes
        """
        return '"{identifier}"'.format(identifier=identifier)

    @staticmethod
    def check_and_quote(identifier):
        """
        Utility function to quote a string
        if it isn't quoted already

        :param identifier: the string to check+quote
        :returns: the string quoted if not already
        """
        quotes = ('"', "'")
        if identifier[0] in quotes and identifier[-1] in quotes:
            return identifier
        return QuotationUtilities.quote(identifier)

    @staticmethod
    def dequote(identifier):
        """
        Remove the quotes from an identifier
        if they exist
        :param identifier: the string to remove quotes
            from beggining and end
        :returns: dequoted identifier
        """
        return identifier.strip('"').strip("'")

    @staticmethod
    def get_default_type_converter(column_type_string):
        """
        Get a function that can convert the default
        argument of a column to its appropriate type
        :param column_type_string: string version
            of column type object
        """

        if TypeRegexes.STR_RX.search(column_type_string):
            return QuotationUtilities.check_and_quote
        return QuotationUtilities.dequote

    @staticmethod
    def conditionally_reserved_quote(identifier):
        """
        Quote an identifier if it appears inside
        the reserved words array
        :param identifier: the identifier to check + quote
        :returns: the string quoted if it is reserved
        """
        if identifier in constants.RESERVED_WORDS:
            return QuotationUtilities.check_and_quote(identifier)
        return identifier


########################################
#                                      #
#     Splice Machine Type Compiler     #
#                                      #
########################################

class SpliceMachineTypeCompiler(compiler.GenericTypeCompiler):
    """
    Splice Machine SQL Dialect Specific
    Type compiler for SQLAlchemy--
    it contains custom rendering of
    types for our DB, so that they don't throw
    errors when used in SQL
    """

    def visit_TIMESTAMP(self, type_):
        """
        Timestamp rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "TIMESTAMP"

    def visit_DATE(self, type_):
        """
        Date rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "DATE"

    def visit_TIME(self, type_):
        """
        Time rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "TIME"

    def visit_DATETIME(self, type_):
        """
        datetime rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_TIMESTAMP(type_)
        # this calls the super class because,
        # although it is still a generic
        # data type not specific to Splice,
        # it requires additional parsing

    def visit_SMALLINT(self, type_):
        """
        Smallint rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "SMALLINT"

    def visit_INT(self, type_):
        """
        INT rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "INT"

    def visit_BIGINT(self, type_):
        """
        Big int rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "BIGINT"

    def visit_FLOAT(self, type_):
        """
        Float rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "FLOAT" if type_.precision is None else \
            "FLOAT(%(precision)s)" % {'precision': type_.precision}
        # don't use function precision e.g. FLOAT(3) if no precision
        # is specified

    def visit_DOUBLE(self, type_):
        """
        Double rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "DOUBLE"

    def visit_CLOB(self, type_):
        """
        Clob (character large object) rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "CLOB"

    def visit_BLOB(self, type_):
        """
        Blob (binary large object) rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "BLOB(1M)" if type_.length in (None, 0) else \
            "BLOB(%(length)s)" % {'length': type_.length}
        # use function with size if specified

    def visit_VARCHAR(self, type_):
        """
        varchar rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        # convert to text for unicode
        return "VARCHAR(500)" if type_.length in (None, 0) else \
            "VARCHAR(%(length)s)" % {'length': type_.length}
        # TODO: a VARCHAR without a length specification may need to be a CLOB

    def visit_LONGVARCHAR(self, type_):
        """
        long varchar rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "LONG VARCHAR"

    def visit_CHAR(self, type_):
        """
        char rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return "CHAR" if type_.length in (None, 0) else \
            "CHAR(%(length)s)" % {'length': type_.length}

    def visit_DECIMAL(self, type_):
        """
        decimal rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        if not type_.precision:  # nothing specified
            return "DECIMAL(31, 0)"
        elif not type_.scale:  # precision but not scale
            return "DECIMAL(%(precision)s, 0)" % {'precision': type_.precision}
        else:  # both precision and scale
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                'precision': type_.precision, 'scale': type_.scale}

    def visit_numeric(self, type_):
        """
        numeric rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_DECIMAL(type_)
        # call overriden above method for
        # sqlalchemy datatype resolution

    def visit_datetime(self, type_):
        """
        datetime rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_TIMESTAMP(type_)

    def visit_date(self, type_):
        """
        date rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_DATE(type_)

    def visit_time(self, type_):
        """
        time rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_TIME(type_)

    def visit_integer(self, type_):
        """
        integer rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_INT(type_)

    def visit_boolean(self, type_):
        """
        boolean rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_SMALLINT(type_)

    def visit_float(self, type_):
        """
        float rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        if type_.precision:
            type_.precision = min(type_.precision, 52)
        return self.visit_FLOAT(type_)

    def visit_string(self, type_):
        """
        string rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_VARCHAR(type_)

    def visit_TEXT(self, type_):
        """
        clob (character large object rendering)
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_CLOB(type_)

    def visit_large_binary(self, type_):
        """
        blob (binary large object) rendering
        :param type_: the SQLAlchemy datatype
            specified by the user
        :returns: data type rendering
        """
        return self.visit_BLOB(type_)


########################################
#                                      #
#     Splice Machine SQL Compiler      #
#                                      #
########################################

class SpliceMachineCompiler(compiler.SQLCompiler):
    """
    Class contains various methoods
    that override generic SQL
    (from sqlalchemy.compiler.SQLCompiler)
    to convert to our SQL
    """

    def get_cte_preamble(self, recursive):
        """
        Get the preamble for common
        table expressions
        :param recursive: whether or not
            the preamble is recursive
        :returns: preamble
        """
        return "WITH"  # always WITH

    def visit_now_func(self, fn, **kw):
        """
        Get the SQL function for getting
        NOW()
        :param fn: function for getting timestamp
        :returns: function
        """
        return "CURRENT_TIMESTAMP"

    def for_update_clause(self, select):
        """
        Specifies that cursor should not
        be read-only, should be updatable
        :param select: the Splice SQL select query
        :returns: clause at the end of select
        "for update"
        """
        if select.for_update == True:  # updatable
            return ' WITH RS USE AND KEEP UPDATE LOCKS'
        elif select.for_update == 'read':  # read-only
            return ' WITH RS USE AND KEEP SHARE LOCKS'
        else:
            return ''  # no clause

    def visit_mod_binary(self, binary, operator, **kw):
        """
        Modulo operator with binary numbers
        :param binary: the binary to extract
        :operator: the operation specified
        :returns: clause for doing modulo with binary numbers
            on Splice
        """
        return "mod(%s, %s)" % (self.process(binary.left),
                                self.process(binary.right))

    def limit_clause(self, select, **kwargs):
        """
        Generate a LIMIT clause for SQL
        SELECT queries via Fetch First
        :param select: the select query class
        :returns: clause for limit
        """
        text = ''
        if select._offset_clause is not None:
            text += ' OFFSET %s ROWS' % select._offset
        if select._limit_clause is not None:
            text += " FETCH FIRST %s ROWS ONLY" % select._limit  # get fetch first
        return text

    def visit_select(self, select, **kwargs):
        """
        Function for handling the creation of SELECT statements
        Splice does not support native casting for WHERE IN comparisons, so we need to add an explicit cast if necessary
        :param select:
        :param kwargs:
        :return: the SQL select statement to execute
        """
        try:
            sql = super(SpliceMachineCompiler, self).visit_select(select, **kwargs)
            col_types = {}
            if select._columns_plus_names[0][0]:
                for e in select._columns_plus_names:
                    col_types[e[1].name] = str(e[1].type).split('.')[-1]
            return sql
        except:
            import traceback
            traceback.print_exc()
            raise Exception

    def visit_sequence(self, sequence):
        """
        Get the next value clause in a Splice Sequence
        :param sequence: sequence object to get the next
            value for
        :returns: clause for extracting next value
        """
        return "NEXT VALUE FOR %s" % sequence.name

    def default_from(self):
        """
        We also have SYSIBM.SYSDUMMY1 table,
        so we can use this default from clause
        :returns: default from clause
        """
        # DB2 uses SYSIBM.SYSDUMMY1 table for row count
        return " FROM SYSIBM.SYSDUMMY1"  # which we have too!

    def construct_params(self, params=None, _group_number=None, _check=True):
        """
        Construct parameters for literal binds,
        and convert unicode values to string values
        or the database will get messed up (
        primary keys will not work, db cannot render type etc.)

        :param params: params for the the renderer
        :param _group_number: the id for the statement
        :param _check: whether or not to check for
            literal/non literal binds
        """
        out = super(SpliceMachineCompiler, self).construct_params(
            params=params, _group_number=_group_number, _check=_check
        )
        for param in out:
            # unicode won't be hit in Python3 (short-circuit execution)
            if not IS_PYTHON_3 and (isinstance(out[param], str) or isinstance(out[param], unicode)):
                out[param] = str(out[param]).encode('utf-8')
        return out

    def visit_function(self, func, result_map=None, **kwargs):
        """
        Handle built in functions
        :param func: the built in function
        :param result_map: whether or not to return results back to user
        :returns: the parsed function
        """
        if func.name.upper() == "AVG":  # average function (needs to be uppercase)
            return "AVG(DOUBLE(%s))" % (self.function_argspec(func, **kwargs))
        elif func.name.upper() == "CHAR_LENGTH":  # char length function
            return "CHAR_LENGTH(%s, %s)" % (self.function_argspec(func, **kwargs), 'OCTETS')
        else:
            return compiler.SQLCompiler.visit_function(self, func, **kwargs)  # generic

    def visit_cast(self, cast, **kw):
        """
        Handle casting between
        datatypes on Splice Machine
        :param cast: the cast class
        :returns: clause for casting
        """
        type_ = cast.typeclause.type  # get cast type

        if isinstance(type_, (
                sa_types.DateTime, sa_types.Date, sa_types.Time,
                sa_types.DECIMAL)):  # call superclass visit cast (special handling)
            return super(SpliceMachineCompiler, self).visit_cast(cast, **kw)
        else:
            return self.process(cast.clause)  # call generic cast (regular)

    def get_select_precolumns(self, select, **kwargs):
        """
        Handles selecting distinct records
        in a SELECT query
        :param select: select query class
        :returns: clause for distinct queries
        """
        if isinstance(select._distinct, str):  # are we selecting distinctly?
            return select._distinct.upper() + " "
        elif select._distinct:
            return "DISTINCT "  # add clause
        else:
            return ""  # don't add clause

    def visit_join(self, join, asfrom=False, **kwargs):
        """
        Process Database joins in Splice Machine SQL
        :param join: the join class for Splice Machine
        :param asfrom: are we using AS/FROM clause structure
        :returns: join clause
        """
        return ''.join(
            (self.process(join.left, asfrom=True, **kwargs),  # left join
             (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN "),
             self.process(join.right, asfrom=True, **kwargs),  # right join
             " ON ",
             self.process(join.onclause, **kwargs)))  # what we are joining on

    def visit_savepoint(self, savepoint_stmt):
        """
        Process savepoint statement
        :param savepoint_stmt: the savepoint statement class
        :returns: generated clause
        """
        return "SAVEPOINT %(sid)s ON ROLLBACK RETAIN CURSORS" % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)}

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        """
        Process rollback savepoint statement
        :param savepoint_stmt: the savepoint statement class
        :returns: generated clause
        """
        return 'ROLLBACK TO SAVEPOINT %(sid)s' % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)}

    def visit_release_savepoint(self, savepoint_stmt):
        """
        Process release statement
        :param savepoint_stmt: the savepoint statement class
        :returns: generated clause
        """
        return 'RELEASE TO SAVEPOINT %(sid)s' % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)}

    def visit_unary(self, unary, **kw):
        """
        Process unary operator in Sqlalchemy for Splice SQL
        :param unary: the unary operator class
        :returns: clause generated
        """
        if (unary.operator == operators.exists) and kw.get('within_columns_clause', False):
            usql = super(SpliceMachineCompiler, self).visit_unary(unary, **kw)  # call parent
            usql = "CASE WHEN " + usql + " THEN 1 ELSE 0 END"
            return usql
        else:
            return super(SpliceMachineCompiler, self).visit_unary(unary, **kw)  # call parent

    def visit_column(self, column, add_to_result_map=None, include_table=True, **kwargs):
        """
        Override parent visit_column so that we ensure *all* column
        names are capitalized, because Splice Machine works better
        with capitalized columns
        :param column: column class to process
        :param add_to_result_map: whether to return results back to user
        :param includ_table: whether or not to include the table name
            in column name
        :returns: corrected table name
        """
        out = super(SpliceMachineCompiler, self).visit_column(column,
                                                              add_to_result_map=add_to_result_map,
                                                              include_table=include_table, **kwargs)
        return out


########################################
#                                      #
#     Splice Machine DDL Compiler      #
#                                      #
########################################

class SpliceMachineDDLCompiler(compiler.DDLCompiler):

    def _is_nullable_unique_constraint_supported(self, dialect):
        """
        Check if nullable unique constraints are supported
        in the current dialect
        :param dialect: the current dialect (e.g. Splice Machine)
        :returns: whether or not it is supported
        """
        return False  # it is not supported

    def get_column_default_string(self, column, **kw):
        """
        Convert between types (quote +/-) on default
        cols when mistakes are encountered
        """
        output = compiler.DDLCompiler.get_column_default_string(self, column, **kw)
        if output:
            # We were double quoting things inside a "text()" literal argument, so we need to not do that. We can
            # check for a text literal within the server_default.arg param
            server_default = getattr(column, 'server_default')
            if server_default and type(getattr(server_default, 'arg')) == TextClause: # check for text literal
                return output
            return QuotationUtilities.get_default_type_converter(str(column.type))(output)

    def get_column_specification(self, column, **kw):
        """
        Get column specification for CREATE TABLE
        in Splice Machine SQL
        :param column: column object from SQLAlchemy
        :returns: column name, plus is specification (type)
        """
        col_spec = [self.preparer.format_column(column)]
        col_spec.append(self.dialect.type_compiler.process(column.type, type_expression=column))
        # add SQL Data type to specification, right off the bat

        # not nullable
        if not column.nullable or column.primary_key:
            col_spec.append('NOT NULL')

        # default clause
        default = self.get_column_default_string(column)
        if default is not None:
            col_spec.append('WITH DEFAULT')
            col_spec.append(default)

        # autoincrement identity column
        if column is column.table._autoincrement_column:
            col_spec.append('GENERATED BY DEFAULT')
            col_spec.append('AS IDENTITY')
            col_spec.append('(START WITH 1)')  # mlflow starts default
            # experiment at 0, so we have to start sequences at 1

        column_spec = ' '.join(col_spec)  # convert to String
        return column_spec

    def define_constraint_cascades(self, constraint):
        """
        Add a clause for cascading constraints
        :param constraint: the constraint class
        :returns: constraint clause
        """
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        if constraint.onupdate is not None:
            util.warn(
                "Splice Machine does not support UPDATE CASCADE for foreign keys.")

        return text

    def visit_drop_constraint(self, drop, **kw):
        """
        Handle dropping of constraints in Splice Machine
        DB
        :param drop: the drop class in SQLAlchemy
        :returns: drop constraint command in SQL
        """
        constraint = drop.element
        if isinstance(constraint, sa_schema.ForeignKeyConstraint):
            # drop foreign key constraints
            qual = "FOREIGN KEY "
            const = self.preparer.format_constraint(constraint)
        elif isinstance(constraint, sa_schema.PrimaryKeyConstraint):
            # drop primary key constraints
            qual = "PRIMARY KEY "
            const = ""
        elif isinstance(constraint, sa_schema.UniqueConstraint):
            # drop unique constraint
            qual = "UNIQUE "
            if self._is_nullable_unique_constraint_supported(self.dialect):  # no
                for column in constraint:
                    if column.nullable:
                        constraint.uConstraint_as_index = True
                if getattr(constraint, 'uConstraint_as_index', None):
                    qual = "INDEX "
            const = self.preparer.format_constraint(constraint)
        elif isinstance(constraint, sa_schema.CheckConstraint):
            qual = "CONSTRAINT "
            const = constraint.name
        else:
            qual = ""
            const = self.preparer.format_constraint(constraint)

        if hasattr(constraint, 'uConstraint_as_index') and constraint.uConstraint_as_index:
            return "DROP %s%s" % \
                   (qual, const)

        sql = ("ALTER TABLE %s DROP %s%s" % \
               (self.preparer.format_table(constraint.table),
                qual, const))  # get command
        return sql

    def create_table_constraints(self, table, **kw):
        """
        Create constraints for a given SQLAlchemy table
        object
        :param table: table object in Sqlalchemy
        :returns: command to create table
        """
        if self._is_nullable_unique_constraint_supported(self.dialect):
            for constraint in table._sorted_constraints:
                if isinstance(constraint, sa_schema.UniqueConstraint):
                    for column in constraint:
                        if column.nullable:
                            constraint.use_alter = True
                            constraint.uConstraint_as_index = True
                            break
                    if getattr(constraint, 'uConstraint_as_index', None):
                        if not constraint.name:
                            index_name = "%s_%s_%s" % (
                                'ukey', self.preparer.format_table(constraint.table),
                                '_'.join(column.name for column in constraint))
                        else:
                            index_name = constraint.name
                        index = sa_schema.Index(index_name, *(column for column in
                                                              constraint))  # create a new index
                        index.unique = True
                        index.uConstraint_as_index = True
        result = super(SpliceMachineDDLCompiler, self).create_table_constraints(table,
                                                                                **kw)  # call original
        return result

    def visit_create_table(self, create):
        try:
            temporary_index = create.element._prefixes.index('TEMPORARY')
        except ValueError:
            temporary_index = -1

        if temporary_index != -1:
            create.element._prefixes.insert(temporary_index, 'GLOBAL')  # we require
            # global/local temporary table
        out = super(SpliceMachineDDLCompiler, self).visit_create_table(create)
        # If a column is a primary key, remove the unique constraint
        for c in create.element.c:
            if c.primary_key and c.unique:
                pk_name = c.name
                regxp = re.compile(f',\s*\n.*?UNIQUE \({pk_name}\)')
                out = re.sub(regxp, '', out)
                break
        return out

    def visit_create_index(self, create, include_schema=True, include_table_schema=True):
        """
        Create a new index in Splice Machine DB
        :param create: the object SQLAlchemy for index creation
        :param include_schema: whether or not to include schema in create_index SQL command
        :param include_table_schema: whether or not to include both schema and table in SQL
        :returns: create index command in SQL
        """
        sql = super(SpliceMachineDDLCompiler, self).visit_create_index(create, include_schema,
                                                                       include_table_schema)
        if getattr(create.element, 'uConstraint_as_index', None):
            sql += ' EXCLUDE NULL KEYS'
        return sql

    def visit_add_constraint(self, create):
        """
        Create a new constraint in SQLAlchemy for Splice SQL
        :param create: create class in SQLAlchemy
        :returns: sql to add constraint
        """
        if self._is_nullable_unique_constraint_supported(self.dialect):
            if isinstance(create.element, sa_schema.UniqueConstraint):
                for column in create.element:
                    if column.nullable:
                        create.element.uConstraint_as_index = True
                        break
                if getattr(create.element, 'uConstraint_as_index', None):
                    if not create.element.name:
                        index_name = "%s_%s_%s" % (
                            'uk_index', self.preparer.format_table(create.element.table),
                            '_'.join(column.name for column in create.element))
                    else:
                        index_name = create.element.name
                    index = sa_schema.Index(index_name, *(column for column in create.element))
                    index.unique = True
                    index.uConstraint_as_index = True
                    sql = self.visit_create_index(
                        sa_schema.CreateIndex(index))  # create index for constraint
                    return sql
        sql = super(SpliceMachineDDLCompiler, self).visit_add_constraint(create)
        return sql


########################################
#                                      #
#     Splice Machine Formatters        #
#                                      #
########################################

class SpliceMachineIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = constants.RESERVED_WORDS
    illegal_initial_characters = set(range(0, 10)).union(["_", "$"])


########################################
#                                      #
#     Splice Machine Exec Context      #
#                                      #
########################################

class SpliceMachineExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        """
        Get the next value (increment as well) from a Splice
        Machine Sequence
        :param seq: sequence name
        :param type_: the type of the sequence (typically INTEGER)
        """
        return self._execute_scalar("SELECT NEXTVAL FOR " +
                                    self.dialect.identifier_preparer.format_sequence(seq) +
                                    " FROM SYSIBM.SYSDUMMY1", type_)


########################################
#                                      #
#     Splice Machine Identity Col      #
#                                      #
########################################
class _SelectLastRowIDMixin(object):
    """
    Used for autoincrementing columns
    """
    _select_lastrowid = False  # whether or not to retrieve current value of sequence (if identity)
    _lastrowid = None  # value last in sequence
    _last_column_name = None  # column name of identity col (only 1 supported per table)
    _last_table = None  # last table

    def get_lastrowid(self):
        """
        Get the id of the last row
        """
        return self._lastrowid

    def pre_exec(self):
        """
        Essentially decide whether
        or not to use autoincrementation
        for a given column
        """
        if self.isinsert:
            tbl = self.compiled.statement.table
            seq_column = tbl._autoincrement_column  # is identity?
            insert_has_sequence = seq_column is not None

            self._select_lastrowid = insert_has_sequence and \
                                     not self.compiled.returning and \
                                     not self.compiled.inline  # should we get sequence value?

            if self._select_lastrowid:
                if tbl.schema:
                    self._last_table = (QuotationUtilities.conditionally_reserved_quote(tbl.schema)
                                    + "." + QuotationUtilities.conditionally_reserved_quote(
                            tbl.name))
                else:
                    self._last_table = (QuotationUtilities.conditionally_reserved_quote(
                            tbl.name))
                self._last_column_name = seq_column.key

    def _get_last_id(self):
        """
        Utility method to get the last
        sequence value in a table with an
        identity col (parameters configured
        from pre_exec function)
        :returns: the last id
        """
        # TODO @amrit this is an extremely inefficient way to get last identity value. fixme
        conn = self.root_connection
        query = 'SELECT MAX({identity_col}) FROM {table}'.format(
            identity_col=self._last_column_name, table=self._last_table
        )
        conn._cursor_execute(self.cursor, query, (), self)
        result = self.cursor.fetchall()
        return int(result[0][0])  # fetch results

    def post_exec(self):
        """
        Get the current sequence value
        after executing
        """
        if self._select_lastrowid:
            row_id = self._get_last_id()  # get last seq value
            if row_id is not None:
                self._lastrowid = row_id


########################################
#                                      #
#     Splice Machine SQL Dialect       #
#                                      #
########################################

class SpliceMachineDialect(default.DefaultDialect):
    ##### DATABASE OPTIONS #####
    name = 'splicemachinesa'
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
    supports_native_decimal = True
    supports_native_boolean = False
    preexecute_sequences = False
    supports_alter = True
    supports_sequences = True
    sequences_optional = True

    requires_name_normalize = True

    supports_default_values = False
    supports_empty_insert = False

    two_phase_transactions = False
    savepoints = True
    supports_native_enum = False

    statement_compiler = SpliceMachineCompiler
    ddl_compiler = SpliceMachineDDLCompiler
    type_compiler = SpliceMachineTypeCompiler
    preparer = SpliceMachineIdentifierPreparer
    execution_ctx_cls = SpliceMachineExecutionContext

    _reflector_cls = sm_reflection.SMReflector  # get reflectors

    def __init__(self, **kw):
        super(SpliceMachineDialect, self).__init__(**kw)

        self._reflector = self._reflector_cls(self)

    ##### REFLECTOR WRAPPERS ####
    def initialize(self, connection):
        super(SpliceMachineDialect, self).initialize(connection)
        self.dbms_ver = None
        self.dbms_name = None

    def normalize_name(self, name):
        return self._reflector.capitalize(name)

    def denormalize_name(self, name):
        return self._reflector.capitalize(name)

    def _get_default_schema_name(self, connection):
        return self._reflector._get_default_schema_name(connection)

    def has_table(self, connection, table_name, schema=None):
        return self._reflector.has_table(connection, table_name, schema=schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        return self._reflector.has_sequence(connection, sequence_name,
                                            schema=schema)

    def get_schema_names(self, connection, **kw):
        return self._reflector.get_schema_names(connection, **kw)

    def get_table_names(self, connection, schema=None, **kw):
        return self._reflector.get_table_names(connection, schema=schema, **kw)

    def get_view_names(self, connection, schema=None, **kw):
        return self._reflector.get_view_names(connection, schema=schema, **kw)

    def get_view_definition(self, connection, viewname, schema=None, **kw):
        return self._reflector.get_view_definition(
            connection, viewname, schema=schema, **kw)

    def get_columns(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_columns(
            connection, table_name, schema=schema, **kw)

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_indexes(
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


dialect = SpliceMachineDialect
