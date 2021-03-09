from sqlalchemy import types as sa_types
from sqlalchemy import util
from sqlalchemy import MetaData
from sqlalchemy.engine import reflection

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



class CoerceUnicode(sa_types.TypeDecorator):
    """
    Coerce a given type to unicode
    """
    impl = sa_types.Unicode

    def process_bind_param(self, value, dialect):
        if isinstance(value, str):
            value = value
        return value


class BaseReflector(object):
    """
    Standard functions for Splice Machine
    SQL Reflector
    """

    def __init__(self, dialect):
        """
        :param dialect: current SQL Dialect (splice sql)
        """
        self.dialect = dialect  # splice SQL
        self.ischema_names = dialect.ischema_names  # supported data types
        self.identifier_preparer = dialect.identifier_preparer  # formatting

        self.default_schema_id = None  # default schema (SPLICE)

    def capitalize(self, identifier):
        """
        Capitalize the identifier specified
        :param identifier: the identifier to capitalize
        :returns: the identifier captialized
        """
        return identifier.upper()

    def _get_default_schema_name(self, connection):
        """
        Get the default schema name
        :param connection: ODBC cnxn to Splice
        Return: current setting of the schema attribute
        """
        return connection.execute('VALUES(CURRENT SCHEMA)').fetchone()[0]  # our default schema is SPLICE

    @property
    def default_schema_name(self):
        """
        Getter for default schema name
        """
        return self.dialect.default_schema_name


class SMReflector(BaseReflector):
    ischema = MetaData()

    #### Internal Splice Machine Table Schemas ####

    SYS_SCHEMA = 'SYS.SYSSCHEMAS'
    SYS_TABLEVIEW = 'SYSVW.SYSTABLESVIEW'
    SYS_TABLE = 'SYS.SYSTABLES'
    SYS_VIEWS = 'SYS.SYSVIEWS'
    SYS_SEQUENCES = 'SYS.SYSSEQUENCES'

    def get_schema_id(self, schemaName, connection):
        """
        Returns the schema id associated with a specified
        schema from Splice Machine DB
        :param schemaName: the name of the schema to get the id for
        :param connection: ODBC connection to database
        :returns: schema id if schema exists, else none
        """
        # TODO @amrit: turn this into SQL (saves time)
        query = """
        SELECT SCHEMAID FROM
        {systable} WHERE
        SCHEMANAME = '{schema}'
        """.format(systable=self.SYS_SCHEMA, schema=self.capitalize(schemaName))
        # SQL Query
        out = connection.execute(query).first()
        if out:
            return out[0]  # exists
        else:
            return None  # doesn't exist

    def get_schema_id_or_default(self, schemaName, connection):
        """
        Returns default schema id if schemaName is none,
        otherwise, returns the schema id for the specified 
        schemaName
        :param schemaName: schemaName to retrieve id
        :param connection: ODBC connection to Splice
        :returns: schema id or default schema if schema is not specified, else schema id
        """
        if schemaName:
            schema = self.capitalize(schemaName)  # != None
        else:
            schema = self.default_schema_name  # == null
        return self.get_schema_id(schema, connection)  # get schema id

    def has_table(self, connection, table_name, schema=None):
        """
        Return if table exists in DB
        :param connection: ODBC cnxn
        :param table_name: table name to check if exists
        :param schema: schema of the table
        :returns: whether or not table exists
        """

        current_schema = self.capitalize(
            schema or self.default_schema_name)  # get uppercase for tables
        table_name = self.capitalize(table_name)

        query = """
        SELECT TABLENAME FROM 
        {systable} WHERE
        SCHEMANAME = '{schema}' AND
        TABLENAME = '{table}'
        """.format(systable=self.SYS_TABLEVIEW, schema=current_schema,
                   table=table_name)
        c = connection.execute(query)
        # execute sql over odbc
        out = c.first() is not None

        return out

    def has_sequence(self, connection, sequence_name, schema=None):
        """
        Returns if sequence exists in DB
        :param connection: ODBC cnxn
        :param sequence_name: the sequence name to check existance of
        :param schema: the schema of the sequence
        :returns: wehter or not schema exists
        """
        schema_id = self.get_schema_id_or_default(schema, connection)
        # get schema id for the sequence
        sequence_name = self.capitalize(sequence_name)

        query = """
        SELECT SEQUENCENAME FROM 
        {systable} WHERE SEQUENCESSCHEMAID='{schemaid}'
        AND SEQUENCENAME='{sequencename}'
        """.format(schemaid=schema_id, sequencename=sequence_name,
                   systable=self.SYS_SEQUENCES)

        c = connection.execute(query)
        return c.first() is not None

    def get_schema_names(self, connection, **kw):
        """
        Get schema names in DB 
        :param connection: ODBC cnxn
        """
        query = """
        SELECT SCHEMANAME FROM {systable}
        WHERE SCHEMANAME NOT LIKE 'SYS%' 
        ORDER BY SCHEMANAME
        """.format(systable=self.capitalize(self.SYS_SCHEMA))

        out = [r[0].lower() for r in connection.execute(query)]
        return out

    @reflection.cache
    def get_table_names(self, connection, schema=None, lowercase=True, **kw):
        """
        Get table names in DB
        :param connection: ODBC cnxn
        :param schema: schema to look under
        :param lowercase: MLFlow requires that
            table names are returned in lowercase,
            so even though they are stored in Splice Machine
            uppercase, we will convert them to lowercase
            by default
        :returns: list of all tables under schema
        """

        schema_id = self.get_schema_id_or_default(schema, connection)  # get schema id

        query = """
        SELECT TABLENAME FROM {systable}
        WHERE (TABLETYPE='T' OR TABLETYPE='E') AND 
        SCHEMAID='{schemaid}'
        """.format(systable=self.capitalize(self.SYS_TABLE), schemaid=schema_id)

        tables = [r[0] for r in connection.execute(query)]
        return [table.lower() for table in tables] if lowercase else tables

    def get_table_name(self, connection, tableid):
        """
        Get name of a table given its ID (utility func)
        :param connection: ODBC cnxn
        :param tableid: the id of the table to retrieve name for
        :returns: the name of the table for the given table id
        """
        query = """
        SELECT TABLENAME FROM {systable}
        WHERE TABLEID='{tableid}'
        """.format(systable=self.capitalize(self.SYS_TABLE), tableid=tableid)

        result = connection.execute(query).first().upper()  # execute SQL

        if result:
            return result[0]
        return None

    def get_table_id(self, connection, tablename, schemaid=None, only_views=False):
        """
        Get ID of table given its name
        :param connection: ODBC connection to db
        :param tablename: table to get id for
        :returns: id for table given
        """
        query = """
        SELECT TABLEID FROM 
        {sys_table} WHERE TABLENAME='{table}'
        """.format(sys_table=self.SYS_TABLEVIEW,
                   table=self.capitalize(tablename))
        if schemaid:
            query += "AND SCHEMAID='{schemaid}'\n".format(schemaid=schemaid)
        if only_views:
            query += "AND TABLETYPE='V'"

        # select to get table id
        result = connection.execute(query).first()  # execute SQL
        if result:
            return result[0]
        return None

    @reflection.cache
    def get_view_names(self, connection, schema=None, lowercase=True, **kw):
        """
        Get the names of all views under the specified schema
        :param connection: ODBC cnxn
        :param schema: schema to get views from
        :returns: list of views under schema
        """
        # get schema id for views
        current_schema = self.capitalize(schema or self.default_schema_name)

        query = """
        SELECT TABLENAME FROM 
        {systable} WHERE
        SCHEMANAME = '{schema}'
        AND  TABLETYPE = 'V'
        """.format(systable=self.SYS_TABLEVIEW, schema=current_schema)

        tables = [r[0] for r in connection.execute(query)]
        return [table.lower() for table in tables] if lowercase else tables

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        """
        Get definition of a view from its name
        :param connection: ODBC cnxn
        :param viewname: name of view to get
        :param schema: the name of the schema for view retrieval
        :returns: definition of the view
        """
        schemaid = self.get_schema_id_or_default(schema, connection)
        # get schema id
        viewname = self.capitalize(viewname)
        tableid = self.get_table_id(connection, viewname, schemaid=schemaid, only_views=True)

        query = """
        SELECT VIEWDEFINITION FROM 
        {systable} WHERE TABLEID='{tableid}'
        """.format(systable=self.SYS_VIEWS,
                   tableid=tableid)

        # get view definition
        return connection.execute(query).scalar()

    def get_columns_from_db(self, connection, schema, table, col_indices=[]):
        """
        Utility func to call stored procedure to get columns list 
        :param connection: ODBC cnxn
        :param schema: table schema for cols
        :param table: table which has the columns to extract
        :param col_indices: the indexes of the response 
            to extract, serially
        """
        query = "CALL SYSIBM.SQLCOLUMNS(null, '{schema}','{table}', null, 'DATATYPE'='ODBC')".format(
            schema=schema, table=table
        )
        results = []
        for res in connection.execute(query):  # get all value @ indices in col_indices order
            results.append([res[ci] for ci in col_indices])

        return results

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, lowercase=True, **kw):
        """
        Get all columns for a given table
        :param connection: ODBC cnxn
        :param table_name: the name of the table which has columns
        :param schema: the schema for the table
        :returns: list of columns from db with associated metadata
        """
        current_schema = self.capitalize(schema or self.default_schema_name)
        table = self.capitalize(table_name)

        INDICES = [3, 5, 12, 17, 6, 8, 22, 23]  # these are the indexes in our table
        # which correspond to the ones in IBM DB2
        # name, type, default, nullable, precision, scale, autoincrement 
        column_data = self.get_columns_from_db(connection, current_schema, table,
                                               col_indices=INDICES)  # call SYSIBM.SQLCOLUMNS

        sa_columns = []
        for r in column_data:
            coltype = self.capitalize(r[1])  # extract column type
            if coltype in ['DECIMAL', 'NUMERIC']:
                coltype = self.ischema_names.get(coltype)(int(r[4]), int(r[5]))  # extract
                # full name of two argument types e.g. DECIMAL(3,1)
            elif coltype in ['CHARACTER', 'CHAR', 'VARCHAR']:
                coltype = self.ischema_names.get(coltype)(int(r[4]))
                # one var types: e.g. VARCHAR(100)
            else:
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (coltype, r[0]))
                    coltype = coltype = sa_types.NULLTYPE  # assign no type if not understood

            sa_columns.append({  # add column data to array
                'name': r[0].lower(),
                'type': coltype,
                'nullable': r[3] == 'YES',
                'default': r[2] or None,
                'autoincrement': (r[6] == 'YES'),
            })
        return sa_columns

    def get_primary_keys_from_table(self, connection, schema, table):
        """
        Get list of primary keys from table
        :param connection: ODBC cnxn
        :param schema: schema under which table is found
        :param table: table which you want to find primary keys under
        :returns: primary key columns
        """
        query = "CALL SYSIBM.SQLPRIMARYKEYS(null,'{schema}','{table}',null)".format(
            schema=schema, table=table
        )
        # stored procedure to get primary keys
        results = [i[3] for i in list(connection.execute(query))]  # get primary keys
        return results

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        """
        Get a list of primary keys from a table
        :param connection: odbc cnxn
        :param table_name: the table name to extract keys from
        :param schema: the schema under which table is found
        :returns: list of primary key columns
        """
        current_schema = self.capitalize(schema or self.default_schema_name)
        table = self.capitalize(table_name)

        primary_keys = self.get_primary_keys_from_table(connection, current_schema, table)

        return primary_keys

    def get_foreign_keys_from_db(self, connection, schema, table, imported=True):
        """
        Get outgoing and incoming foreign keys in a given table
        :param connection: ODBC cnxn
        :param schema: schema under which table can be found
        :param table: table to get keys from 
        :param imported: whether or not to get outgoing keys (
            extract PKs from FKs) or incoming (extract FKs from PKs)
        :returns: list of foreing keys
        """
        if imported:
            query = "CALL SYSIBM.SQLFOREIGNKEYS('',null,'','','{schema}','{table}','IMPORTEDKEY=1')"
        else:
            query = "CALL SYSIBM.SQLFOREIGNKEYS('','{schema}','{table}','',null,'','EXPORTEDKEY=1')"
        out = list(connection.execute(query.format(schema=schema, table=table)))
        return out

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Get outgoing foreign keys from a table
        :param connection: ODBC cnxn
        :param table_name: the name of the table to extract keys from
        :param schema: schema where table is 
        :returns: list of outgoing columns
        """
        current_schema = self.capitalize(schema or self.default_schema_name)
        table_name = self.capitalize(table_name)

        results = self.get_foreign_keys_from_db(connection, current_schema, table_name, imported=True)
        fschema = {}
        for r in results:
            if not (r[12]) in fschema:
                referred_schema = r[1]

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                        referred_schema == self.default_schema_name:
                    referred_schema = None

                fschema[r[12]] = {
                    'name': r[12],
                    'constrained_columns': [r[7]],
                    'referred_schema': referred_schema,
                    'referred_table': r[2],
                    'referred_columns': [r[3]]}
            else:
                fschema[r[12]]['constrained_columns'].append(r[7])
                fschema[r[12]]['referred_columns'].append(r[3])
        return [value for key, value in fschema.items()]

    @reflection.cache
    def get_incoming_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Get incoming foreing keys from a table
        :param connection: ODBC cnxn
        :param table_name: the name of the table to extract keys from
        :param schema: schema where table is 
        :returns: list of incoming columns
        """
        current_schema = self.capitalize(schema or self.default_schema_name)
        table_name = self.capitalize(table_name)

        results = self.get_foreign_keys_from_db(connection, current_schema, table_name, imported=True)

        fschema = {}
        for r in results:
            if not fschema.has_key(r[12]):
                constrained_schema = r[5]

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                        constrained_schema == self.default_schema_name:
                    constrained_schema = None

                fschema[r[12]] = {
                    'name': r[12],
                    'constrained_schema': constrained_schema,
                    'constrained_table': r[6],
                    'constrained_columns': [r[7]],
                    'referred_schema': schema,
                    'referred_table': r[2],
                    'referred_columns': [r[3]]}
            else:
                fschema[r[12]]['constrained_columns'].append(r[7])
                fschema[r[12]]['referred_columns'].append(r[3])
        return [value for key, value in fschema.items()]

    def _append_index_dict(self, l, new_dict):
        """
        Appends a new dictionary of index information to a list of dictionaries, creating a list of column_names for
        matching index names.

        ex:
        ld = [{'name': 'SYSTABLES_INDEX1',
               'unique': True,
                'column_names': ['TABLENAME']
              }]
        new_d =  {'name': 'SYSTABLES_INDEX1',
                  'unique': True,
                  'column_names': ['SCHEMAID']
                 }

        _append_index_dict(ld, new_d) ->
        [{'name': 'SYSTABLES_INDEX1',
          'unique': True,
          'column_names': ['TABLENAME', 'SCHEMAID']
        }]

        :param l: a list of dictionaries with keys 'name', 'unique', and 'column_names'
        :param new_dict: dictionary with matching keys
        :return: None, operation applied inplace
        """


        # Append or combine to existing list in place
        for dct in l:
            if new_dict['name'] == dct['name']:
                dct['column_names'].append(new_dict['column_names'][0])
                return
        # Dict didn't exist in the dictionary
        l.append(new_dict)

    def _merge_list_of_dicts(self, l):
        """
        Merges a list of "index" dictionaries, combining column_names for matching indexes into lists
        :param l: a list of dictionaries with keys 'INDEX_NAME', 'NON_UNIQUE', and 'COLUMN_NAME'

        ex:
        [{
          'INDEX_NAME': 'SYSTABLES_INDEX1',
          'NON_UNIQUE': False,
          'COLUMN_NAME': 'TABLENAME'
         },
         {
          'INDEX_NAME': 'SYSTABLES_INDEX1',
          'NON_UNIQUE': False,
          'COLUMN_NAME': 'SCHEMAID'
         },
         {
          'INDEX_NAME': 'SYSTABLES_INDEX2',
          'NON_UNIQUE': False,
          'COLUMN_NAME': 'TABLEID'
         }]

         Will yield:

         [{
           'name': 'SYSTABLES_INDEX1',
           'unique': True,
           'column_names': ['TABLENAME', 'SCHEMAID']
          },
          {
           'name': 'SYSTABLES_INDEX2',
           'unique': True,
           'column_names': ['TABLEID']
          }]

        :return: List of merged dictionaries
        """

        merged_list = []
        for d in l:
            new_d = {
                'name': d['INDEX_NAME'],
                'unique': not d['NON_UNIQUE'],
                'column_names': [d['COLUMN_NAME']]
            }
            self._append_index_dict(merged_list, new_d)
        return merged_list



    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Return information about indexes in `table_name`.
        :param connection: SqlAlchemy Session
        :param table_name: the name of the table to extract keys from
        :param schema: schema where table is
        :returns: list of dicts with the keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean
        """
        current_schema = self.capitalize(schema or self.default_schema_name)
        table_name = self.capitalize(table_name)
        query = "CALL SYSIBM.SQLSTATISTICS(null,'{current_schema}','{table_name}', 1, 1, null)"
        res = connection.execute(query.format(current_schema=current_schema, table_name=table_name))
        cols = res.keys()
        indexes = [dict(zip(cols, i)) for i in res.fetchall()]
        merged = self._merge_list_of_dicts(indexes)
        return merged
