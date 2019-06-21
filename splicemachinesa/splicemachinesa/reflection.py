import sys, time
from sqlalchemy import types as sa_types
from sqlalchemy import sql, util
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
import re
import codecs
from sys import version_info

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
        self.dialect = dialect # splice SQL
        self.ischema_names = dialect.ischema_names # supported data types
        self.identifier_preparer = dialect.identifier_preparer # formatting

        self.default_schema_id = None # default schema (SPLICE)

    def normalize_name(self, name):
        """
        Normalize the given name (table, column, schema etc)
        :param name: the name to normalize
        :returns: normalized name
        """
        return name

    def denormalize_name(self, name):
        """
        Denormalize a given name (put back quotes
        etc.)
        :param name: the name of the entity
        :returns: denormalized name
        """
        return name

    def _get_default_schema_name(self, connection):
        """
        Get the default schema name
        :param connection: ODBC cnxn to Splice
        Return: current setting of the schema attribute
        """
        return 'SPLICE' # our default schema is SPLICE

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
    # SYS.SYSSCHEMAS
    sys_schemas = Table("SYSSCHEMAS", ischema,
      Column("SCHEMAID", CoerceUnicode, key="schemaid"),
      Column("SCHEMANAME", CoerceUnicode, key="schemaname"),
      schema="SYS")

    # SYS.SYSTABLES
    sys_tables = Table("SYSTABLES", ischema,
      Column("SCHEMAID", CoerceUnicode, key="schemaid"),
      Column("TABLENAME", CoerceUnicode, key="tablename"),
      Column("TABLETYPE", CoerceUnicode, key="tabletype"),
      schema="SYS")

    # SYSVW.SYSTABLESVIEW
    sys_tables_view = Table("SYSTABLESVIEW", ischema,
      Column("TABLEID", CoerceUnicode, key="tableid"),
      Column("TABLENAME", CoerceUnicode, key="tablename"),
      Column("SCHEMAID", CoerceUnicode, key="schemaid"),
      Column("STATUS", CoerceUnicode, key="status"),
      Column("SCHEMANAME", CoerceUnicode, key="schemaname"),
      schema="SYSVW")

    # SYS.SYSVIEWS
    sys_views = Table("SYSVIEWS", ischema,
      Column("TABLEID", CoerceUnicode, key="viewschema"),
      Column("VIEWDEFINITION", CoerceUnicode, key="viewdefinition"),
      Column("COMPILATIONSCHEMAID", CoerceUnicode, key="compilationschemaid"),
      schema="SYS")

    # SYS.SYSSEQUENCES
    sys_sequences = Table("SYSSEQUENCES", ischema,
      Column("SEQUENCENAME", CoerceUnicode, key="sequencename"),
      Column("SEQUENCESCHEMAID", CoerceUnicode, key="sequenceschemaid"),
      schema="SYS")

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
        """.format(systable=self.SYS_SCHEMA, schema=schemaName)

        print(query)
        # SQL Query
        out = connection.execute(query).first()
        if out:
            return out[0] # exists
        else:
            return None # doesn't exist
    
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
            schema = schemaName.upper() # != None
        else:
            schema = self.default_schema_name.upper() # == null
        print("Schema ID OR DEFAULT: schema is " + str(schema))
        return self.get_schema_id(schema, connection) # get schema id
    
    def has_table(self, connection, table_name, schema=None):
        """
        Return if table exists in DB
        :param connection: ODBC cnxn
        :param table_name: table name to check if exists
        :param schema: schema of the table
        :returns: whether or not table exists
        """
       
        current_schema = self.denormalize_name(
            schema or self.default_schema_name) # get uppercase for tables
        table_name = self.denormalize_name(table_name)

        query = """
        SELECT TABLENAME FROM 
        {systable} WHERE
        SCHEMANAME = '{schema}' AND
        TABLENAME = '{table}'
        """.format(systable=self.SYS_TABLEVIEW, schema=current_schema,
            table=table_name)

        print(query)
        # TODO @amrit: TURN THIS INTO SQL FOR FASTER EXECUTION w/o ORM
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
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        sequence_name = self.denormalize_name(sequence_name)
        if current_schema:
            whereclause = sql.and_(self.sys_sequences.c.sequenceschemaid == schema_id,
                                   self.sys_sequences.c.sequencename == sequence_name)
        else:
            whereclause = self.sys_sequences.c.sequencename == sequence_name
        s = sql.select([self.sys_sequences.c.sequencename], whereclause) # execute sql
        c = connection.execute(s)
        return c.first() is not None

    def get_schema_names(self, connection, **kw):
        """
        Get schema names in DB 
        :param connection: ODBC cnxn
        """
        sysschema = self.sys_schemas
        query = sql.select([sysschema.c.schemaname],
            sql.not_(sysschema.c.schemaname.like('SYS%')),
            order_by=[sysschema.c.schemaname]
        ) # get all tables except SYS tables
        return [self.normalize_name(r[0]) for r in connection.execute(query)]


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

        schema_id = self.get_schema_id_or_default(schema, connection) # get schema id
        print("Schema ID is " + str(schema_id))

        query = """
        SELECT TABLENAME FROM {systable}
        WHERE TABLETYPE='T' AND 
        SCHEMAID='{schemaid}'
        """.format(systable=self.SYS_TABLE, schemaid=schema_id)

        out = connection.execute(query)
        tables = [self.normalize_name(r[0]) for r in connection.execute(query)]
        return map(lambda tbl: tbl.lower(), tables) if lowercase else tables

    def get_table_name(self, connection, tableid):
        """
        Get name of a table given its ID (utility func)
        :param connection: ODBC cnxn
        :param tableid: the id of the table to retrieve name for
        :returns: the name of the table for the given table id
        """
        query = sql.select([self.sys_tables.tablename]).where(
              self.sys_tables.tableid == tableid)

        result = connection.execute(query).first() # execute SQL

        if result:
            return result[0]
        return None
      
    def get_table_id(self, connection, tablename):
        """
        Get ID of table given its name
        :param connection: ODBC connection to db
        :param tablename: table to get id for
        :returns: id for table given
        """
        query = sql.select([self.sys_tables.tableid]).where(
              self.sys_tables.tablename == tablename)
        # select to get table id
        result = connection.execute(query).first() # execute SQL
        if result:
            return result[0]
        return None

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """
        Get the names of all views under the specified schema
        :param connection: ODBC cnxn
        :param schema: schema to get views from
        :returns: list of views under schema
        """
        schemaid = self.get_schema_id_or_default(schema, connection)
        # get schema id for views
        current_schema = self.denormalize_name(schema or self.default_schema_name)

        query = sql.select([self.sys_views.c.tableid]).\
            where(self.sys_views.c.compilationschemaid == schemaid)

        viewids = connection.execute(query)

        out = []
        for viewid in viewids: # get all view names under schema
            view_name = self.get_table_name(connection, tableid[0]) # same func
            if view_name:
              out.append(view_name)

        return [self.normalize_name(r[0]) for r in out]

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
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)
        tableid = self.get_table_id(connecton, viewname)
        query = sql.select([self.sys_views.c.viewdefinition]).\
            where(self.sys_views.c.compilationschemaid == schemaid).\
            where(self.sys_views.c.tableid == tableid)
        # get view definition
        return connection.execute(query).scalar()

    def get_columns_from_db(connection, schema, table, col_indices=[]):
        """
        Utility func to call stored procedure to get columns list 
        :param connection: ODBC cnxn
        :param schema: table schema for cols
        :param table: table which has the columns to extract
        :param col_indices: the indexes of the response 
            to extract, serially
        """
        query = "CALL SYSIBM.SQLCOLUMNS(null, '{schema}', \
            '{table}', null, 'DATATYPE'='ODBC')"


        results = []
        for res in connection.execute(query): # get all value @ indices in col_indices order
            results.append([res[ci] for ci in col_indices])

        return results

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Get all columns for a given table
        :param connection: ODBC cnxn
        :param table_name: the name of the table which has columns
        :param schema: the schema for the table
        :returns: list of columns from db with associated metadata
        """
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)

        INDICES = [3, 5, 12, 10, 6, 8, 22, 23] # these are the indexes in our table
        # which correspond to the ones in IBM DB2
        column_data = self.get_columns_from_db(connection, current_schema, table,
            col_indices=INDICES) # call SYSIBM.SQLCOLUMNS

        sa_columns = [] 
        for r in column_data:
            coltype = r[1].upper() # extract column type
            if coltype in ['DECIMAL', 'NUMERIC']:
                coltype = self.ischema_names.get(coltype)(int(r[4]), int(r[5])) # extract
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
                    coltype = coltype = sa_types.NULLTYPE # assign no type if not understood

            sa_columns.append({ # add column data to array
                    'name': self.normalize_name(r[0]),
                    'type': coltype,
                    'nullable': r[3] == 'YES',
                    'default': r[2] or None,
                    'autoincrement': (r[6] == 'YES'),
                })
        return sa_columns

    def get_primary_keys_from_table(self, connection,schema, table):
        """
        Get list of primary keys from table
        :param connection: ODBC cnxn
        :param schema: schema under which table is found
        :param table: table which you want to find primary keys under
        :returns: primary key columns
        """
        query = "CALL SYSIBM.SQLPRIMARYKEYS(null, '{schema}', '{table}', null)"
        # stored procedure to get primary keys
        results = [i[3] for i in list(connection.execute(query))] # get primary keys
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
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)

        primary_keys = self.get_primary_keys_from_table(connection, current_schema, table)
        
        return [self.normalize_name(col) for col in primary_keys]


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
            query = "CALL SYSIBM.SQLFOREIGNKEYS('','{schema}','{table}','',null,'','EXPORTEDKEY=1'"

        return list(connection.execute(query))

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Get outgoing foreign keys from a table
        :param connection: ODBC cnxn
        :param table_name: the name of the table to extract keys from
        :param schema: schema where table is 
        :returns: list of outgoing columns
        """
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        
        results = self.get_foreign_keys_from_db(connection, current_schema, table_name, imported=False)
        fschema = {}
        for r in results:
            if not (r[12]) in fschema:
                referred_schema = self.normalize_name(r[1])

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                    referred_schema == default_schema:
                    referred_schema = None

                fschema[r[12]] = {
                    'name': self.normalize_name(r[12]),
                    'constrained_columns': [self.normalize_name(r[7])],
                    'referred_schema': referred_schema,
                    'referred_table': self.normalize_name(r[2]),
                    'referred_columns': [self.normalize_name(r[3])]}
            else:
                fschema[r[12]]['constrained_columns'].append(self.normalize_name(r[7]))
                fschema[r[12]]['referred_columns'].append(self.normalize_name(r[3]))
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
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        
        results = self.get_foreign_keys_from_db(connection, current_schema, table_name, imported=True)
        print("FK 2 Results: " + str(results))

        fschema = {}
        for r in results:
            if not fschema.has_key(r[12]):
                constrained_schema = self.normalize_name(r[5])

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                    constrained_schema == default_schema:
                    constrained_schema = None

                fschema[r[12]] = {
                    'name': self.normalize_name(r[12]),
                    'constrained_schema': constrained_schema,
                    'constrained_table': self.normalize_name(r[6]),
                    'constrained_columns': [self.normalize_name(r[7])],
                    'referred_schema': schema,
                    'referred_table': self.normalize_name(r[2]),
                    'referred_columns': [self.normalize_name(r[3])]}
            else:
                fschema[r[12]]['constrained_columns'].append(self.normalize_name(r[7]))
                fschema[r[12]]['referred_columns'].append(self.normalize_name(r[3]))
        return [value for key, value in fschema.items()]

