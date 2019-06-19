import sys, time
from sqlalchemy import types as sa_types
from sqlalchemy import sql, util
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
import re
import codecs
from sys import version_info

class CoerceUnicode(sa_types.TypeDecorator):
    impl = sa_types.Unicode

    def process_bind_param(self, value, dialect):
        if isinstance(value, str):
            value = value
        return value

class BaseReflector(object):
    def __init__(self, dialect):
        self.dialect = dialect
        self.ischema_names = dialect.ischema_names
        self.identifier_preparer = dialect.identifier_preparer

        self.default_schema_id = None

    def normalize_name(self, name):
        if isinstance(name, str):
            name = name
        if name != None:
            return name.lower() if name.upper() == name and \
               not self.identifier_preparer._requires_quotes(name.lower()) \
               else name
        return name

    def denormalize_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and \
                not self.identifier_preparer._requires_quotes(name.lower()):
            name = name.upper()
        if not self.dialect.supports_unicode_binds:
            if(isinstance(name, str)):
                name = name
            else:
                name = codecs.decode(name)
        else:
            if version_info[0] < 3:
                name = unicode(name)
            else:
                name = str(name)
        return name

    def _get_default_schema_name(self, connection):
        """Return: current setting of the schema attribute"""
         # default_schema_name = connection.execute(
         #           u'SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1').scalar()
        return 'SPLICE'

    @property
    def default_schema_name(self):
        return self.dialect.default_schema_name

class SMReflector(BaseReflector):
    ischema = MetaData()

    # Internal Splice Machine Table Schemas

    sys_schemas = Table("SYSSCHEMAS", ischema,
      Column("SCHEMAID", CoerceUnicode, key="schemaid"),
      Column("SCHEMANAME", CoerceUnicode, key="schemaname"),
      schema="SYS")

    sys_tables = Table("SYSTABLES", ischema,
      Column("SCHEMAID", CoerceUnicode, key="schemaid"),
      Column("TABLE_NAME", CoerceUnicode, key="tablename"),
      Column("TABLE_TYPE", CoerceUnicode, key="tabletype"),
      schema="SYS")

    sys_tables_view = Table("SYSTABLESVIEW", ischema,
      Column("TABLEID", CoerceUnicode, key="tableid"),
      Column("TABLENAME", CoerceUnicode, key="tablename"),
      Column("SCHEMAID", CoerceUnicode, key="schemaid"),
      Column("STATUS", CoerceUnicode, key="status"),
      Column("SCHEMANAME", CoerceUnicode, key="schemaname"),
      schema="SYSVW")

    sys_views = Table("SYSVIEWS", ischema,
      Column("TABLEID", CoerceUnicode, key="viewschema"),
      Column("VIEWDEFINITION", CoerceUnicode, key="viewdefinition"),
      Column("COMPILATIONSCHEMAID", CoerceUnicode, key="compilationschemaid"),
      schema="SYS")

    sys_sequences = Table("SYSSEQUENCES", ischema,
      Column("SEQUENCENAME", CoerceUnicode, key="sequencename"),
      Column("SEQUENCESCHEMAID", CoerceUnicode, key="sequenceschemaid"),
      schema="SYS")

    def get_schema_id(self, schemaName, connection):
        """
        Returns the schema id associated with a specified
        schema from Splice Machine DB
        """
        schema_id_query = sql.select([self.sys_schemas.c.schemaid],
            self.sys_schemas.schemaname == schemaName)
        c = connection.execute(schema_id_query)
        if c.first():
            print("Schema ID: " + str(c.first()))
            return c.first()[0]
        else:
            return None
    
    def get_schema_id_or_default(self, schemaName, connection):
        """
        Returns default schema id if schemaName is none,
        otherwise, returns the schema id for the specified 
        schemaName
        """
        if schemaName:
            schema = schemaName.upper()
        else:
            schema = self.default_schema_name.upper()
        
        return self.get_schema_id(schema, connection)
    
    def has_table(self, connection, table_name, schema=None):
        """
        Return if table exists in DB
        """
        t = time.time()
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
            whereclause = sql.and_(self.sys_tables_view.c.schemaname == current_schema,
                                   self.sys_tables_view.c.tablename == table_name)
        else:
            whereclause = self.sys_tables_view.c.tablename == table_name
        s = sql.select([self.sys_tables_view.c.tablename], whereclause)
        c = connection.execute(s)
        out = c.first() is not None
        print("Function has table ran in {} ms".format((time.time() - t) * 1000))
        return out

    def has_sequence(self, connection, sequence_name, schema=None):
        """
        Returns if sequence exists in DB
        """
        print("has sequence " + sequence_name)
        schema_id = self.get_schema_id_or_default(schema, connection)

        current_schema = self.denormalize_name(schema or self.default_schema_name)
        sequence_name = self.denormalize_name(sequence_name)
        if current_schema:
            whereclause = sql.and_(self.sys_sequences.c.sequenceschemaid == schema_id,
                                   self.sys_sequences.c.sequencename == sequence_name)
        else:
            whereclause = self.sys_sequences.c.sequencename == sequence_name
        s = sql.select([self.sys_sequences.c.sequencename], whereclause)
        c = connection.execute(s)
        return c.first() is not None

    def get_schema_names(self, connection, **kw):
        """
        Get schema names in DB 
        """
        sysschema = self.sys_schemas
        query = sql.select([sysschema.c.schemaname],
            sql.not_(sysschema.c.schemaname.like('SYS%')),
            order_by=[sysschema.c.schemaname]
        )
        return [self.normalize_name(r[0]) for r in connection.execute(query)]


    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """
        Get table names in DB
        """
        print("Get table names: " + str(schema))
        schema_id = self.get_schema_id_or_default(schema, connection)
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        systbl = self.sys_tables
        query = sql.select([systbl.c.tablename]).\
                    where(systbl.c.tabletype == 'T').\
                    where(systbl.c.schemaid == schema_id).\
                    order_by(systbl.c.tablename)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    def get_table_name(self, connection, tableid):
        """
        Get name of a table given its ID
        """
        query = sql.select([self.sys_tables.tablename]).where(
              self.sys_tables.tableid == tableid)

        result = connection.execute(query).first()
        if result:
            return result[0]
        return None
      
    def get_table_id(self, connection, tablename):
        """
        Get ID of table given its name
        """
        query = sql.select([self.sys_tables.tableid]).where(
              self.sys_tables.tablename == tablename)

        result = connection.execute(query).first()
        if result:
            return result[0]
        return None

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """
        Get the names of all views under the specified schema
        #FIXME :: This would be much simpler if we could figure out
        joins in sql level SQLAlchemy
        """
        print("Get view names: " + str(schema))
        schemaid = self.get_schema_id_or_default(schema, connection)

        current_schema = self.denormalize_name(schema or self.default_schema_name)

        query = sql.select([self.sys_views.c.tableid]).\
            where(self.sys_views.c.compilationschemaid == schemaid)

        tableids = connection.execute(query)
        print("Table IDS: " + str(tableids))

        out = []
        for tableid in tableids:
            table_name = self.get_table_name(connection, tableid[0])
            if table_name:
              out.append(table_name)

        return [self.normalize_name(r[0]) for r in out]

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        """
        Get definition of a view from its name
        """
        print("getting view definition: " + str(schema) + "." + str(viewname))
        schemaid = self.get_schema_id_or_default(schema, connection)

        current_schema = self.denormalize_name(schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)
        tableid = self.get_table_id(connecton, viewname)
        query = sql.select([self.sys_views.c.viewdefinition]).\
            where(self.sys_views.c.compilationschemaid == schemaid).\
            where(self.sys_views.c.tableid == tableid)

        return connection.execute(query).scalar()

    def get_columns_from_db(connection, schema, table, col_indices=[]):
        """
        Temporary workaround for getting column information
        from database. We must specify datatype=odbc or we
        will receive a segmentation fault by trying to get a
        java object
        """
        query = "CALL SYSIBM.SQLCOLUMNS(null, '{schema}', \
            '{table}', null, 'DATATYPE'='ODBC')"


        results = []
        for res in connection.execute(query):
            results.append([res[ci] for ci in col_indices])

        return results

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        print("Get columns: " + str(schema) + "." + str(table_name))
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)

        INDICES = [3, 5, 12, 10, 6, 8, 22, 23]
        column_data = self.get_columns_from_db(connection, current_schema, table,
            col_indices=INDICES)

        sa_columns = []
        for r in column_data:
            coltype = r[1].upper()
            if coltype in ['DECIMAL', 'NUMERIC']:
                coltype = self.ischema_names.get(coltype)(int(r[4]), int(r[5]))
            elif coltype in ['CHARACTER', 'CHAR', 'VARCHAR']:
                coltype = self.ischema_names.get(coltype)(int(r[4]))
            else:
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                            (coltype, r[0]))
                    coltype = coltype = sa_types.NULLTYPE

            sa_columns.append({
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
        """
        query = "CALL SYSIBM.SQLPRIMARYKEYS(null, '{schema}', '{table}', null)"
        results = [i[3] for i in list(connection.execute(query))] # get primary keys
        return results

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        print("Get primary keys: " + str(schema) + "." + str(table))
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)

        primary_keys = self.get_primary_keys_from_table(connection, current_schema, table)
        
        return [self.normalize_name(col) for col in primary_keys]


    def get_foreign_keys_from_db(self, connection, schema, table, imported=True):
        if imported:
            query = "CALL SYSIBM.SQLFOREIGNKEYS('',null,'','','{schema}','{table}','IMPORTEDKEY=1')"
        else:
            query = "CALL SYSIBM.SQLFOREIGNKEYS('','{schema}','{table}','',null,'','EXPORTEDKEY=1'"

        return list(connection.execute(query))

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        print("Get foreign keys: " + str(schema) + "." + str(table))
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        
        results = self.get_foreign_keys_from_db(connection, current_schema, table_name, imported=False)
        # fix me
        print("FK 1 Results: " + str(results))
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
        print("Get foreign (e) keys: " + str(schema) + "." + str(table))
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

