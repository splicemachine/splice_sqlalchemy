from sqlalchemy import util
from sqlalchemy.connectors.pyodbc import PyODBCConnector
import urllib
from .base import _SelectLastRowIDMixin, SpliceMachineExecutionContext, SpliceMachineDialect

class SpliceMachineExecutionContext_pyodbc(_SelectLastRowIDMixin, SpliceMachineExecutionContext):
    pass

class SpliceMachineDialect_pyodbc(PyODBCConnector, SpliceMachineDialect):
    """
    ODBC dialect for Splice Machine SQLAlchemy Driver
    """
    supports_unicode_statements = True
    supports_char_length = True
    supports_native_decimal = False

    execution_ctx_cls = SpliceMachineExecutionContext_pyodbc

    pyodbc_driver_name = "SM ODBC DRIVER"
