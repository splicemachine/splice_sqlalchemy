from sqlalchemy import util
from sqlalchemy.connectors.pyodbc import PyODBCConnector
import urllib
from .base import _SelectLastRowIDMixin, SpliceMachineExecutionContext, SpliceMachineDialect

"""
Copyright 2019 Amrit Baveja

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

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
