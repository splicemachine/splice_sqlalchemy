from sqlalchemy.connectors.pyodbc import PyODBCConnector, util
from pyodbc import connect as odbc_connect
from platform import system
from .base import _SelectLastRowIDMixin, SpliceMachineExecutionContext, SpliceMachineDialect
import os
import re

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



HOME = os.environ.get('HOME','~') + '/splice'
DRIVER_LOCATIONS = {
    'Darwin': f'{HOME}/libsplice_odbc64.dylib',
    'Linux': f'{HOME}/libsplice_odbc.so'
}
def splice_connect(UID, PWD, URL, PORT='1527', SSL='basic', Driver=None):
    Driver = Driver or DRIVER_LOCATIONS[system()]
    ODBC_CONNECTION = odbc_connect(Driver=Driver,UID=UID,PWD=PWD,URL=URL,PORT=PORT,SSL=SSL)
    return ODBC_CONNECTION

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

    # check for overridden ODBC driver name

    if os.environ.get('SPLICE_ODBC_DRIVER_NAME'):
        pyodbc_driver_name = os.environ['SPLICE_ODBC_DRIVER_NAME']
    else:
        pyodbc_driver_name = "SpliceODBCDriver"

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user")
        opts.update(url.query)

        keys = opts

        query = url.query

        connect_args = {}
        for param in ("ansi", "unicode_results", "autocommit"):
            if param in keys:
                connect_args[param] = util.asbool(keys.pop(param))

        if "odbc_connect" in keys:
            connectors = [util.unquote_plus(keys.pop("odbc_connect"))]
        else:

            def check_quote(token):
                if ";" in str(token):
                    token = "'%s'" % token
                return token

            keys = dict((k, check_quote(v)) for k, v in keys.items())

            dsn_connection = "dsn" in keys or (
                    "host" in keys and "database" not in keys
            )
            if dsn_connection:
                connectors = [
                    "dsn=%s" % (keys.pop("host", "") or keys.pop("dsn", ""))
                ]
            else:
                port = ""
                if "port" in keys and "port" not in query:
                    port = int(keys.pop("port"))

                connectors = []
                driver = keys.pop("driver", self.pyodbc_driver_name)
                if driver is None:
                    util.warn(
                        "No driver name specified; "
                        "this is expected by PyODBC when using "
                        "DSN-less connections"
                    )
                else:
                    connectors.append("DRIVER={%s}" % driver)

                connectors.extend(
                    [
                        "Database=%s" % keys.pop("database", ""),
                    ]
                )
                if not "URL" in keys:
                    connectors.extend(
                        [
                            "URL=%s" % (keys.pop("host", "")),
                            "PORT=%s" % (port)
                        ]
                    )
            user = keys.pop("user", None)
            if user:
                connectors.append("UID=%s" % user)
                connectors.append("PWD=%s" % keys.pop("password", ""))
            else:
                connectors.append("Trusted_Connection=Yes")

            # if set to 'Yes', the ODBC layer will try to automagically
            # convert textual data from your database encoding to your
            # client encoding.  This should obviously be set to 'No' if
            # you query a cp1253 encoded database from a latin1 client...
            if "odbc_autotranslate" in keys:
                connectors.append(
                    "AutoTranslate=%s" % keys.pop("odbc_autotranslate")
                )

            connectors.extend(["%s=%s" % (k, v) for k, v in keys.items()])

        out = [[";".join(connectors)], connect_args]
        print(re.sub(r';PWD=\w+',';PWD=***',str(out)))
        return out
