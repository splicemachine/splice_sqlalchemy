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

Some parts of this source code are based on Apache Derby, and the following notices apply to
Apache Derby:

Apache Derby is a subproject of the Apache DB project, and is licensed under
the Apache License, Version 2.0 (the "License"); you may not use these files
except in compliance with the License. You may obtain a copy of the License at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

Splice Machine, Inc. has modified the Apache Derby code in this file.

All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
and are licensed to you under the GNU Affero General Public License.
"""



"""
External utilities
to make using the SpliceMachineSa
driver easier
"""

def url_builder(odbc_driver_location, host='0.0.0.0', port=1527, user='splice', password='admin', ssl=None):
    """
    Build the SpliceMachineSA DB URL for SQLAlchemy create_engine

    :param odbc_driver_location: (str) the local path to the platform
        specific ODBC driver (found in splice-odbc bucket in S3)
    :param host: (str) the Splice Machine database host
    :param port: (int) the port on which Splice Machine is running
    :param user: (str) the username for the database
    :param password: (str) the password for the database
    :param ssl: (str) the SSL for the connection. If connection locally, use None. If connecting to an external DB,
        use 'basic'

    :returns: (str) create_engine connection string
    """
    return 'splicemachinesa+pyodbc:///?DRIVER={driver_loc}&PORT={port}\
&PWD={password}&UID={user}&URL={host}&ssl={ssl}'.format(
        driver_loc=odbc_driver_location, port=port,
        password=password, user=user, host=host, ssl=ssl
    )
