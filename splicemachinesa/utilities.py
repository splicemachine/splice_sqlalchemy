"""
Copyright 2019 Splice Machine Inc.

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

"""
External utilities
to make using the SpliceMachineSa
driver easier
"""


def url_builder(odbc_driver_location, host='0.0.0.0', port=1527, user='splice', password='admin'):
    """
    Build the SpliceMachineSA DB URL for SQLAlchemy create_engine

    :param odbc_driver_location: (str) the local path to the platform
        specific ODBC driver (found in splice-odbc bucket in S3)
    :param host: (str) the Splice Machine database host
    :param port: (int) the port on which Splice Machine is running
    :param user: (str) the username for the database
    :param password: (str) the password for the database

    :returns: (str) create_engine connection string
    """
    return 'splicemachinesa+pyodbc:///?DRIVER={driver_loc}&PORT={port}\
&PWD={password}&UID={user}&URL={host}'.format(
        driver_loc=odbc_driver_location, port=port,
        password=password, user=user, host=host
    )
