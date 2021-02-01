# test/test_suite.py

from sqlalchemy.testing.suite import *
from sqlalchemy.inspection import inspect
import sqlalchemy.testing as testing
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest

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



"""
Overriden test cases from
SQLAlchemy

Running Test Cases 
-----------------
cd /path/to/spliceengine/
./start-splice-cluster # fresh db instance (no tables besides defaults)
./sqlshell.sh 
splice> CREATE SCHEMA test_schema;
<ctrl+c>

cd /path/to/package/dir
sudo pip install . pytest pyodbc
pytest -vv .
"""

class ComponentReflectionTest(_ComponentReflectionTest):
    def _test_get_foreign_keys(self, schema=None):
        # we just need to lowercase the assertion columns
        # in this test -- test is intact otherwise
        meta = self.metadata
        users, addresses, dingalings = (
            self.tables.users,
            self.tables.email_addresses,
            self.tables.dingalings,
        )
        insp = inspect(meta.bind)
        expected_schema = schema
        # users
        if testing.requires.self_referential_foreign_keys.enabled:
            users_fkeys = insp.get_foreign_keys(users.name, schema=schema)
            fkey1 = users_fkeys[0]
            if schema:
                eq_(fkey1["referred_schema"].lower(), expected_schema)
            else:
                eq_(fkey1["referred_schema"], None)
            eq_(fkey1["referred_table"].lower(), users.name)
            eq_(fkey1["referred_columns"][0].lower(), "user_id")
            if testing.requires.self_referential_foreign_keys.enabled:
                eq_(fkey1["constrained_columns"][0].lower(), "parent_user_id")

        # addresses
        addr_fkeys = insp.get_foreign_keys(addresses.name, schema=schema)
        fkey1 = addr_fkeys[0]
        if schema:
            eq_(fkey1["referred_schema"].lower(), expected_schema)
        else:
            eq_(fkey1["referred_schema"], None)

        eq_(fkey1["referred_table"].lower(), users.name)
        eq_(len(fkey1["referred_columns"]), 1)
        eq_(len(fkey1["constrained_columns"]), 1)

    def _test_get_pk_constraint(self, schema=None):
        # we just need to lowercase the assertion columns
        # in this test -- test is intact otherwise
        meta = self.metadata
        users, addresses = self.tables.users, self.tables.email_addresses
        insp = inspect(meta.bind)

        users_cons = insp.get_pk_constraint(users.name, schema=schema)
        users_pkeys = users_cons["constrained_columns"]
        eq_(users_pkeys[0].lower(), "user_id")

        addr_cons = insp.get_pk_constraint(addresses.name, schema=schema)
        addr_pkeys = addr_cons["constrained_columns"]
        eq_(addr_pkeys[0].lower(), "address_id")

    def test_get_table_names_fk(self):
        self.test_get_table_names()
    
    def test_get_table_names_fks(self):
        self.test_get_table_names()
