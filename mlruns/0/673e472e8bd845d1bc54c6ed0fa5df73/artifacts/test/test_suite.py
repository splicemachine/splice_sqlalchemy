# test/test_suite.py

from sqlalchemy.testing.suite import *
from sqlalchemy.inspection import inspect
import sqlalchemy.testing as testing
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
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
