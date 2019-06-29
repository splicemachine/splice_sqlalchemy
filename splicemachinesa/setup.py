#!/usr/bin/env python

from setuptools import setup
import os
import re

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

VERSION = "0.0.2"

setup(
     name='splicemachinesa',
     version=VERSION,
     license='Apache License 2.0',
     description='SQLAlchemy support for Splice Machine RDBMS',
     author='IBM Application Development Team, Amrit Baveja',
     author_email='abaveja@splicemachine.com',
     platforms='All',
     install_requires=['sqlalchemy>=0.7.3','ibm_db>=2.0.0'],
     packages=['splicemachinesa'],
     entry_points={
     'sqlalchemy.dialects': [
                 'splicemachinesa=splicemachinesa.splice_machine:SpliceMachineDialect_sm',
                 'splicemachinesa.splice_machine=splicemachinesa.splice_machine:SpliceMachineDialect_sm',
                 'splicemachinesa.pyodbc=splicemachinesa.pyodbc:SpliceMachineDialect_pyodbc',
                ]
   },
   zip_safe=False,
   tests_require=['nose >= 0.11'],
)
