#!/usr/bin/env python

from setuptools import setup
import os
import re

VERSION = "0.0.1"

setup(
     name='splicemachine_sa',
     version=VERSION,
     license='Apache License 2.0',
     description='SQLAlchemy support for Splice Machine RDBMS - Based on IBM DB2 Driver',
     author='IBM Application Development Team, Amrit Baveja',
     author_email='abaveja@splicemachine.com',
     platforms='All',
     install_requires=['sqlalchemy>=0.7.3','ibm_db>=2.0.0'],
     packages=['splicemachine_sa'],
     entry_points={
     'sqlalchemy.dialects': [
                 'splicemachine_sa=splice_machine_sa.splice_machine:SpliceMachineDialect_sm',
                 'splicemachine_sa.splice_machine=splicemachine_sa.ibm_db:SpliceMachineDialect_sm',
                 'splicemachine_sa.pyodbc=splicemachine_sa.pyodbc:SpliceMachineDialect_pyodbc',
                ]
   },
   zip_safe=False,
   tests_require=['nose >= 0.11'],
)
