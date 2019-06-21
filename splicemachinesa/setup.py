#!/usr/bin/env python

from setuptools import setup
import os
import re

VERSION = "0.0.1"

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
