#!/usr/bin/env python

from setuptools import setup

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

VERSION = '0.1.3'

setup(
    name='splicemachinesa',
    version=VERSION,
    url='https://splicemachine.com',
    keywords=['sqlalchemy', 'splice', 'machine', 'splice machine', 'odbc', 'driver', 'sqlalchemy driver'],
    license='Apache License 2.0',
    description='SQLAlchemy support for Splice Machine RDBMS',
    author='Amrit Baveja',
    author_email='abaveja@splicemachine.com',
    download_url='https://splice-releases.s3.amazonaws.com/splice-sqlalchemy/splicemachinesa-{version}.dev0.tar.gz'.format(
	version=VERSION),
    platforms='All',
    install_requires=[
        'sqlalchemy',
        'pyodbc>=4.0.26'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Topic :: Database'
    ],
    packages=['splicemachinesa'],
    entry_points={
        'sqlalchemy.dialects': [
            'splicemachinesa=splicemachinesa.pyodbc:SpliceMachineDialect_pyodbc',
            'splicemachinesa.splice_machine=splicemachinesa.splice_machine:SpliceMachineDialect_sm',
            'splicemachinesa.pyodbc=splicemachinesa.pyodbc:SpliceMachineDialect_pyodbc',
        ]
    },
    long_description_content_type='text/markdown',
    long_description=open('README.md').read(),
    zip_safe=False,
    tests_require=['nose >= 0.11']
)
