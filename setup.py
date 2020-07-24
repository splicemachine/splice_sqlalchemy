#!/usr/bin/env python

from setuptools import setup
from setuptools.command.install import install as Install
from platform import system
from subprocess import check_call as run_bash

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

VERSION = '0.1.5.1'
ODBC_VERSION = '2.8.66.0'

def bash(command):
    run_bash(command.split())

class CustomInstall(Install):
    linux_url = 'https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-2.8.66.0.tar.gz'
    window_url = 'https://splice-releases.s3.amazonaws.com/odbc-driver/Win64/splice_odbc_setup_64bit_2.8.66.0.msi'
    mac_url = 'https://splice-releases.s3.amazonaws.com/odbc-driver/MacOSX64/splice_odbc_macosx64-2.8.66.0.tar.gz'
    nix_url = {'Darwin': mac_url, 'Linux': linux_url}
    def nix_hanlder(self):
        url = CustomInstall.nix_url[system()]
        file_name = url.split('/')[-1]
        bash('mkdir -p /tmp')
        bash('curl -kLs {url} -o /tmp/{file_name}'.format(url=url,file_name=file_name))
        bash('tar -xzf /tmp/{} -C /tmp'.format(file_name))
        driver_name = 'libsplice_odbc64.dylib' if system()=='Darwin' else 'lib64/libsplice_odbc.so'
        driver_location = '/Library/ODBC/SpliceMachine/' if system()=='Darwin' else '/usr/local/splice'
        file_name = file_name.rstrip('.tar.gz')
        bash('mkdir -p {}'.format(driver_location))
        bash('mv -f /tmp/{file_name}/{driver_name} {driver_location}'.format(file_name=file_name, driver_name=driver_name,
                                                                          driver_location=driver_location))

        bash('mkdir -p /usr/local/splice/errormessages/en-US/')
        bash('cp /tmp/{file_name}/errormessages/en-US/*.xml /usr/local/splice/errormessages/en-US/'.format(file_name=file_name) )

    def windows_handler(self):
        pass

    def run(self):
        Install.run(self)
        {'Linux': self.nix_hanlder,
         'Windows': self.windows_handler,
         'Darwin': self.nix_hanlder}[system()]()


setup(
    name='splicemachinesa',
    version=VERSION,
    url='https://splicemachine.com',
    keywords=['sqlalchemy', 'splice', 'machine', 'splice machine', 'odbc', 'driver', 'sqlalchemy driver'],
    license='Apache License 2.0',
    description='SQLAlchemy support for Splice Machine RDBMS',
    author='Amrit Baveja',
    author_email='abaveja@splicemachine.com',
    download_url='https://splice-releases.s3.amazonaws.com/splice-sqlalchemy/splicemachinesa-{version}.dev1.tar.gz'.format(
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
    tests_require=['nose >= 0.11'],
    cmdclass={'install': CustomInstall}
)
