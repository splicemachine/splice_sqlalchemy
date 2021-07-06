#!/usr/bin/env python

from setuptools import setup
from setuptools.command.install import install as Install
from platform import system
from subprocess import check_call as run_bash
import os

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


VERSION = '0.4.3'
ODBC_VERSION = '2.8.77.0'

def bash(command):
    run_bash(command.split())

class CustomInstall(Install):
    linux_url = 'https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-{}.tar.gz'.format(ODBC_VERSION)
    window_url = 'https://splice-releases.s3.amazonaws.com/odbc-driver/Win64/splice_odbc_setup_64bit_{}.msi'.format(ODBC_VERSION)
    mac_url = 'https://splice-releases.s3.amazonaws.com/odbc-driver/MacOSX64/splice_odbc_macosx64-{}.tar.gz'.format(ODBC_VERSION)
    nix_url = {'Darwin': mac_url, 'Linux': linux_url}
    def nix_hanlder(self):
        home = os.environ.get('HOME', '~') + '/splice'
        url = CustomInstall.nix_url[system()]
        file_name = url.split('/')[-1]
        bash('mkdir -p /tmp')
        bash('curl -kLs {url} -o /tmp/{file_name}'.format(url=url,file_name=file_name))
        bash('tar -xzf /tmp/{} -C /tmp'.format(file_name))
        driver_name = 'libsplice_odbc64.dylib' if system()=='Darwin' else 'lib64/libsplice_odbc.so'
        # driver_location = '/Library/ODBC/SpliceMachine' if system()=='Darwin' else '/usr/local/splice'
        file_name = file_name.rstrip('.tar.gz')
        bash('mkdir -p {}'.format(home))
        bash('mv -f /tmp/{file_name}/{driver_name} {driver_location}'.format(file_name=file_name, driver_name=driver_name,
                                                                          driver_location=home))

        bash('mkdir -p {}/en-US/'.format(home))
        err_files = os.popen('ls /tmp/{}/errormessages/en-US'.format(file_name)).read().split()
        for xml in err_files:
            bash('cp /tmp/{file_name}/errormessages/en-US/{xml} {driver_location}/en-US/'.format(file_name=file_name, xml=xml,driver_location=home))

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
    tests_require=['nose >= 0.11'],
    cmdclass={'install': CustomInstall}
)
