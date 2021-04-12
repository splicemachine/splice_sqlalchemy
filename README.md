# Splice Machine SQLAlchemy Adapter

[![PyPI version](https://badge.fury.io/py/splicemachinesa.svg)](https://badge.fury.io/py/splicemachinesa)

## Description:
* This python package allows users to integrate new and existing 
SQLAlchemy applications with Splice Machine
* SQLAlchemy exposes a uniform API for database-backed applications,
which can be easily used with Splice Machine via this adaptor
* Supported on Splice Machine Versions >= 2.8.0.1920
* Requires Python 3.6+
## Installation:

There are two ways to install this Driver. With and without ODBC Driver custom installation. We recommend without. When running these commands, you must either use sudo or have root access.

### Without ODBC Driver Custom Installation
#### Mac
`brew install unixodbc`
`pip install splicemachinesa`
#### Linux
First ensure you have the following packages installed. They are necessary for pyodbc.<br>
Ubuntu:
* build-essential
* libssl-dev
* libffi-dev
* unixodbc
* unixodbc-dev
* python3-dev
* curl
* unzip
* iodbc

`sudo apt -y update && sudo apt -y upgrade && sudo apt -y install build-essential libssl-dev libffi-dev unixodbc unixodbc-dev python3-dev curl unzip iodbc`

Centos:
* unzip
* gcc
* openssl-devel
* gcc-c++
* unixODBC-devel
* unixODBC
* python3-devel

`sudo yum -y update && sudo yum -y install unzip gcc openssl-devel gcc-c++ unixODBC-devel unixODBC python3-devel`

Then run `pip install splicemachinesa`
<br>

**Note:** If you have an error during installation, it is likely with the installation of PyODBC. When searching the error, reference PyODBC in the search.

### With Custom ODBC Driver Installation
#### ODBC Driver:
First, download the appropriate [Splice Machine ODBC](https://doc.splicemachine.com/tutorials_connect_odbcinstall.html) driver for your system.
<hr><br>
Mac OSX 64 bit Driver: <a href="https://splice-releases.s3.amazonaws.com/odbc-driver/MacOSX64/splice_odbc_macosx64-2.8.73.0.tar.gz">Download</a><br>
Linux 64 bit Driver: <a href="https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-2.8.73.0.tar.gz">Download</a><br>
<br><b>Note: Windows is not currently supported for Splice Machine SQLAlchemy ODBC</b><br>
<hr><br>
Then, follow the instructions <a href="https://doc.splicemachine.com/tutorials_connect_odbcinstall.html">here</a> to finish the installation, but use the download links above to retrieve the Driver tarball
<br><hr>

Once the Driver is installed, you can pip install splicemachinesa.

<b>NOTE: If you are on Mac, you must install unixODBC</b><br>
`brew install unixodbc`

```
pip install splicemachinesa
```

### Usage

You can use this package for SqlAlchemy usage or raw ODBC usage

#### ODBC Connection Only (Basic Auth)
```
from splicemachinesa.pyodbc import splice_connect
ODBC_CONNECTION = splice_connect(URL=[URL], UID=[UID], PWD=[PWD], SSL=[SSL])
```
Filling in `UID`, `PWD`, `URL`, and `SSL` with the proper values for your database. SSL defaults to 'basic' If you are connecting to the Splice Database _inside_ the same network (ie standalone splice) you will set `SSL=None`

#### ODBC Connection Only (JWT Connection)
```
from splicemachinesa.pyodbc import splice_connect
ODBC_CONNECTION = splice_connect(URL=[URL], JWT_TOKEN=[JWT_TOKEN], JWT_TYPE=[JWT_TYPE], SSL=[SSL])
```
Filling in `JWT_TOKEN`, `JWT_TYPE`, `URL`, and `SSL` with the proper values for your database. SSL defaults to 'basic' If you are connecting to the Splice Database _inside_ the same network (ie standalone splice) you will set `SSL=None`<br>
You must set the JWT_TYPE to use this format. Available options are ('SPLICE_JWT', 'SPLICE_JWT_PUB', 'OKTA_OAUTH', 'SPLICE_OAUTH')


#### SqlAlchemy

There are three URL formats that can be used to access 
SpliceMachine via ODBC through SQLAlchemy. The first will suffice in most cases.
<hr>

#### 1: Standard URL format<br>
Format: `splicemachinesa://[user]:[password]@[host]:[port]/[database]`<br><br>
The Driver Name defaults to `SpliceODBCDriver`, which is the default name 
when the driver is installed (specified in `odbc.ini`). However, this name can be overridden through the 
`SPLICE_ODBC_DRIVER_NAME` environment variable.

Example:
```
from sqlalchemy import create_engine
url = 'splicemachinesa://splice:admin@127.0.0.1:1527/splicedb'
engine = create_engine(url)
```

#### 2. Custom Configuration Without Driver
Format: `splicemachinesa://?DRIVER=[driver]&URL=[URL]&PORT=[PORT]&UID=[USER]&PWD=[PASSWORD]`
<br><br>You can use this SQLAlchemy driver without an ODBC configuration (meaning running the installation from Splice Machine docs)
by manually specifying the driver path.

* On OSX (given installation with `sudo` ), the Driver is found in `/Library/ODBC/SpliceMachine/libsplice_odbc64.dylib`
* On OSX, Without `sudo` this driver is located at `$HOME/Library/ODBC/SpliceMachine/libsplice_odbc64.dylib`
* On Linux (given ODBC Driver installation with `sudo`), this is found in `/usr/local/splice/libsplice_odbc.so`
* On Linux, Without `sudo` it is found in `$HOME/splice`.

<br>
We provide a utility function for simplifying the URL building process. <br>
This method is easier for an automated driver configuration (e.g. inside Docker) because you only need
to copy to Driver binary. <br><br>Note: Error messages will not be rendered properly with this approach.


Example Linux:
```
from sqlalchemy import create_engine
from splicemachinesa.utilities import url_builder

url = url_builder('/usr/local/splice/lib/libsplice_odbc.so', host='localhost', port=1527,
user='splice', password='admin')
 
engine = create_engine(url) 
```

Example Mac:
```
from sqlalchemy import create_engine
from splicemachinesa.utilities import url_builder

url = url_builder('/Library/ODBC/SpliceMachine/libsplice_odbc64.dylib', host='localhost', port=1527, 
user='splice', password='admin')
 
engine = create_engine(url) 
```

#### 3. DSN Configuration
Format: `splicemachinesa://[dsn]`
Splice Machine SQLAlchemy also supports ODBC DSNs for 
Driver configuration. This means that rather than explicitly specifying
configuration options in the URL string, they are rendered from the odbc.ini file. This is also the only method that 
supports Kerberos Authentication for Splice Clusters. You can see how to use Kerberos 
<a href="https://doc.splicemachine.com/developers_fundamentals_haproxy.html">here</a>. The `odbc.ini` file is located 
at these locations depending on whether or not the installer was run as root.<br><br>
With/without `sudo` installation- Mac OSX: `$HOME/.odbc.ini`<br>
With `sudo` Linux: `/etc/odbc.ini`<br>
Without `sudo` Linux: `$HOME/.odbc.ini`. In this file, if you have a key named `USER`,
rename this to `UID`. <br><br>The default DSN is `SpliceODBC64`.

Example:
```
from sqlalchemy import create_engine
url = 'splicemachinesa://SpliceODBC64'
engine = create_engine(url)
```


#### Testing
1) First make sure you have a fresh
installation of Splice Machine
running (either Standalone or Cloud/Bespoke/On-Prem)
with an appropriate SQLAlchemy Splice Machine connection
string for accessing it

```
# register any changes for testing
cd /path/to/splice_sqlalchemy
sudo pip install .
sudo pip install pytest
pytest -vv --db-url 'splicemachinesa://[...]'
```

#### Features not yet supported
- Common Table Expressions
- Indices Reflection
- Check + Unicode Reflection
- Nullable Reflection
- ORDER BY COLLATE
- Empty Set Insertion
- Unicode String Support
- Reflector for Order by Foreign Key
- limit_offset_in_unions_from_alias
