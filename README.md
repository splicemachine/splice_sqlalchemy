## Splice Machine SQLAlchemy Adapter
### Description:
* This python package allows users to integrate new and existing 
SQLAlchemy applications with Splice Machine
* SQLAlchemy exposes a uniform API for database-backed applications,
which can be easily used with Splice Machine via this adaptor
* Supported on Splice Machine Versions >= 2.8.0.1920
### Installation:
#### ODBC Driver:
First, download the appropriate Splice Machine ODBC driver for your system.
<hr><br>
Mac OSX 64 bit Driver: <a href="https://splice-releases.s3.amazonaws.com/odbc-driver/MacOSX64/splice_odbc_macosx64-2.7.60.0.tar.gz">Download</a><br>
Linux 32 bit Driver: <a href="https://splice-releases.s3.amazonaws.com/odbc-driver/Linux32/splice_odbc_linux32-2.7.58.0.tar.gz">Download</a><br>
Linux 64 bit Driver: <a href="https://splice-releases.s3.amazonaws.com/odbc-driver/Linux64/splice_odbc_linux64-2.7.60.0.tar.gz">Download</a><br>
<br><b>Note: Windows is not currently supported for Splice Machine SQLAlchemy ODBC</b><br>
<hr><br>
Then, follow the instructions <a href="https://doc.splicemachine.com/tutorials_connect_odbcinstall.html">here</a> to finish the installation, but use the download links above to retrieve the Driver tarball
<br><hr>

Once the Driver is installed, you can pip install splicemachinesa

```
sudo pip install splicemachinesa
```

### Usage
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
by manually specifying the driver path. On OSX (given installation with `sudo` ), this is found in `/Library/ODBC/SpliceMachine/libsplice_odbc64.dylib.`
Without `sudo` this located at the same path, but in the home directory e.g. `$HOME/Library/...`
On Linux (given ODBC Driver installation with `sudo`), this is found in `/usr/local/splice/lib/libsplice_odbc.so`. Without `sudo`
it is found in `$HOME/splice`. We provide a utility function for simplifying the URL building process. <br>
This method is easier for an automated driver configuration (e.g. inside Docker) because you only need
to copy to Driver binary. <br><br>Note: Error messages will not be rendered properly with this approach.


Example:
```
from sqlalchemy import create_engine
from splicemachinesa.utilities import url_builder

url = url_builder('/usr/local/splice/lib/libsplice_odbc.so', host=[0.0.0.0], port=[1527]
 user=['splice'], password=['admin'])
 
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
py.test -vv --db-url 'splicemachinesa://[...]'
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