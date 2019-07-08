# splicemachinesa
## Splice Machine SQLAlchemy Adapter
#### Description:
* This python package allows users to integrate new and existing 
SQLAlchemy applications with Splice Machine
* SQLAlchemy exposes a uniform API for database-backed applications,
which can be easily used with Splice Machine via this adaptor

#### Installation:
```
sudo pip install pyodbc sqlalchemy # install dependencies
sudo pip install git+https://github.com/splicemachine/splice_sqlalchemy
```
#### Usage
```
from sqlalchemy import create_engine
from splicemachinesa.utilities import url_builder

db_url = url_builder('/path/to/platform/specific/odbc/driver',
    host='0.0.0.0', port=1527, user='splice', 
    password='<password>'
)

engine = create_engine(db_url)
```

#### Testing
1) First make sure you have a fresh
installation of standalone running on
localhost:1527 with the default database 
credentials

```
cd /path/to/splice_sqlalchemy
sudo pip install .
sudo pip install pytest
py.test -vv .
```