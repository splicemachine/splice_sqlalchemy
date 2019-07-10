from . import splice_machine, pyodbc, base
from ..setup import VERSION

__VERSION__ = VERSION  # alias

###################
# Package Version #
###################

base.dialect = splice_machine.dialect
