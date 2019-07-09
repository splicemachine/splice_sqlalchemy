from . import splice_machine, pyodbc, base

###################
# Package Version #
###################

VERSION = "0.0.4"
__VERSION__ = VERSION  # alias

# default dialect
base.dialect = splice_machine.dialect
