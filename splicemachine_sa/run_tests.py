from sqlalchemy.dialects import registry

registry.register("splicemachine_sa", "splicemachine_sa.splice_machine", "SpliceMachineDialect_sm")
registry.register("splicemachine_sa.ibm_db", "ibm_db_sa.ibm_db", "SpliceMachineDialect_sm")
registry.register("splicemachine_sa.pyodbc", "ibm_db_sa.pyodbc", "SpliceMachineDialect_pyodbc")

from sqlalchemy.testing import runner

runner.main()

