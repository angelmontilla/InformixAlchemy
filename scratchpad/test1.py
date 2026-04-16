
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry

# import IfxPyDbi as dbapi2

registry.register("informix",        "IfxAlchemy.IfxPy", "IfxDialect_IfxPy")
registry.register("informix.IfxPy",  "IfxAlchemy.IfxPy", "IfxDialect_IfxPy")
registry.register("informix.pyodbc", "IfxAlchemy.pyodbc", "IfxDialect_pyodbc")

import IfxAlchemy.IfxPy
import IfxAlchemy.pyodbc

ConStr = 'informix://<UserName>:<Password>@<HostName>:<Port Number>/<Database Name>;SERVER=<Server Name>'
engine = create_engine(ConStr)
with engine.begin() as connection:
    connection.exec_driver_sql('drop table if exists t1911')
    connection.exec_driver_sql('create table t1911(c1 int, c2 char(20), c3 float, c4 varchar(10))')
    connection.exec_driver_sql("insert into t1911 values(1, 'Sheetal', 12.01, 'Hello')")
    result = connection.exec_driver_sql('select * from  t1911')

    for row in result:
        print("c1:", row[0])
        print("c2:", row[1])
        print("c3:", row[2])
        print("c4:", row[3])
print( "Done2" )
