### IfxAlchemy Scratchpad

Notas rapidas para mantener este fork alineado con SQLAlchemy 2.0 y con el
dialecto Informix actual del repo.

### Estado actual
- Python: `>=3.10`
- SQLAlchemy: `>=2.0,<2.1`
- Driver por defecto del paquete: `pyodbc`
- Dialecto registrado por defecto: `informix -> IfxAlchemy.pyodbc:IfxDialect_pyodbc`

### Referencias oficiales
- SQLAlchemy Unified Tutorial: https://docs.sqlalchemy.org/20/tutorial/
- Establishing Connectivity - the Engine: https://docs.sqlalchemy.org/20/tutorial/engine.html
- Engine Configuration / Database URLs: https://docs.sqlalchemy.org/20/core/engines.html
- Working with Engines and Connections: https://docs.sqlalchemy.org/20/core/connections.html
- ORM Quick Start: https://docs.sqlalchemy.org/20/orm/quickstart.html

### URL de conexion recomendada

Patron actual para este repo con `pyodbc`:

```python
from sqlalchemy import create_engine

DATABASE_URL = (
    "informix+pyodbc://USER:PASSWORD@HOST/DATABASE"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&server=INFORMIX_SERVER"
    "&protocol=onsoctcp"
    "&service=9088"
)

engine = create_engine(DATABASE_URL)
```

Ejemplo real usado en scratchpad:

```python
DATABASE_URL = (
    "informix+pyodbc://ctl:magogo@192.168.11.64/faempre999"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&server=pru_famadesa_s9"
    "&protocol=onsoctcp"
    "&service=9088"
)
```

### URL que no conviene usar

Esto es legacy y hoy no encaja bien con el dialecto `pyodbc` actual:

```python
ConStr = "informix://informix:Blue4You@127.0.0.1:9088/db1;SERVER=ids0"
```

Con esa forma, SQLAlchemy interpreta `db1;SERVER=ids0` como parte de
`database`, no como parametro `server`.

### Formas soportadas por el dialecto pyodbc

1. Parametros explicitos en la URL:

```python
informix+pyodbc://USER:PASSWORD@HOST/DATABASE?driver=...&server=...&protocol=...&service=...
```

2. DSN:

```python
informix+pyodbc://USER:PASSWORD@MY_DSN
```

3. `odbc_connect` completo:

```python
informix+pyodbc:///?odbc_connect=DRIVER%3D%7BIBM+INFORMIX+ODBC+DRIVER+%2864-bit%29%7D%3BHOST%3D...%3BSERVICE%3D...%3BSERVER%3D...%3BDATABASE%3D...
```

### SQLAlchemy 2.0: patrones correctos

SQL textual:

```python
with engine.begin() as conn:
    conn.exec_driver_sql("SELECT FIRST 1 tabname FROM systables ORDER BY tabname")
```

Si se usa `Connection.execute()`, debe ser con `text(...)`:

```python
from sqlalchemy import text

with engine.connect() as conn:
    row = conn.execute(
        text("SELECT FIRST 1 tabname FROM systables ORDER BY tabname")
    ).first()
```

ORM moderno:

```python
from sqlalchemy import select
from sqlalchemy.orm import Session

with Session(engine) as session:
    row = session.execute(select(SysTable)).scalars().first()
```

### Notas del repo

- `import_dbapi()` ya sustituye a `dbapi()` en los dialectos.
- El reflector usa SQL valido de Informix para el esquema por defecto:

```sql
SELECT USER FROM systables WHERE tabid = 1
```

- En rutas con SQL textual crudo, el patron correcto es `exec_driver_sql()`.
- El paquete sigue conservando una ruta `IfxPy`, pero el camino principal hoy es
  `pyodbc`.

### Scripts utiles del scratchpad
- `scratchpad/test-ifx-sa.py`: prueba simple de conexion y consulta textual.
- `scratchpad/test-ifx-system.py`: prueba de mapeo ORM sobre `systables`.

### Pendientes tecnicos a vigilar
- Revisar si la ruta `IfxPy` sigue necesitando workarounds por `get_current_schema()`.
- Revisar si merece la pena modernizar `scratchpad/test-ifx-system.py` de
  `session.query(...)` a `select(...)`.
- Mantener README y scratchpad alineados con el dialecto por defecto `pyodbc`.
