# IfxAlchemy

**IfxAlchemy** provides a SQLAlchemy dialect for IBM Informix.

This fork's supported backend is **`informix+pyodbc`** with the IBM Informix
ODBC driver. The old IfxPy module is kept only for compatibility tests and is
not part of the primary support contract.

## Support Matrix

| Component | Supported baseline |
| --- | --- |
| Python | 3.10, 3.11, 3.12, 3.13 |
| SQLAlchemy | `>=2.0.45,<2.2` |
| DBAPI | `pyodbc>=5.0` |
| Driver | IBM Informix ODBC Driver |
| Protocol | Informix over `onsoctcp` |

## SQLAlchemy Support Policy

Supported SQLAlchemy versions:

- SQLAlchemy `>=2.0.45,<2.2`
- SQLAlchemy 2.0.x supported
- SQLAlchemy 2.1.x supported

Supported backend:

- `informix+pyodbc`

Legacy backend:

- IfxPy is kept for compatibility tests only and is not part of the primary
  support contract.

Unsupported by contract:

- schema-qualified SQLAlchemy ownership model: `supports_schemas = False`
- temp table enumeration/reflection
- temporary views
- materialized views
- check constraint reflection
- `ON UPDATE CASCADE`
- portable microsecond reflection for generic `DateTime`
- unbounded `VARCHAR`
- SQLAlchemy `Identity()` DDL; integer primary keys compile to Informix
  `SERIAL`

## Connection Examples

URL with UID/PWD:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "informix+pyodbc://informix:in4mix@127.0.0.1/prueba4db"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&protocol=onsoctcp"
    "&server=informix"
    "&service=9088"
    "&DELIMIDENT=Y"
)
```

URL with a configured ODBC DSN:

```python
engine = create_engine(
    "informix+pyodbc://informix:in4mix@/"
    "?dsn=ifx_dev"
    "&DELIMIDENT=Y"
)
```

URL with Informix trusted context:

```python
engine = create_engine(
    "informix+pyodbc://127.0.0.1/prueba4db"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&protocol=onsoctcp"
    "&server=informix"
    "&service=9088"
    "&TCTX=1"
    "&DELIMIDENT=Y"
)
```

`trusted_context=true` is accepted as a readable alias and is rendered as
`TCTX=1`. If a URL has no UID/PWD and no trusted context flag, the dialect does
not add authentication keywords; authentication is left to the DSN, driver, or
environment.

## ODBC Type Reporting

`NeedODBCTypesOnly=1` is added by default for generated connection strings
unless the URL already contains that keyword in any casing. IBM documents this
setting for the Informix ODBC driver to report standard ODBC types where
possible, which avoids pyodbc seeing Informix-specific type codes such as
`SQL_INFX_BIGINT (-114)` for common SQLAlchemy result handling.

When using `odbc_connect`, the connection string is passed through unchanged.
If your environment requires `NeedODBCTypesOnly=1`, include it explicitly in
the `odbc_connect` string.

```python
from urllib.parse import quote_plus

from sqlalchemy import create_engine

odbc_str = quote_plus(
    "DRIVER={IBM INFORMIX ODBC DRIVER};"
    "SERVER=informix_server;"
    "DATABASE=mydb;"
    "HOST=localhost;"
    "SERVICE=9088;"
    "PROTOCOL=onsoctcp;"
    "UID=user;"
    "PWD=password;"
    "DELIMIDENT=Y;"
    "NeedODBCTypesOnly=1;"
)

engine = create_engine(f"informix+pyodbc:///?odbc_connect={odbc_str}")
```

## Runtime Contract

- `LIMIT` without `OFFSET` compiles to Informix `FIRST n`.
- `OFFSET` is compiled with a `ROW_NUMBER()` wrapper; the compiler does not
  parse rendered SQL strings.
- For deterministic pagination, pass an explicit `ORDER BY` with `OFFSET`.
- `supports_schemas = False`; owner-qualified cross-schema DDL and reflection
  are outside the supported SQLAlchemy surface.
- `Inspector.has_table()` supports known connection-local temporary table names
  on the same connection.
- `Inspector.get_temp_table_names()` and `Inspector.get_temp_view_names()`
  intentionally return `[]` because the supported Informix ODBC path does not
  expose a reliable enumeration API for connection-local temporary objects.
- The SQLAlchemy suite requirements mark full temp-table enumeration/reflection
  as unsupported for this backend.

## Running Tests

Unit and compilation tests do not require Informix. Tests using the `engine`,
`conn`, `db_builder`, or `pinned_connection_session` fixtures are marked
`requires_informix` and need a live database.

The package tests use `INFORMIX_SQLALCHEMY_URL`. The official SQLAlchemy suite
runner uses `INFORMIX_SQLALCHEMY_SUITE_URL` first, then falls back to
`INFORMIX_SQLALCHEMY_URL`.

Use an isolated database for the official suite. SQLAlchemy's multi-reflection
tests expect the database under test to contain only suite fixtures.

### Test with SQLAlchemy 2.0.x

```bat
env\scripts\activate
python -m pytest -m "not requires_informix and not legacy_ifxpy" -W error
python run_tests.py
deactivate
```

### Test with SQLAlchemy 2.1.x

```bat
env2\scripts\activate
python -m pytest -m "not requires_informix and not legacy_ifxpy" -W error
python run_tests.py
deactivate
```
