# IfxAlchemy

**IfxAlchemy** provides a SQLAlchemy 2.x dialect for IBM Informix.

This fork's supported backend is **`informix+pyodbc`** with the IBM Informix
ODBC driver. The old IfxPy module is kept only as legacy source compatibility
and is not part of the public entry-point contract.

## Support Matrix

| Component | Supported baseline |
| --- | --- |
| Python | 3.10, 3.11, 3.12 |
| SQLAlchemy | 2.0.x and 2.1.x target line, dependency range `>=2.0,<2.3` |
| DBAPI | `pyodbc>=5.0` |
| Driver | IBM Informix ODBC Driver |
| Protocol | Informix over `onsoctcp` |

Statement caching remains disabled until the SQLAlchemy third-party suite is
green enough for this backend.

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

The dialect adds `NeedODBCTypesOnly=1` by default unless the URL already
contains that keyword in any casing. IBM documents this setting for the
Informix ODBC driver to report standard ODBC types where possible, which avoids
pyodbc seeing Informix-specific type codes such as `SQL_INFX_BIGINT (-114)` for
common SQLAlchemy result handling.

## Runtime Contract

- `LIMIT` without `OFFSET` compiles to Informix `FIRST n`.
- `OFFSET` is compiled with a `ROW_NUMBER()` wrapper; the compiler does not
  parse rendered SQL strings.
- For deterministic pagination, pass an explicit `ORDER BY` with `OFFSET`.
- `supports_schemas = False`; owner-qualified cross-schema DDL and reflection
  are outside the supported SQLAlchemy surface.
- `Inspector.has_table()` supports connection-local temporary tables when
  checked on the same connection.
- `Inspector.get_temp_table_names()` and `Inspector.get_temp_view_names()`
  intentionally return `[]`.

## Running Tests

Unit and compilation tests do not require Informix. Tests using the `engine`,
`conn`, `db_builder`, or `pinned_connection_session` fixtures are marked
`requires_informix` and need a live database.

The package tests use `INFORMIX_SQLALCHEMY_URL`. The official SQLAlchemy suite
runner uses `INFORMIX_SQLALCHEMY_SUITE_URL` first, then falls back to
`INFORMIX_SQLALCHEMY_URL`.

Use an isolated database for the official suite. SQLAlchemy's multi-reflection
tests expect the database under test to contain only suite fixtures.
