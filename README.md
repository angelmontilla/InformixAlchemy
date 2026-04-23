# IfxAlchemy

**IfxAlchemy** provides a SQLAlchemy 2.x dialect for IBM Informix.

This fork is focused on **SQLAlchemy 2.x + pyodbc + Informix** and is being hardened through integration tests against a real Informix backend.

Please report bugs and edge cases through the project issue tracker.

## Current state

- Active development
- **Not yet production-certified**
- Main validated path: **`informix+pyodbc`**
- Statement caching remains disabled until the official SQLAlchemy third-party suite reaches an acceptable pass level for the selected backend

---

## Runtime contract

### Supported default dialect

- Default and recommended dialect: `informix+pyodbc`

### Current support baseline

The current validated baseline is centered on:

- SQLAlchemy 2.x
- `pyodbc`
- IBM Informix ODBC Driver
- Informix over `onsoctcp`

### Statement cache

Statement caching is currently disabled for:

- `IfxDialect`
- `IfxDialect_IfxPy`
- `IfxDialect_pyodbc`

This will remain so until the official SQLAlchemy suite is green at a reasonable level for the corresponding backend.

### LIMIT/OFFSET compilation

- `LIMIT` without `OFFSET` compiles to Informix `FIRST n`
- `OFFSET` is compiled by wrapping the original `SELECT` with `ROW_NUMBER()`, not by parsing rendered SQL strings
- Complex projections such as scalar subqueries, function calls with commas, and CTE-based selects are preserved by that translation
- For deterministic pagination, pass an explicit `ORDER BY` when using `OFFSET`

### External schemas

The selected Informix backend is currently gated as:

- `supports_schemas = False`

Owner-qualified cross-schema DDL and reflection are **not** part of the supported SQLAlchemy 2.x surface for this package.

### Temporary object introspection

- `Inspector.has_table()` supports temp tables **on the same connection**
- `Inspector.get_temp_table_names()` intentionally returns `[]`
- `Inspector.get_temp_view_names()` intentionally returns `[]`

This is by design: the Informix ODBC metadata available to this dialect does not expose a reliable connection-local listing for temp tables, and Informix temp views are not supported by this package.

### Official SQLAlchemy suite runner

`run_tests.py` prefers:

- `INFORMIX_SQLALCHEMY_SUITE_URL`

over the generic package test URL.

Use that variable to point the official SQLAlchemy suite to an **isolated Informix database**. Running multi-reflection tests against a shared application database will produce false failures, because SQLAlchemy expects the database under test to contain only the suite fixtures.

---

## Canonical supported URL

The **official and supported** connection format for this fork is:

```python
informix+pyodbc://<user>:<password>@<host>/<database>?driver=<odbc_driver>&protocol=<protocol>&server=<server>&service=<service>
