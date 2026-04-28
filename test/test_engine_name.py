from sqlalchemy import create_engine

import IfxAlchemy
from IfxAlchemy import base, pyodbc


def test_engine_name_reports_informix():
    engine = create_engine(
        "informix+pyodbc://user:pass@host/database"
        "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
        "&protocol=onsoctcp"
        "&server=demo"
        "&service=9088"
    )

    try:
        assert engine.name == "informix"
    finally:
        engine.dispose()


def test_package_default_dialect_export_has_no_base_side_effect():
    assert IfxAlchemy.dialect is pyodbc.IfxDialect_pyodbc
    assert base.dialect is base.IfxDialect


def test_real_pyodbc_engine_executes_catalog_smoke_query(engine):
    with engine.connect() as conn:
        value = conn.exec_driver_sql(
            "SELECT FIRST 1 tabid FROM systables"
        ).scalar()

    assert value is not None
