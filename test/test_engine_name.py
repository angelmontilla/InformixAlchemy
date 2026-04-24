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
