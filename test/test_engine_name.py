from sqlalchemy import create_engine


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
