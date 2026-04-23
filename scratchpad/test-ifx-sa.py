from sqlalchemy import create_engine, text

DATABASE_URL = (
    "informix+pyodbc://informix:@127.0.0.1/prueba4db"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&server=informix"
    "&protocol=onsoctcp"
    "&service=9088"
)

engine = create_engine(DATABASE_URL, future=True)

with engine.connect() as conn:
    result = conn.execute(text("SELECT FIRST 1 tabname FROM systables ORDER BY tabname"))
    print(result.fetchone())
