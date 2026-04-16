from sqlalchemy import create_engine, text

DATABASE_URL = (
    "informix+pyodbc://ctl:magogo@192.168.11.64/faempre999"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&server=pru_famadesa_s9"
    "&protocol=onsoctcp"
    "&service=9088"
)

engine = create_engine(DATABASE_URL, future=True)

with engine.connect() as conn:
    result = conn.execute(text("SELECT FIRST 1 tabname FROM systables ORDER BY tabname"))
    print(result.fetchone())