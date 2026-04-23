from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import declarative_base, Session

DATABASE_URL = (
    "informix+pyodbc://informix:@127.0.0.1/prueba4db"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&server=informix"
    "&protocol=onsoctcp"
    "&service=9088"
)

Base = declarative_base()

class SysTable(Base):
    __tablename__ = "systables"

    tabname = Column(String, primary_key=True)

engine = create_engine(DATABASE_URL, future=True)

with Session(engine) as session:
    row = session.query(SysTable).first()
    print(row.tabname if row else None)
