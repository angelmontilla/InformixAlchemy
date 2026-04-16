from __future__ import annotations

import uuid
from dataclasses import dataclass

from IfxAlchemy import BIGSERIAL, SERIAL8
from sqlalchemy import Column, Integer, String, create_engine, select, text
from sqlalchemy.orm import Session, declarative_base

DATABASE_URL = (
    "informix+pyodbc://ctl:magogo@192.168.11.64/faempre999"
    "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
    "&server=pru_famadesa_s9"
    "&protocol=onsoctcp"
    "&service=9088"
)

Base = declarative_base()


def make_table_name(prefix: str = "z_sa20") -> str:
    # Informix suele ir bien con nombres simples y cortos.
    suffix = uuid.uuid4().hex[:8].lower()
    return f"{prefix}_{suffix}"


@dataclass
class TestContext:
    table_name: str


def create_test_table(engine, ctx: TestContext) -> None:
    ddl = f"""
    CREATE TABLE {ctx.table_name} (
        id INTEGER NOT NULL,
        nombre VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)
    )
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
    print(f"[OK] Tabla creada: {ctx.table_name}")


def drop_test_table(engine, ctx: TestContext) -> None:
    # DROP explícito en finally: no dependemos de rollback de DDL
    # para devolver el esquema a su estado original.
    exists_sql = text(
        "SELECT COUNT(*) "
        "FROM systables "
        "WHERE tabtype = 'T' AND tabname = :tabname"
    )
    with engine.begin() as conn:
        exists = conn.execute(exists_sql, {"tabname": ctx.table_name}).scalar_one()
        if exists:
            conn.exec_driver_sql(f"DROP TABLE {ctx.table_name}")
            print(f"[OK] Tabla eliminada: {ctx.table_name}")
        else:
            print(f"[INFO] La tabla ya no existe: {ctx.table_name}")


def build_model(table_name: str):
    return type(
        f"ExplicitPkRow_{table_name}",
        (Base,),
        {
            "__tablename__": table_name,
            "id": Column(Integer, primary_key=True, autoincrement=False),
            "nombre": Column(String(50), nullable=False),
            "__repr__": lambda self: (
                f"ExplicitPkRow(id={self.id!r}, nombre={self.nombre!r})"
            ),
        },
    )


def build_generated_model(table_name: str, id_type, label: str):
    return type(
        f"{label}_{table_name}",
        (Base,),
        {
            "__tablename__": table_name,
            "id": Column(id_type, primary_key=True),
            "nombre": Column(String(50), nullable=False),
            "__repr__": lambda self: (
                f"{label}(id={self.id!r}, nombre={self.nombre!r})"
            ),
        },
    )


def create_model_table(engine, Model) -> None:
    Model.__table__.create(bind=engine)
    print(f"[OK] Tabla creada: {Model.__table__.name}")


def smoke_test(engine) -> None:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT FIRST 1 tabname FROM systables ORDER BY tabname")
        ).fetchone()
        print("[OK] Smoke test:", row)


def test_rollback(engine, Model) -> None:
    with Session(engine) as session:
        row = Model(id=1001, nombre="rollback")
        session.add(row)
        session.flush()
        print("[OK] Insert temporal hecho")
        session.rollback()
        print("[OK] Rollback ejecutado")

    with Session(engine) as session:
        row = session.get(Model, 1001)
        assert row is None, "Rollback falló: la fila sigue existiendo"
        print("[OK] Validado rollback: fila inexistente")


def test_commit(engine, Model) -> None:
    with Session(engine) as session:
        row = Model(id=1002, nombre="commit")
        session.add(row)
        session.commit()
        print("[OK] Commit ejecutado")

    with Session(engine) as session:
        row = session.get(Model, 1002)
        assert row is not None, "Commit falló: la fila no existe"
        assert row.nombre == "commit", f"Nombre inesperado: {row.nombre!r}"
        print("[OK] Validado commit:", row)


def test_update(engine, Model) -> None:
    with Session(engine) as session:
        row = session.get(Model, 1002)
        assert row is not None, "No existe fila para update"
        row.nombre = "update"
        session.commit()
        print("[OK] Update ejecutado")

    with Session(engine) as session:
        row = session.execute(
            select(Model).where(Model.id == 1002)
        ).scalars().first()
        assert row is not None, "Fila desaparecida tras update"
        assert row.nombre == "update", f"Update falló: {row.nombre!r}"
        print("[OK] Validado update:", row)


def test_delete(engine, Model) -> None:
    with Session(engine) as session:
        row = session.get(Model, 1002)
        assert row is not None, "No existe fila para delete"
        session.delete(row)
        session.commit()
        print("[OK] Delete ejecutado")

    with Session(engine) as session:
        row = session.get(Model, 1002)
        assert row is None, "Delete falló: la fila sigue existiendo"
        print("[OK] Validado delete: fila inexistente")


def test_select_style_20(engine, Model) -> None:
    with Session(engine) as session:
        session.add(Model(id=1003, nombre="select20"))
        session.commit()

    with Session(engine) as session:
        row = session.execute(select(Model).where(Model.id == 1003)).scalars().one()
        assert row.nombre == "select20"
        print("[OK] Validado select estilo 2.0:", row)

    with Session(engine) as session:
        row = session.get(Model, 1003)
        if row is not None:
            session.delete(row)
            session.commit()


def test_generated_pk(engine, Model, label: str) -> None:
    with Session(engine) as session:
        row = Model(nombre=label)
        session.add(row)
        session.flush()
        assert row.id is not None, f"{label}: PK no generada"
        generated_id = int(row.id)
        assert generated_id > 0, f"{label}: PK inesperada {row.id!r}"
        print(f"[OK] PK autogenerada {label}: {generated_id}")
        session.commit()

    with Session(engine) as session:
        row = session.get(Model, generated_id)
        assert row is not None, f"{label}: fila no recuperable por PK"
        assert row.nombre == label, f"{label}: nombre inesperado {row.nombre!r}"
        print(f"[OK] Validado {label}:", row)
        session.delete(row)
        session.commit()


def main() -> None:
    engine = create_engine(
        DATABASE_URL,
        future=True,
        pool_pre_ping=True,
    )

    ctx = TestContext(table_name=make_table_name())
    Model = build_model(ctx.table_name)
    serial8_ctx = TestContext(table_name=make_table_name("z_sa20_s8"))
    serial8_model = build_generated_model(serial8_ctx.table_name, SERIAL8(), "Serial8Row")
    bigserial_ctx = TestContext(table_name=make_table_name("z_sa20_bs"))
    bigserial_model = build_generated_model(
        bigserial_ctx.table_name,
        BIGSERIAL(),
        "BigSerialRow",
    )

    try:
        smoke_test(engine)
        create_test_table(engine, ctx)

        test_rollback(engine, Model)
        test_commit(engine, Model)
        test_update(engine, Model)
        test_delete(engine, Model)
        test_select_style_20(engine, Model)
        create_model_table(engine, serial8_model)
        test_generated_pk(engine, serial8_model, "serial8")
        create_model_table(engine, bigserial_model)
        test_generated_pk(engine, bigserial_model, "bigserial")

        print(f"[OK] Suite ORM completada sobre tabla temporal: {ctx.table_name}")

    finally:
        drop_errors = []
        for cleanup_ctx in (bigserial_ctx, serial8_ctx, ctx):
            try:
                drop_test_table(engine, cleanup_ctx)
            except Exception as drop_exc:
                print(
                    f"[WARN] No se pudo eliminar la tabla "
                    f"{cleanup_ctx.table_name}: {drop_exc}"
                )
                drop_errors.append(drop_exc)
        if drop_errors:
            raise drop_errors[0]


if __name__ == "__main__":
    main()
