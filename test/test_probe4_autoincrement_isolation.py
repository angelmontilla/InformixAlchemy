from __future__ import annotations

"""
Aísla el caso exacto del probe opcional de prueba4.py para responder con claridad:

¿Falla realmente SERIAL en el dialecto, o falló solo ese probe concreto por cómo
estaba montado / por el estado previo de la conexión?

Este módulo no prueba "autoincrement" de forma genérica (eso ya lo cubren
`test_autoincrement_serial.py` y compañía), sino el flujo exacto de `prueba4.py`:

1. crear una tabla ORM con `Integer(primary_key=True)` -> compilada como SERIAL
2. hacer `session.flush()` sin asignar PK
3. comprobar que la PK se rellena
4. comprobar que la fila existe y que la reflexión marca autoincrement=True
5. repetir el mismo flujo después de provocar el mismo SAVEPOINT inválido que
   falló en `prueba4.py`

Si ambos tests pasan, la conclusión es fuerte:
- SERIAL NO falla de forma general en el dialecto.
- El fallo opcional de `prueba4.py` no era "SERIAL roto", sino el probe concreto
  o el contexto en el que se ejecutaba.
"""

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, select, text
from sqlalchemy.orm import Session, registry
from sqlalchemy.schema import CreateTable

from IfxAlchemy.base import _SelectLastRowIDMixin


pytestmark = [pytest.mark.serial_identity]


class _LastrowidContext(_SelectLastRowIDMixin):
    pass


@pytest.fixture
def unique_name(name_factory):
    def _make(prefix: str = "sa_probe4_auto_") -> str:
        return name_factory(prefix)

    return _make


@pytest.fixture
def force_unsupported_savepoint_sql(engine):
    """
    Ejecuta exactamente el SQL de SAVEPOINT que falló en `prueba4.py` y deja la
    conexión realmente limpia (rollback + invalidation del handle físico), para
    verificar si ese fallo contamina o no un probe posterior de
    SERIAL/autoincrement.
    """

    def _run() -> str:
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                conn.exec_driver_sql("SAVEPOINT sa_savepoint_1 ON ROLLBACK RETAIN CURSORS")
            except Exception:
                try:
                    tx.rollback()
                finally:
                    # El SQL legado de savepoint deja esta sesión ODBC en un
                    # estado poco fiable para el siguiente DDL; invalidamos el
                    # handle para forzar una conexión fresca desde el pool.
                    conn.invalidate()
                return "failed_as_expected"
            else:
                tx.rollback()
                return "unexpectedly_supported"

    return _run


def _drop_table_if_exists(engine, table):
    with engine.begin() as conn:
        try:
            if inspect(conn).has_table(table.name):
                table.drop(conn)
        except Exception:
            # último intento por SQL directo; si tampoco sale, dejamos que la
            # siguiente operación revele el problema real.
            try:
                conn.exec_driver_sql(f'DROP TABLE "{table.name}"')
            except Exception:
                pass


def _run_probe4_style_autoincrement_roundtrip(engine, table_name: str) -> dict:
    """
    Reproduce el mismo patrón que el probe opcional de `prueba4.py`, pero con
    asserts más finos y limpieza robusta.

    Tabla deliberadamente similar a ProbeAuto:
      id Integer, primary_key=True   -> el dialecto la compila como SERIAL
      payload String(50)
    """
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(50), nullable=False),
    )

    mapper_registry = registry()

    class ProbeAutoRow:
        pass

    mapper_registry.map_imperatively(ProbeAutoRow, table)

    compiled = str(CreateTable(table).compile(dialect=engine.dialect)).upper()

    _drop_table_if_exists(engine, table)

    try:
        with engine.begin() as conn:
            metadata.create_all(conn)

        generated_pk = None

        with Session(engine, expire_on_commit=False) as session:
            row = ProbeAutoRow()
            row.payload = "auto"
            session.add(row)
            session.flush()
            generated_pk = row.id
            session.commit()

        with engine.connect() as conn:
            cols = {col["name"]: col for col in inspect(conn).get_columns(table_name)}
            fetched = conn.execute(
                select(table.c.id, table.c.payload).where(table.c.id == generated_pk)
            ).one()

        return {
            "compiled": compiled,
            "generated_pk": generated_pk,
            "autoincrement": cols["id"].get("autoincrement"),
            "nullable": cols["id"].get("nullable"),
            "payload": fetched.payload,
        }
    finally:
        try:
            _drop_table_if_exists(engine, table)
        finally:
            mapper_registry.dispose()


def test_probe4_style_autoincrement_works_on_pristine_engine(engine, unique_name):
    """
    Responde a la pregunta principal sin ruido externo:

    Si este test pasa, el patrón exacto de `prueba4.py` NO tiene un fallo general
    de SERIAL/autoincrement.
    """
    result = _run_probe4_style_autoincrement_roundtrip(engine, unique_name())

    assert "ID SERIAL NOT NULL" in result["compiled"], result["compiled"]
    assert result["generated_pk"] is not None
    assert int(result["generated_pk"]) > 0
    assert result["payload"] == "auto"
    assert result["autoincrement"] is True, result
    assert result["nullable"] is False, result


@pytest.mark.optional_probe_isolation
def test_probe4_style_autoincrement_still_works_after_failed_savepoint(
    engine,
    unique_name,
    force_unsupported_savepoint_sql,
):
    """
    Aísla la hipótesis de contaminación por el SAVEPOINT fallido anterior.

    Si este test pasa también, la respuesta es muy fuerte:
    - SERIAL no falla.
    - ni siquiera un SAVEPOINT inválido previo deja el engine en un estado que
      rompa este roundtrip de autoincrement, siempre que la conexión se limpie.
    """
    savepoint_outcome = force_unsupported_savepoint_sql()
    if savepoint_outcome == "unexpectedly_supported":
        pytest.skip("El servidor soporta ese SAVEPOINT; este aislamiento deja de ser relevante.")

    result = _run_probe4_style_autoincrement_roundtrip(engine, unique_name())

    assert savepoint_outcome == "failed_as_expected"
    assert "ID SERIAL NOT NULL" in result["compiled"], result["compiled"]
    assert result["generated_pk"] is not None
    assert int(result["generated_pk"]) > 0
    assert result["payload"] == "auto"
    assert result["autoincrement"] is True, result


@pytest.mark.optional_probe_isolation
def test_probe4_style_autoincrement_matches_existing_working_contract(engine, unique_name):
    """
    Cruza el caso de `prueba4.py` con el contrato que ya sabíamos que funciona:
    Integer(primary_key=True) debe compilar como SERIAL para Informix.

    Esto evita confundir un fallo de SQL/DDL con un fallo del ORM flush.
    """
    table_name = unique_name()
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(50), nullable=False),
    )

    compiled = str(CreateTable(table).compile(dialect=engine.dialect)).upper()

    assert "CREATE TABLE" in compiled
    assert f"CREATE TABLE {table_name.upper()}" in compiled or f'CREATE TABLE "{table_name.upper()}"' in compiled
    assert "ID SERIAL NOT NULL" in compiled, compiled
    assert "PRIMARY KEY (ID)" in compiled, compiled


def test_probe4_explicit_pk_does_not_schedule_dbinfo_lastrowid_query(unique_name):
    table_name = unique_name()
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(50), nullable=False),
    )

    context = _LastrowidContext()
    context.isinsert = True
    context.compiled = SimpleNamespace(
        dml_compile_state=SimpleNamespace(dml_table=table),
        effective_returning=None,
        inline=False,
    )
    context.compiled_parameters = [{"id": 1001, "payload": "manual"}]
    context.executemany = False

    context.pre_exec()

    assert context._select_lastrowid is False
    assert context._lastrowid_query is None
