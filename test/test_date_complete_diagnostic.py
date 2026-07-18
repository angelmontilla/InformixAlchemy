import os
from datetime import date

import pytest
from sqlalchemy import Column, Date, Integer, MetaData, Table, event, insert, text

from IfxAlchemy.base import IfxDialect, _IFXDate
from IfxAlchemy.pyodbc import IfxDialect_pyodbc


TEST_DATE = date(2026, 7, 16)


def test_date_bind_processor_preserves_date_object():
    dialect = IfxDialect()
    processor = _IFXDate().bind_processor(dialect)

    processed = processor(TEST_DATE)

    assert processed == TEST_DATE
    assert isinstance(processed, date)
    assert not isinstance(processed, str)


def test_effective_date_type_preserves_date_object():
    dialect = IfxDialect_pyodbc()
    metadata = MetaData()

    table = Table(
        "test_date_type",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("fecha", Date, nullable=False),
    )

    effective_type = table.c.fecha.type.dialect_impl(dialect)
    processor = effective_type.bind_processor(dialect)
    processed = processor(TEST_DATE) if processor else TEST_DATE

    print(f"Declared type: {table.c.fecha.type!r}")
    print(f"Effective type: {effective_type!r}")
    print(f"Processor: {processor!r}")
    print(f"Processed value: {processed!r}")
    print(f"Processed type: {type(processed)!r}")

    assert processed == TEST_DATE
    assert isinstance(processed, date)
    assert not isinstance(processed, str)


def test_date_parameter_before_cursor_execute(engine):
    metadata = MetaData()

    table = Table(
        "ifx_test_date_parameter",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("fecha", Date, nullable=False),
    )

    metadata.drop_all(engine, checkfirst=True)
    metadata.create_all(engine)

    captured = {}

    def capture_parameters(
        connection,
        cursor,
        statement,
        parameters,
        context,
        executemany,
    ):
        if table.name not in statement or not statement.lstrip().upper().startswith(
            "INSERT"
        ):
            return

        captured["statement"] = statement
        captured["parameters"] = parameters

        current_parameters = parameters[0] if executemany else parameters

        if current_parameters:
            captured["value"] = current_parameters[0]

    event.listen(engine, "before_cursor_execute", capture_parameters)

    execution_error = None

    try:
        with engine.begin() as connection:
            connection.execute(insert(table).values(fecha=TEST_DATE))
    except Exception as exc:
        execution_error = exc
    finally:
        event.remove(engine, "before_cursor_execute", capture_parameters)
        metadata.drop_all(engine, checkfirst=True)

    value = captured.get("value")

    print(f"Statement: {captured.get('statement')}")
    print(f"Parameters: {captured.get('parameters')!r}")
    print(f"Captured value: {value!r}")
    print(f"Captured type: {type(value)!r}")
    print(f"Execution error: {execution_error!r}")

    assert "value" in captured
    assert value == TEST_DATE
    assert isinstance(value, date)
    assert not isinstance(value, str)

    if execution_error is not None:
        pytest.fail(
            "SQLAlchemy preserved datetime.date, but database execution failed: "
            f"{execution_error!r}"
        )


def test_date_roundtrip_with_sqlalchemy(engine):
    metadata = MetaData()

    table = Table(
        "ifx_test_date_roundtrip",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("fecha", Date, nullable=False),
    )

    metadata.drop_all(engine, checkfirst=True)
    metadata.create_all(engine)

    try:
        with engine.begin() as connection:
            connection.execute(insert(table).values(fecha=TEST_DATE))

        with engine.connect() as connection:
            received = connection.execute(
                table.select().with_only_columns(table.c.fecha)
            ).scalar_one()

        print(f"Sent value: {TEST_DATE!r}")
        print(f"Received value: {received!r}")
        print(f"Received type: {type(received)!r}")

        assert received == TEST_DATE
        assert isinstance(received, date)
        assert not isinstance(received, str)
    finally:
        metadata.drop_all(engine, checkfirst=True)


def test_date_roundtrip_with_direct_pyodbc(engine):
    table_name = "ifx_test_date_direct"

    with engine.begin() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE {table_name} (
                    id SERIAL NOT NULL,
                    fecha DATE NOT NULL,
                    PRIMARY KEY (id)
                )
                """
            )
        )

    raw_connection = engine.raw_connection()

    try:
        cursor = raw_connection.cursor()

        cursor.execute(
            f"INSERT INTO {table_name} (fecha) VALUES (?)",
            TEST_DATE,
        )
        raw_connection.commit()

        cursor.execute(f"SELECT fecha FROM {table_name}")
        received = cursor.fetchone()[0]

        print(f"Sent value: {TEST_DATE!r}")
        print(f"Sent type: {type(TEST_DATE)!r}")
        print(f"Received value: {received!r}")
        print(f"Received type: {type(received)!r}")

        assert received == TEST_DATE
        assert isinstance(received, date)
        assert not isinstance(received, str)
    finally:
        raw_connection.close()

        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE {table_name}"))


def test_print_date_environment():
    variables = {
        "DBDATE": os.getenv("DBDATE"),
        "GL_DATE": os.getenv("GL_DATE"),
        "CLIENT_LOCALE": os.getenv("CLIENT_LOCALE"),
        "DB_LOCALE": os.getenv("DB_LOCALE"),
    }

    for name, value in variables.items():
        print(f"{name}={value!r}")

    assert isinstance(variables, dict)
