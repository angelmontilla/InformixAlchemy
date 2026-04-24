from __future__ import annotations

import pytest
from sqlalchemy import Boolean, LargeBinary, Text, inspect


def test_decode_opaque_blob_clob_boolean_without_database():
    from IfxAlchemy.pyodbc import IfxDialect_pyodbc

    reflector = IfxDialect_pyodbc()._reflector

    blob, autoinc, nullable = reflector._decode_ifx_type(
        41, 0, extended_id=1, extended_type_name="blob"
    )
    assert isinstance(blob, LargeBinary)

    clob, autoinc, nullable = reflector._decode_ifx_type(
        41, 0, extended_id=2, extended_type_name="clob"
    )
    assert isinstance(clob, Text)

    boolean, autoinc, nullable = reflector._decode_ifx_type(
        41, 0, extended_id=3, extended_type_name="boolean"
    )
    assert isinstance(boolean, Boolean)


def test_decode_lvarchar_opaque_uses_extended_maxlen_without_database():
    from IfxAlchemy.pyodbc import IfxDialect_pyodbc

    reflector = IfxDialect_pyodbc()._reflector

    lvarchar, autoinc, nullable = reflector._decode_ifx_type(
        41,
        0,
        extended_id=4,
        extended_type_name="lvarchar",
        extended_maxlen=128,
    )

    assert getattr(lvarchar, "length", None) == 128


@pytest.mark.requires_informix
def test_reflect_opaque_types_round_trip(engine, name_factory):
    table_name = name_factory("sa_opaque_")

    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER NOT NULL,
                payload_blob BLOB,
                payload_clob CLOB,
                flag BOOLEAN
            )
            """
        )

    try:
        cols = {
            col["name"]: col
            for col in inspect(engine).get_columns(table_name)
        }

        assert isinstance(cols["payload_blob"]["type"], LargeBinary)
        assert isinstance(cols["payload_clob"]["type"], Text)
        assert isinstance(cols["flag"]["type"], Boolean)
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE {table_name}")
