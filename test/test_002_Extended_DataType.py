from __future__ import annotations

import pytest
from sqlalchemy import text


def test_extended_collection_and_row_types_round_trip(engine, name_factory):
    suffix = name_factory("xdt_")[-8:]
    row_type_name = f"udt_t4_{suffix}"
    table_name = f"t1911_{suffix}"

    create_row_type_sql = f"CREATE ROW TYPE {row_type_name} (a INT)"
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        c1 INT,
        c2 CHAR(20),
        c3 FLOAT,
        c4 VARCHAR(10),
        s1 SET(INT NOT NULL),
        m1 MULTISET(INT NOT NULL),
        l1 LIST(BIGINT NOT NULL),
        u {row_type_name}
    )
    """
    insert_sql = (
        f"INSERT INTO {table_name} VALUES("
        f"1, 'Sheetal', 12.01, 'Hello', "
        f"set{{11, 10}}, multiset{{22, 33}}, list{{10000, 20000}}, "
        f"(ROW(201)::{row_type_name})"
        f")"
    )

    with engine.connect() as connection:
        try:
            connection.exec_driver_sql(create_row_type_sql)
            connection.exec_driver_sql(create_table_sql)
            connection.exec_driver_sql(insert_sql)
            connection.commit()

            row = connection.execute(text(f"SELECT * FROM {table_name}")).one()
        finally:
            try:
                connection.exec_driver_sql(f"DROP TABLE {table_name}")
                connection.exec_driver_sql(f"DROP ROW TYPE {row_type_name} RESTRICT")
                connection.commit()
            except Exception:
                connection.rollback()

    assert row.c1 == 1
    assert row.c2.strip() == "Sheetal"
    assert row.c3 == pytest.approx(12.01)
    assert row.c4 == "Hello"
    assert row.s1.startswith("SET{")
    assert "11" in row.s1 and "10" in row.s1
    assert row.m1.startswith("MULTISET{")
    assert "22" in row.m1 and "33" in row.m1
    assert row.l1.startswith("LIST{")
    assert "10000" in row.l1 and "20000" in row.l1
    assert row.u.startswith("ROW(")
    assert "201" in row.u
