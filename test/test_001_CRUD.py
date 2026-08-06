from __future__ import annotations

import datetime as dt
from decimal import Decimal

from sqlalchemy import Numeric, bindparam, text


def test_basic_crud_round_trip(engine, name_factory):
    table_name = name_factory("employee_")

    create_sql = f"""
    CREATE TABLE {table_name} (
        id INTEGER NOT NULL,
        fname VARCHAR(20),
        lname VARCHAR(20),
        salary MONEY(16, 2),
        purchase DATE
    )
    """

    insert_sql = text(
        f"""
        INSERT INTO {table_name} (id, fname, lname, salary, purchase)
        VALUES (:id, :fname, :lname, :salary, :purchase)
        """
    ).bindparams(
        bindparam(
            "salary",
            type_=Numeric(16, 2, asdecimal=True),
        )
    )

    update_sql = text(f"UPDATE {table_name} SET id = :new_id WHERE id = :old_id")
    select_sql = text(
        f"""
        SELECT id, fname, lname, salary, purchase
        FROM {table_name}
        ORDER BY id
        """
    )

    with engine.connect() as connection:
        try:
            connection.exec_driver_sql(create_sql)
            connection.commit()

            connection.execute(
                insert_sql,
                [
                    {
                        "id": 1,
                        "fname": "Sheetal",
                        "lname": "J",
                        "salary": Decimal("20100.19"),
                        "purchase": dt.date(2019, 2, 2),
                    },
                    {
                        "id": 2,
                        "fname": "Joe",
                        "lname": "T",
                        "salary": Decimal("20111.19"),
                        "purchase": dt.date(2019, 11, 23),
                    },
                ],
            )
            connection.commit()

            connection.execute(update_sql, {"new_id": 200, "old_id": 2})
            connection.commit()

            rows = connection.execute(select_sql).all()
        finally:
            try:
                connection.exec_driver_sql(f"DROP TABLE {table_name}")
                connection.commit()
            except Exception:
                connection.rollback()

    assert len(rows) == 2
    assert rows[0].id == 1
    assert rows[0].fname.strip() == "Sheetal"
    assert rows[0].lname.strip() == "J"
    assert rows[0].salary == Decimal("20100.19")
    assert rows[0].purchase == dt.date(2019, 2, 2)

    assert rows[1].id == 200
    assert rows[1].fname.strip() == "Joe"
    assert rows[1].lname.strip() == "T"
    assert rows[1].salary == Decimal("20111.19")
    assert rows[1].purchase == dt.date(2019, 11, 23)
