from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, column, select, values


pytestmark = pytest.mark.requires_informix


@pytest.mark.parametrize("literal_binds", [False, True])
@pytest.mark.parametrize("cte_named", [False, True])
@pytest.mark.parametrize("values_named", [False, True])
def test_values_named_via_cte_round_trip(
    conn,
    literal_binds,
    cte_named,
    values_named,
):
    cte = (
        values(
            column("col1", String),
            column("col2", Integer),
            literal_binds=literal_binds,
            name="some name" if values_named else None,
        )
        .data([("a", 2), ("b", 3)])
        .cte("cte1" if cte_named else None)
    )
    statement = select(cte)

    first_rows = conn.execute(statement).all()
    second_rows = conn.execute(statement).all()

    assert first_rows == [("a", 2), ("b", 3)]
    assert second_rows == first_rows
