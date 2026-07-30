from __future__ import annotations

import pytest
from sqlalchemy import Sequence, inspect
from sqlalchemy.schema import CreateSequence, DropSequence


pytestmark = [
    pytest.mark.requires_informix,
    pytest.mark.ddl_execute,
]


def test_sequence_if_exists_ddl_is_idempotent_on_informix(
    engine,
    name_factory,
):
    """Exercise both native clauses against Informix 14.10 or later.

    The second CREATE and second DROP are the essential regression checks:
    they must be successful no-ops performed atomically by Informix itself.
    """
    sequence_name = name_factory("sa_seq_exists_")
    sequence = Sequence(
        sequence_name,
        start=17,
        increment=3,
        minvalue=1,
        maxvalue=100000,
        cache=20,
        cycle=True,
    )

    try:
        with engine.begin() as connection:
            connection.execute(
                DropSequence(sequence, if_exists=True)
            )
            connection.execute(
                CreateSequence(sequence, if_not_exists=True)
            )
            connection.execute(
                CreateSequence(sequence, if_not_exists=True)
            )

        with engine.connect() as connection:
            assert inspect(connection).has_sequence(sequence_name)

        with engine.begin() as connection:
            connection.execute(
                DropSequence(sequence, if_exists=True)
            )
            connection.execute(
                DropSequence(sequence, if_exists=True)
            )

        with engine.connect() as connection:
            assert not inspect(connection).has_sequence(sequence_name)
    finally:
        # Keep the database clean even if an intermediate assertion fails.
        with engine.begin() as connection:
            connection.execute(
                DropSequence(sequence, if_exists=True)
            )
