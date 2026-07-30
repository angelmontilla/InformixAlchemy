"""Official SQLAlchemy dialect compliance suite."""

from sqlalchemy import Column, Integer, MetaData, Sequence, Table, inspect
from sqlalchemy.schema import CreateSequence, DropSequence
from sqlalchemy.testing import config, fixtures
from sqlalchemy.testing.suite import *  # noqa: F401,F403  # pyright: ignore[reportWildcardImportFromLibrary]


class InformixPhysicalTableOptionsTest(fixtures.TestBase):
    """Certify native physical table options in the official suite lane."""

    table_name = "ifx_suite_physical_options"

    @classmethod
    def setup_class(cls):
        cls.metadata = MetaData()
        cls.table = Table(
            cls.table_name,
            cls.metadata,
            Column(
                "id",
                Integer,
                primary_key=True,
                autoincrement=False,
            ),
            informix_lock_level="ROW",
            informix_first_extent=96,
            informix_next_extent=96,
        )

        with config.db.begin() as connection:
            cls.table.drop(connection, checkfirst=True)
            cls.table.create(connection)

    def test_physical_options_create_reflect_round_trip(self):
        with config.db.connect() as connection:
            reflected = inspect(connection).get_table_options(
                self.table_name
            )

        assert reflected["informix_lock_level"] == "ROW"
        assert reflected["informix_first_extent"] == 96
        assert reflected["informix_next_extent"] == 96
        assert reflected["informix_page_size"] > 0
        assert isinstance(reflected["informix_page_size"], int)

    @classmethod
    def teardown_class(cls):
        with config.db.begin() as connection:
            cls.table.drop(connection, checkfirst=True)


class InformixSequenceIfExistsTest(fixtures.TestBase):
    """Certify native idempotent sequence DDL in the official suite lane."""

    sequence_name = "ifx_suite_sequence_if_exists"

    def test_create_and_drop_sequence_if_exists_are_idempotent(self):
        sequence = Sequence(
            self.sequence_name,
            start=17,
            increment=3,
            minvalue=1,
            maxvalue=100000,
            cache=20,
            cycle=True,
        )

        try:
            with config.db.begin() as connection:
                connection.execute(
                    DropSequence(sequence, if_exists=True)
                )
                connection.execute(
                    CreateSequence(sequence, if_not_exists=True)
                )
                connection.execute(
                    CreateSequence(sequence, if_not_exists=True)
                )

            with config.db.connect() as connection:
                assert inspect(connection).has_sequence(
                    self.sequence_name
                )

            with config.db.begin() as connection:
                connection.execute(
                    DropSequence(sequence, if_exists=True)
                )
                connection.execute(
                    DropSequence(sequence, if_exists=True)
                )

            with config.db.connect() as connection:
                assert not inspect(connection).has_sequence(
                    self.sequence_name
                )
        finally:
            with config.db.begin() as connection:
                connection.execute(
                    DropSequence(sequence, if_exists=True)
                )
