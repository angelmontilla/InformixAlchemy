from __future__ import annotations

from contextlib import suppress

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, func, inspect
from sqlalchemy import exc, literal, select, text
from sqlalchemy.schema import CreateIndex
from sqlalchemy.sql import quoted_name

from IfxAlchemy import LVARCHAR
from IfxAlchemy.base import IfxDialect
from IfxAlchemy.reflection import IfxReflector
from IfxAlchemy.requirements import Requirements


@pytest.fixture
def dialect():
    dialect = IfxDialect()
    dialect.default_schema_name = "informix"
    return dialect


def _table(metadata: MetaData | None = None) -> Table:
    metadata = metadata or MetaData()
    return Table(
        "functional_people",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(128)),
        Column("surname", String(128)),
    )


def test_functional_index_compiles_only_with_explicit_opt_in(dialect):
    table = _table()
    index = Index(
        "ix_normalized_name",
        func.normalized_name(table.c.name),
        informix_functional=True,
    )

    assert str(CreateIndex(index).compile(dialect=dialect)) == (
        "CREATE INDEX ix_normalized_name ON functional_people "
        "(normalized_name(name))"
    )


def test_functional_index_compiles_unique_descending_and_quoted(dialect):
    metadata = MetaData()
    table = Table(
        quoted_name("Functional People", True),
        metadata,
        Column(quoted_name("Display Name", True), String(128)),
    )
    index = Index(
        quoted_name("Ix Normalized Name", True),
        func.normalized_name(table.c["Display Name"]).desc(),
        unique=True,
        informix_functional=True,
    )

    assert str(CreateIndex(index).compile(dialect=dialect)) == (
        'CREATE UNIQUE INDEX "Ix Normalized Name" '
        'ON "Functional People" (normalized_name("Display Name") DESC)'
    )


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (
            lambda table: Index(
                "ix_missing_opt_in",
                func.normalized_name(table.c.name),
            ),
            "informix_functional=True",
        ),
        (
            lambda table: Index(
                "ix_not_a_function",
                table.c.name.desc(),
                informix_functional=True,
            ),
            "requires a SQLAlchemy function call",
        ),
        (
            lambda table: Index(
                "ix_literal_argument",
                func.normalized_name(literal("constant")),
                informix_functional=True,
            ),
            "must be direct table columns",
        ),
        (
            lambda table: Index(
                "ix_nested_expression",
                func.normalized_name(table.c.name + table.c.surname),
                informix_functional=True,
            ),
            "must be direct table columns",
        ),
        (
            lambda table: Index(
                "ix_multiple_keys",
                func.normalized_name(table.c.name),
                table.c.surname,
                informix_functional=True,
            ),
            "supports exactly one function key",
        ),
    ],
)
def test_functional_index_rejects_unsafe_shapes(dialect, factory, message):
    table = _table()
    index = factory(table)

    with pytest.raises(exc.CompileError, match=message):
        CreateIndex(index).compile(dialect=dialect)


def test_functional_index_rejects_columns_from_another_table(dialect):
    _ = dialect
    metadata = MetaData()
    table = _table(metadata)
    other = Table("other_people", metadata, Column("name", String(128)))

    # SQLAlchemy itself prevents an Index expression associated with one
    # table from being forcibly attached to another table. The dialect keeps
    # the same invariant as a defensive compiler check.
    with pytest.raises(exc.ArgumentError, match="cannot be associated"):
        Index(
            "ix_wrong_table",
            func.normalized_name(other.c.name),
            informix_functional=True,
            _table=table,
        )


def test_index_dialect_arguments_are_registered(dialect):
    table = _table()
    index = Index(
        "ix_options",
        func.normalized_name(table.c.name),
        informix_functional=True,
        informix_procedure="normalized_name",
        informix_access_method="btree",
        informix_opclass="btree_ops",
    )

    options = index.dialect_options["informix"]
    assert options["functional"] is True
    assert options["procedure"] == "normalized_name"
    assert options["access_method"] == "btree"
    assert options["opclass"] == "btree_ops"


def test_requirements_publish_reflection_without_generic_expression_creation():
    requirements = Requirements()

    assert requirements.indexes_with_expressions.enabled is False
    assert requirements.reflect_indexes_with_expressions.enabled is True


def test_parse_sysindices_indexkeys_for_columns_functions_and_descending(dialect):
    reflector = IfxReflector(dialect)

    assert reflector._parse_indexkeys(
        "1 [1], <574> (-3, 2) [7], -6 [1]"
    ) == [
        {
            "kind": "column",
            "colnos": [1],
            "descending": False,
            "opclassid": 1,
        },
        {
            "kind": "function",
            "procid": 574,
            "colnos": [3, 2],
            "descending": True,
            "opclassid": 7,
        },
        {
            "kind": "column",
            "colnos": [6],
            "descending": True,
            "opclassid": 1,
        },
    ]


def test_parse_sysindices_indexkeys_rejects_unknown_format(dialect):
    reflector = IfxReflector(dialect)

    with pytest.raises(ValueError, match="unrecognized index key"):
        reflector._parse_indexkeys("functional(name)")


def test_index_catalog_query_uses_sysindices_and_native_catalog_joins(dialect):
    reflector = IfxReflector(dialect)

    class Result:
        def fetchall(self):
            return []

    class Connection:
        statement = None
        params = None

        def exec_driver_sql(self, statement, params=()):
            self.statement = statement
            self.params = params
            return Result()

    connection = Connection()
    assert reflector._index_rows(connection, 42) == []

    lowered = connection.statement.lower()
    assert "from sysindices i" in lowered
    assert "sysindexes" not in lowered
    assert "cast(i.indexkeys as lvarchar(8192))" in lowered
    assert "left join sysams" in lowered
    assert "i.collation" in lowered
    assert "i.tabid" in lowered
    assert connection.params == (42,)


def test_reflect_functional_index_from_sysindices_metadata(dialect, monkeypatch):
    reflector = IfxReflector(dialect)
    connection = object()

    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda *args, **kwargs: (42, "people", "informix", "T"),
    )
    monkeypatch.setattr(reflector, "_constraint_duplicates_by_index", lambda *args: {})
    monkeypatch.setattr(
        reflector,
        "_get_column_name_map",
        lambda *args: {1: "id", 2: "name", 3: "surname"},
    )
    monkeypatch.setattr(
        reflector,
        "_index_rows",
        lambda *args: [
            (
                "ix_normalized_name",
                "informix",
                "U",
                "<574> (-2) [1]",
                1,
                "btree",
                "en_US.819",
                42,
            )
        ],
    )
    monkeypatch.setattr(
        reflector,
        "_index_procedure_map",
        lambda *args: {574: {"name": "normalized_name", "owner": "informix"}},
    )
    monkeypatch.setattr(
        reflector,
        "_index_opclass_map",
        lambda *args: {1: {"name": "btree_ops", "owner": "informix", "amid": 1}},
    )

    indexes = reflector.get_indexes(connection, "people")

    assert indexes == [
        {
            "name": "ix_normalized_name",
            "unique": True,
            "column_names": [None],
            "expressions": ["normalized_name(name) DESC"],
            "dialect_options": {
                "informix_procedure": "normalized_name",
                "informix_access_method": "btree",
                "informix_opclass": "btree_ops",
            },
        }
    ]


def test_reflect_mixed_functional_index_keeps_component_positions(dialect, monkeypatch):
    reflector = IfxReflector(dialect)
    connection = object()

    monkeypatch.setattr(
        reflector,
        "_require_table_row",
        lambda *args, **kwargs: (42, "people", "informix", "T"),
    )
    monkeypatch.setattr(reflector, "_constraint_duplicates_by_index", lambda *args: {})
    monkeypatch.setattr(
        reflector,
        "_get_column_name_map",
        lambda *args: {1: "id", 2: "name", 3: "surname"},
    )
    monkeypatch.setattr(
        reflector,
        "_index_rows",
        lambda *args: [
            (
                "ix_mixed",
                "informix",
                "D",
                "1 [1], <574> (2, 3) [1]",
                1,
                "btree",
                "en_US.819",
                42,
            )
        ],
    )
    monkeypatch.setattr(
        reflector,
        "_index_procedure_map",
        lambda *args: {574: {"name": "normalized_name", "owner": "informix"}},
    )
    monkeypatch.setattr(
        reflector,
        "_index_opclass_map",
        lambda *args: {1: {"name": "btree_ops", "owner": "informix", "amid": 1}},
    )

    [index] = reflector.get_indexes(connection, "people")

    assert index["column_names"] == ["id", None]
    assert index["expressions"] == ["id", "normalized_name(name, surname)"]
    assert index["unique"] is False


def test_reflected_functional_text_can_be_recompiled(dialect):
    metadata = MetaData()
    table = Table("people", metadata, Column("name", String(128)))
    index = Index(
        "ix_reflected",
        text("normalized_name(name) DESC"),
        _table=table,
        informix_procedure="normalized_name",
        informix_access_method="btree",
        informix_opclass="btree_ops",
    )

    assert str(CreateIndex(index).compile(dialect=dialect)) == (
        "CREATE INDEX ix_reflected ON people (normalized_name(name) DESC)"
    )


def test_reflected_mixed_functional_text_can_be_recompiled(dialect):
    metadata = MetaData()
    table = Table(
        "people",
        metadata,
        Column("id", Integer),
        Column("name", String(128)),
    )
    index = Index(
        "ix_reflected_mixed",
        table.c.id,
        text("normalized_name(name) DESC"),
        _table=table,
        informix_procedure="normalized_name",
        informix_access_method="btree",
        informix_opclass="btree_ops",
    )

    assert str(CreateIndex(index).compile(dialect=dialect)) == (
        "CREATE INDEX ix_reflected_mixed ON people "
        "(id, normalized_name(name) DESC)"
    )


@pytest.fixture
def functional_index_objects(engine, name_factory):
    suffix = name_factory("fi_")[-8:]
    table_name = f"sa_fi_{suffix}"
    function_name = f"normalize_{suffix}"
    index_name = f"ix_fi_{suffix}"

    create_function_sql = f"""
        CREATE FUNCTION {function_name}(input_value LVARCHAR(128))
        RETURNING LVARCHAR(128)
        WITH (NOT VARIANT);
        RETURN LOWER(TRIM(input_value));
        END FUNCTION
    """

    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", LVARCHAR(128), nullable=True),
    )
    function_call = getattr(func, function_name)(table.c.name)
    Index(
        index_name,
        function_call.desc(),
        unique=True,
        informix_functional=True,
    )

    with engine.begin() as connection:
        connection.exec_driver_sql(create_function_sql)
        metadata.create_all(connection)
        connection.execute(
            table.insert(),
            [
                {"id": 1, "name": "  Alpha "},
                {"id": 2, "name": "Beta"},
                {"id": 3, "name": None},
            ],
        )

    try:
        yield {
            "table": table,
            "table_name": table_name,
            "function_name": function_name,
            "index_name": index_name,
        }
    finally:
        with engine.connect() as connection:
            with suppress(Exception):
                table.drop(connection, checkfirst=True)
                connection.commit()
            with suppress(Exception):
                connection.exec_driver_sql(
                    f"DROP FUNCTION {function_name}"
                )
                connection.commit()


def _functional_index_by_name(indexes, index_name):
    return next(
        index
        for index in indexes
        if str(index["name"]).casefold() == index_name.casefold()
    )


@pytest.mark.requires_informix
def test_functional_index_round_trip_reflection_multi_autoload_and_query(
    engine,
    functional_index_objects,
):
    objects = functional_index_objects
    table = objects["table"]
    function_call = getattr(func, objects["function_name"])(table.c.name)

    with engine.connect() as connection:
        inspector = inspect(connection)
        reflected = _functional_index_by_name(
            inspector.get_indexes(objects["table_name"]),
            objects["index_name"],
        )

        assert reflected["column_names"] == [None]
        assert reflected["unique"] is True
        assert objects["function_name"].casefold() in reflected["expressions"][0].casefold()
        assert reflected["expressions"][0].upper().endswith(" DESC")
        assert reflected["dialect_options"]["informix_procedure"].casefold().endswith(
            objects["function_name"].casefold()
        )
        assert reflected["dialect_options"]["informix_access_method"].casefold() == "btree"
        assert reflected["dialect_options"]["informix_opclass"].casefold().endswith(
            "btree_ops"
        )

        multi = inspector.get_multi_indexes(filter_names=[objects["table_name"]])
        multi_indexes = multi[(None, objects["table_name"])]
        assert _functional_index_by_name(multi_indexes, objects["index_name"])[
            "expressions"
        ] == reflected["expressions"]

        autoloaded = Table(
            objects["table_name"],
            MetaData(),
            autoload_with=connection,
        )
        [autoloaded_index] = [
            index
            for index in autoloaded.indexes
            if str(index.name).casefold() == objects["index_name"].casefold()
        ]
        assert autoloaded_index.unique is True
        assert autoloaded_index.dialect_options["informix"]["procedure"]

        result = connection.execute(
            select(table.c.id).where(function_call == "alpha")
        ).scalar_one()
        assert result == 1

        # The INDEX directive forces Informix to accept this index as the
        # access path for the same functional predicate.
        directive_sql = (
            f"SELECT {{+ INDEX({objects['table_name']} {objects['index_name']}) }} id "
            f"FROM {objects['table_name']} "
            f"WHERE {objects['function_name']}(name) = ?"
        )
        assert connection.exec_driver_sql(directive_sql, ("alpha",)).scalar_one() == 1


@pytest.mark.requires_informix
def test_functional_index_explicit_owner_reflection(engine, functional_index_objects):
    objects = functional_index_objects

    with engine.connect() as connection:
        owner = connection.exec_driver_sql(
            "SELECT USER FROM systables WHERE tabid = 1"
        ).scalar_one()
        indexes = inspect(connection).get_indexes(
            objects["table_name"],
            schema=str(owner).strip(),
        )

    reflected = _functional_index_by_name(indexes, objects["index_name"])
    assert reflected["column_names"] == [None]
    assert reflected["dialect_options"]["informix_procedure"]


@pytest.mark.requires_informix
def test_functional_index_reflects_quoted_identifiers(engine, name_factory):
    suffix = name_factory("fi_quote_")[-8:]
    function_name = f"normalize_q_{suffix}"
    table_name = quoted_name(f"Functional People {suffix}", True)
    column_name = quoted_name("Display Name", True)
    index_name = quoted_name(f"Ix Normalize {suffix}", True)

    create_function_sql = f"""
        CREATE FUNCTION {function_name}(input_value LVARCHAR(128))
        RETURNING LVARCHAR(128)
        WITH (NOT VARIANT);
        RETURN LOWER(TRIM(input_value));
        END FUNCTION
    """

    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column(column_name, LVARCHAR(128)),
    )
    Index(
        index_name,
        getattr(func, function_name)(table.c[column_name]),
        informix_functional=True,
    )

    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(create_function_sql)
            metadata.create_all(connection)

        with engine.connect() as connection:
            reflected = _functional_index_by_name(
                inspect(connection).get_indexes(table_name),
                str(index_name),
            )

        assert str(reflected["name"]) == str(index_name)
        assert reflected["column_names"] == [None]
        assert f'"{column_name}"' in reflected["expressions"][0]
        assert reflected["dialect_options"]["informix_procedure"].casefold().endswith(
            function_name.casefold()
        )
    finally:
        with engine.connect() as connection:
            with suppress(Exception):
                table.drop(connection, checkfirst=True)
                connection.commit()
            with suppress(Exception):
                connection.exec_driver_sql(
                    f"DROP FUNCTION {function_name}"
                )
                connection.commit()


@pytest.mark.requires_informix
def test_functional_index_alembic_autogenerate_is_stable(engine, functional_index_objects):
    alembic = pytest.importorskip("alembic")
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    _ = alembic
    objects = functional_index_objects

    with engine.connect() as connection:
        reflected_metadata = MetaData()
        Table(
            objects["table_name"],
            reflected_metadata,
            autoload_with=connection,
        )
        def include_name(name, type_, parent_names):
            _ = parent_names
            if type_ == "table":
                return (
                    name is not None
                    and str(name).casefold()
                    == objects["table_name"].casefold()
                )
            return True

        context = MigrationContext.configure(
            connection,
            opts={"include_name": include_name},
        )
        differences = compare_metadata(context, reflected_metadata)

    index_differences = [
        difference
        for difference in differences
        if isinstance(difference, tuple)
        and difference
        and difference[0] in {"add_index", "remove_index"}
    ]
    assert index_differences == []
