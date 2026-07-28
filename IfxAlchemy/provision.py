# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any

from sqlalchemy import exc
from sqlalchemy.testing.provision import drop_all_schema_objects_post_tables
from sqlalchemy.testing.provision import drop_all_schema_objects_pre_tables
from sqlalchemy.testing.provision import post_configure_engine
from sqlalchemy.testing.provision import temp_table_keyword_args


_TEST_SCHEMA_OWNERS = (
    "test_schema",
    "test_schema_2",
)

_CATALOG_RELATION_TYPES = (
    "V",
    "T",
)

_CATALOG_SEQUENCE_TYPE = "Q"


def _quote_authorization_identifier(connection, name):
    return (
        connection.dialect
        .identifier_preparer
        .quote_identifier(name)
    )


def _qualified_identifier(connection, owner, name):
    quoted_owner = _quote_authorization_identifier(
        connection,
        owner,
    )
    quoted_name = _quote_authorization_identifier(
        connection,
        name,
    )
    return f"{quoted_owner}.{quoted_name}"


def _clean_catalog_name(value: Any) -> str | None:
    if value is None:
        return None

    cleaned = str(value).strip()
    return cleaned or None


def _suite_object_owners(cfg, connection) -> tuple[str, ...]:
    """
    Devuelve los propietarios cuyo contenido puede crear la suite oficial.

    Se incluyen el propietario por defecto de la conexión y los dos
    propietarios configurados por SQLAlchemy. La comparación se hace sin
    distinguir mayúsculas para evitar ejecutar dos veces el mismo borrado.
    """
    default_owner = _clean_catalog_name(
        getattr(
            connection.dialect,
            "default_schema_name",
            None,
        )
    )

    if default_owner is None:
        default_owner = _clean_catalog_name(
            connection.exec_driver_sql(
                "SELECT USER FROM systables WHERE tabid = 1"
            ).scalar_one()
        )

    candidates = (
        default_owner,
        _clean_catalog_name(
            getattr(cfg, "test_schema", None)
        ),
        _clean_catalog_name(
            getattr(cfg, "test_schema_2", None)
        ),
        *_TEST_SCHEMA_OWNERS,
    )

    owners: list[str] = []
    seen: set[str] = set()

    for owner in candidates:
        if owner is None:
            continue

        key = owner.casefold()
        if key in seen:
            continue

        seen.add(key)
        owners.append(owner)

    return tuple(owners)


def _catalog_objects(
    connection,
    owners: tuple[str, ...],
    tabtypes: tuple[str, ...],
) -> list[dict[str, Any]]:
    """
    Inventaría objetos de usuario directamente desde ``systables``.

    ``tabid >= 100`` excluye los catálogos del sistema. El propietario se
    consulta por separado para mantener una parametrización simple y segura
    con el driver ODBC de Informix.
    """
    if not tabtypes:
        return []

    placeholders = ", ".join("?" for _ in tabtypes)
    sql_text = f"""
        SELECT
            t.owner,
            t.tabname,
            t.tabtype,
            t.tabid
        FROM systables t
        WHERE LOWER(t.owner) = LOWER(?)
          AND t.tabid >= 100
          AND t.tabtype IN ({placeholders})
        ORDER BY t.tabid DESC
    """

    objects: list[dict[str, Any]] = []

    for owner in owners:
        rows = connection.exec_driver_sql(
            sql_text,
            (owner, *tabtypes),
        ).fetchall()

        for row in rows:
            object_owner = _clean_catalog_name(row[0])
            object_name = _clean_catalog_name(row[1])
            object_type = _clean_catalog_name(row[2])

            if (
                object_owner is None
                or object_name is None
                or object_type is None
            ):
                continue

            normalized_type = object_type.upper()
            tabid = int(row[3])

            if normalized_type not in tabtypes or tabid < 100:
                continue

            objects.append(
                {
                    "owner": object_owner,
                    "name": object_name,
                    "type": normalized_type,
                    "tabid": tabid,
                }
            )

    return objects


def _drop_catalog_object(connection, catalog_object) -> None:
    """Elimina un objeto ya inventariado con DDL cualificado."""
    object_type = catalog_object["type"]
    qualified_name = _qualified_identifier(
        connection,
        catalog_object["owner"],
        catalog_object["name"],
    )

    if object_type == "V":
        statement = f"DROP VIEW {qualified_name}"
        object_label = "view"
    elif object_type == "T":
        statement = f"DROP TABLE {qualified_name} CASCADE"
        object_label = "table"
    elif object_type == "Q":
        statement = f"DROP SEQUENCE {qualified_name}"
        object_label = "sequence"
    else:
        raise RuntimeError(
            "Unsupported Informix catalog object type: "
            f"{object_type!r}"
        )

    try:
        connection.exec_driver_sql(statement)
    except exc.DBAPIError as error:
        raise RuntimeError(
            "Unable to remove stale Informix "
            f"{object_label} {qualified_name} before the official suite."
        ) from error


def _ensure_test_schema_owner(connection, owner):
    """Garantiza que owner pueda poseer objetos en la BD de pruebas."""

    row = connection.exec_driver_sql(
        """
        SELECT FIRST 1 u.usertype
        FROM sysusers u
        WHERE LOWER(u.username) = LOWER(?)
        """,
        (owner,),
    ).first()

    quoted_owner = _quote_authorization_identifier(
        connection,
        owner,
    )

    if row is None:
        connection.exec_driver_sql(
            f"GRANT CONNECT TO {quoted_owner}"
        )
        connection.exec_driver_sql(
            f"GRANT RESOURCE TO {quoted_owner}"
        )
        return

    usertype = str(row[0]).strip().upper()

    if usertype == "G":
        raise RuntimeError(
            f"Informix authorization identifier {owner!r} "
            "is a role, but the SQLAlchemy suite requires "
            "an object owner."
        )

    if usertype not in {"D", "R"}:
        connection.exec_driver_sql(
            f"GRANT RESOURCE TO {quoted_owner}"
        )


@post_configure_engine.for_db("informix")
def _informix_post_configure_engine(
    url,
    engine,
    follower_ident,
):
    """Prepara los dos propietarios usados por la suite oficial."""

    _ = (
        url,
        follower_ident,
    )

    try:
        with engine.begin() as connection:
            for owner in _TEST_SCHEMA_OWNERS:
                _ensure_test_schema_owner(
                    connection,
                    owner,
                )

    except exc.DBAPIError as error:
        raise RuntimeError(
            "Unable to provision the Informix owners required "
            "by the SQLAlchemy schema suite: test_schema and "
            "test_schema_2. Run the suite with a DBA account "
            "or provision both authorization identifiers "
            "manually."
        ) from error


@drop_all_schema_objects_pre_tables.for_db("informix")
def _informix_drop_all_schema_objects_pre_tables(cfg, eng):
    """
    Elimina vistas y tablas residuales antes del barrido genérico.

    Las vistas se eliminan antes que las tablas para romper dependencias. Las
    tablas se borran con ``CASCADE`` para retirar claves foráneas dependientes.
    """
    with eng.begin() as connection:
        owners = _suite_object_owners(
            cfg,
            connection,
        )
        objects = _catalog_objects(
            connection,
            owners,
            _CATALOG_RELATION_TYPES,
        )

        for object_type in _CATALOG_RELATION_TYPES:
            for catalog_object in objects:
                if catalog_object["type"] == object_type:
                    _drop_catalog_object(
                        connection,
                        catalog_object,
                    )


@drop_all_schema_objects_post_tables.for_db("informix")
def _informix_drop_all_schema_objects_post_tables(cfg, eng):
    """Elimina secuencias residuales después de retirar las tablas."""
    with eng.begin() as connection:
        owners = _suite_object_owners(
            cfg,
            connection,
        )
        sequences = _catalog_objects(
            connection,
            owners,
            (_CATALOG_SEQUENCE_TYPE,),
        )

        for sequence in sequences:
            _drop_catalog_object(
                connection,
                sequence,
            )


@temp_table_keyword_args.for_db("informix")
def _informix_temp_table_keyword_args(cfg, eng):
    _ = (
        cfg,
        eng,
    )
    return {
        "prefixes": ["TEMP"],
    }
