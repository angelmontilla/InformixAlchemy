from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import make_url


PROJECT_ROOT = Path(__file__).resolve().parents[1]

OFFICIAL_ENV_FILE = (
    PROJECT_ROOT / ".env.official-suites"
)


FORBIDDEN_DEFAULTS = {
    "faempre",
    "faempre_dev",
    "prueba4db",
    "sysmaster",
    "sysadmin",
    "sysutils",
    "sysuser",
}


TRUE_VALUES = {
    "1",
    "true",
    "yes",
    "y",
    "on",
    "si",
    "sí",
}


OFFICIAL_SUITE_OWNERS = (
    "test_schema",
    "test_schema_2",
)


CATALOG_KIND_BY_TABTYPE = {
    "T": "tables",
    "V": "views",
    "Q": "sequences",
}


def env_bool(
    name: str,
    default: bool = False,
) -> bool:
    raw = os.getenv(name)

    if raw is None:
        return default

    return raw.strip().casefold() in TRUE_VALUES


def split_names(raw: str) -> set[str]:
    return {
        item.strip().casefold()
        for item in raw.split(",")
        if item.strip()
    }


def load_official_suite_environment() -> Path:
    """
    Carga el fichero exclusivo de las suites oficiales.

    override=True evita que variables antiguas de CMD hagan
    que la suite se conecte accidentalmente a otra base.
    """
    if not OFFICIAL_ENV_FILE.is_file():
        raise RuntimeError(
            f"No existe {OFFICIAL_ENV_FILE}. "
            "Copie .env.official-suites.example "
            "y configure la base exclusiva."
        )

    loaded = load_dotenv(
        OFFICIAL_ENV_FILE,
        override=True,
    )

    if not loaded:
        raise RuntimeError(
            f"No se pudo cargar {OFFICIAL_ENV_FILE}"
        )

    return OFFICIAL_ENV_FILE


def official_suite_dburi() -> str:
    """
    Devuelve exclusivamente la URL de las suites oficiales.

    No existe fallback a INFORMIX_SQLALCHEMY_URL porque esa
    variable apunta a prueba4db y no debe utilizarse aquí.
    """
    url = os.getenv(
        "INFORMIX_SQLALCHEMY_SUITE_URL",
        "",
    ).strip()

    if not url:
        raise RuntimeError(
            "INFORMIX_SQLALCHEMY_SUITE_URL "
            "no está configurada"
        )

    if "delimident=" not in url.casefold():
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}DELIMIDENT=Y"

    return url


def resolve_junit_file(
    raw_path: str,
    default_name: str,
) -> Path:
    """
    Resuelve y crea el directorio padre del informe JUnit.
    """
    value = raw_path.strip()

    if value:
        path = Path(value)
    else:
        path = (
            PROJECT_ROOT
            / "artifacts"
            / default_name
        )

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    path = path.resolve()

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    return path


def _clean_catalog_name(value: Any) -> str | None:
    if value is None:
        return None

    cleaned = str(value).strip()
    return cleaned or None


def _unique_owner_names(*owners: Any) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()

    for raw_owner in owners:
        owner = _clean_catalog_name(raw_owner)
        if owner is None:
            continue

        key = owner.casefold()
        if key in seen:
            continue

        seen.add(key)
        result.append(owner)

    return tuple(result)


def _collect_official_suite_inventory(
    connection,
) -> dict[str, dict[str, list[str]]]:
    """
    Inventaría tablas, vistas y secuencias de todos los propietarios de test.

    Se consulta directamente ``systables`` para que la autorización previa no
    dependa de las funciones de reflexión que la suite oficial está validando.
    """
    default_owner = _clean_catalog_name(
        connection.exec_driver_sql(
            "SELECT USER FROM systables WHERE tabid = 1"
        ).scalar_one()
    )

    owners = _unique_owner_names(
        default_owner,
        *OFFICIAL_SUITE_OWNERS,
    )

    inventory = {
        owner: {
            "tables": [],
            "views": [],
            "sequences": [],
        }
        for owner in owners
    }

    sql_text = """
        SELECT
            t.tabname,
            t.tabtype
        FROM systables t
        WHERE LOWER(t.owner) = LOWER(?)
          AND t.tabid >= 100
          AND t.tabtype IN ('T', 'V', 'Q')
        ORDER BY t.tabtype, t.tabname
    """

    for owner in owners:
        rows = connection.exec_driver_sql(
            sql_text,
            (owner,),
        ).fetchall()

        for row in rows:
            name = _clean_catalog_name(row[0])
            tabtype = _clean_catalog_name(row[1])

            if name is None or tabtype is None:
                continue

            kind = CATALOG_KIND_BY_TABTYPE.get(
                tabtype.upper()
            )

            if kind is not None:
                inventory[owner][kind].append(name)

        for names in inventory[owner].values():
            names.sort(key=str.casefold)

    return inventory


def _non_empty_inventory(
    inventory: dict[str, dict[str, list[str]]],
) -> dict[str, dict[str, list[str]]]:
    return {
        owner: {
            kind: list(names)
            for kind, names in objects.items()
            if names
        }
        for owner, objects in inventory.items()
        if any(objects.values())
    }


def verify_official_suite_database(
    dburi: str | None = None,
    *,
    require_empty: bool | None = None,
) -> dict[str, Any]:
    """
    Verifica la identidad y el inventario de la base destructiva.

    ``require_empty=False`` sólo relaja la comprobación de inventario. Nunca
    desactiva las barreras de nombre esperado, bases prohibidas o autorización
    destructiva explícita.
    """
    if not env_bool(
        "ALLOW_OFFICIAL_SUITE_DESTRUCTIVE_TESTS",
        False,
    ):
        raise RuntimeError(
            "ALLOW_OFFICIAL_SUITE_DESTRUCTIVE_TESTS "
            "debe ser true"
        )

    expected = os.getenv(
        "OFFICIAL_SUITE_EXPECTED_DATABASE",
        "",
    ).strip()

    if not expected:
        raise RuntimeError(
            "OFFICIAL_SUITE_EXPECTED_DATABASE "
            "es obligatoria"
        )

    forbidden = FORBIDDEN_DEFAULTS | split_names(
        os.getenv(
            "FORBIDDEN_DATABASE_NAMES",
            "",
        )
    )

    if expected.casefold() in forbidden:
        raise RuntimeError(
            f"La base esperada {expected!r} "
            "está prohibida"
        )

    url = dburi or official_suite_dburi()
    parsed = make_url(url)
    configured = (
        parsed.database or ""
    ).strip()

    if configured.casefold() != expected.casefold():
        raise RuntimeError(
            f"La URL apunta a {configured!r}, "
            f"pero la base autorizada es {expected!r}"
        )

    if require_empty is None:
        require_empty = env_bool(
            "OFFICIAL_SUITE_REQUIRE_EMPTY",
            True,
        )

    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={
            "timeout": 15,
        },
    )

    try:
        with engine.connect() as connection:
            actual = str(
                connection.execute(
                    text(
                        "SELECT DBINFO('dbname') "
                        "FROM systables "
                        "WHERE tabid = 1"
                    )
                ).scalar_one()
            ).strip()

            if actual.casefold() != expected.casefold():
                raise RuntimeError(
                    f"Informix conectó a {actual!r}, "
                    f"pero se esperaba {expected!r}"
                )

            inventory = _collect_official_suite_inventory(
                connection
            )

        dirty_inventory = _non_empty_inventory(
            inventory
        )

        if require_empty and dirty_inventory:
            raise RuntimeError(
                "La base exclusiva contiene objetos residuales "
                "de la suite oficial. "
                f"Inventario={dirty_inventory}"
            )

        default_owner = next(iter(inventory), None)
        default_objects = (
            inventory.get(default_owner, {})
            if default_owner is not None
            else {}
        )

        return {
            "database": actual,
            "default_owner": default_owner,
            "owners": tuple(inventory),
            "inventory": inventory,
            "dirty_inventory": dirty_inventory,
            "has_objects": bool(dirty_inventory),
            "tables": list(
                default_objects.get("tables", [])
            ),
            "views": list(
                default_objects.get("views", [])
            ),
            "sequences": list(
                default_objects.get("sequences", [])
            ),
            "require_empty": require_empty,
            "safe_url": parsed.render_as_string(
                hide_password=True
            ),
        }

    finally:
        engine.dispose()
