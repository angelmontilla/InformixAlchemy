from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
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


def verify_official_suite_database(
    dburi: str | None = None,
) -> dict[str, Any]:
    """
    Verifica la base antes de una suite destructiva.
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

    if (
        configured.casefold()
        != expected.casefold()
    ):
        raise RuntimeError(
            f"La URL apunta a {configured!r}, "
            f"pero la base autorizada es "
            f"{expected!r}"
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

            if (
                actual.casefold()
                != expected.casefold()
            ):
                raise RuntimeError(
                    f"Informix conectó a {actual!r}, "
                    f"pero se esperaba {expected!r}"
                )

            inspector = inspect(connection)

            tables = sorted(
                inspector.get_table_names()
            )

            views = sorted(
                inspector.get_view_names()
            )

        require_empty = env_bool(
            "OFFICIAL_SUITE_REQUIRE_EMPTY",
            True,
        )

        if require_empty and (
            tables or views
        ):
            raise RuntimeError(
                "La base exclusiva no está vacía. "
                f"Tablas={tables}; "
                f"vistas={views}"
            )

        return {
            "database": actual,
            "tables": tables,
            "views": views,
            "require_empty": require_empty,
            "safe_url": parsed.render_as_string(
                hide_password=True
            ),
        }

    finally:
        engine.dispose()
