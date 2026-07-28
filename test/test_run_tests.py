from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest
from sqlalchemy.dialects import registry


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_DIR = PROJECT_ROOT / ".local"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if str(LOCAL_DIR) not in sys.path:
    sys.path.insert(0, str(LOCAL_DIR))


from official_suite_db import (  # noqa: E402
    SuiteDatabaseError,
    clean_database,
    collect_inventory,
    create_suite_engine,
    load_settings,
    render_inventory,
    suite_owners,
    verify_database_identity,
)

from tools.official_suite_support import (  # noqa: E402
    resolve_junit_file,
)


def register_dialect() -> None:
    """Registra el dialecto desde el checkout actual del proyecto."""

    registry.register(
        "informix",
        "IfxAlchemy.pyodbc",
        "IfxDialect_pyodbc",
    )
    registry.register(
        "informix.pyodbc",
        "IfxAlchemy.pyodbc",
        "IfxDialect_pyodbc",
    )


def prepare_database():
    """Limpia exactamente la base que recibirá pytest."""

    settings = load_settings(PROJECT_ROOT)

    # Ésta será la única URL usada por limpieza y pytest.
    dburi = settings.dburi

    # Refuerza que cualquier componente que consulte la variable de entorno
    # obtenga exactamente la misma cadena.
    os.environ["INFORMIX_SQLALCHEMY_SUITE_URL"] = dburi

    engine = create_suite_engine(settings)

    try:
        actual_database, current_owner = verify_database_identity(
            engine,
            settings,
        )

        owners = suite_owners(
            settings,
            current_owner,
        )

        print(
            "============================================================",
            file=sys.stderr,
        )
        print(
            "Preparación de la base de la suite oficial",
            file=sys.stderr,
        )
        print(
            "============================================================",
            file=sys.stderr,
        )
        print(
            f"Configuración : {settings.env_file}",
            file=sys.stderr,
        )
        print(
            f"Base real     : {actual_database}",
            file=sys.stderr,
        )
        print(
            f"Usuario       : {current_owner}",
            file=sys.stderr,
        )
        print(
            f"URL segura    : {settings.safe_url}",
            file=sys.stderr,
        )
        print(
            f"Propietarios  : {', '.join(owners)}",
            file=sys.stderr,
        )

        before = collect_inventory(
            engine,
            owners,
        )

        print(
            "Inventario antes de limpiar:",
            file=sys.stderr,
        )
        print(
            render_inventory(before),
            file=sys.stderr,
        )

        dropped = clean_database(
            engine,
            owners,
        )

        print(
            f"Objetos eliminados: {len(dropped)}",
            file=sys.stderr,
        )

        residual = collect_inventory(
            engine,
            owners,
        )

        print(
            "Inventario inmediatamente antes de pytest:",
            file=sys.stderr,
        )
        print(
            render_inventory(residual),
            file=sys.stderr,
        )

        if residual:
            raise SuiteDatabaseError(
                "La base no quedó limpia antes de pytest:\n"
                + render_inventory(residual)
            )

        return settings, dburi, owners

    finally:
        engine.dispose()


def main(argv: list[str] | None = None) -> int:
    register_dialect()

    try:
        settings, dburi, owners = prepare_database()

    except Exception as exc:
        print(
            f"ERROR: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2

    print(
        "Ejecutando la suite oficial sobre la misma URL:",
        file=sys.stderr,
    )
    print(
        settings.safe_url,
        file=sys.stderr,
    )

    suite_targets = [
        "test/test_suite.py",
    ]

    include_outparams = os.getenv(
        "IFXALCHEMY_INCLUDE_OUTPARAMS",
        "0",
    ).casefold() not in {
        "0",
        "false",
        "no",
    }

    if include_outparams:
        suite_targets.append(
            "test/test_out_parameters.py"
        )

    junit_file = resolve_junit_file(
        os.getenv(
            "SQLALCHEMY_SUITE_JUNIT",
            "",
        ),
        "sqlalchemy-suite.xml",
    )

    if junit_file.exists():
        junit_file.unlink()
        print(
            f"Informe anterior eliminado: {junit_file}",
            file=sys.stderr,
        )

    pytest_args = [
        "-c",
        "pytest.ini",
        "-p",
        "sqlalchemy.testing.plugin.pytestplugin",
        *suite_targets,
        "--dburi",
        dburi,
        "--dropfirst",
        "--junitxml",
        str(junit_file),
        "-ra",
    ]

    if argv:
        pytest_args.extend(argv)

    return pytest.main(pytest_args)


if __name__ == "__main__":
    raise SystemExit(
        main(sys.argv[1:])
    )