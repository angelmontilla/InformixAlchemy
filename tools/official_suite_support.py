from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NON_ANSI_ENV_FILE = PROJECT_ROOT / ".env.informix"
OFFICIAL_ENV_FILE = PROJECT_ROOT / ".env.official-suites"

DEFAULT_NON_ANSI_DATABASE = "ifxalchemy_test"
DEFAULT_ANSI_DATABASE = "ifxalchemy_test_ansi"
DEFAULT_ADMIN_DATABASE = "sysmaster"
DEFAULT_DOCKER_CONTAINER = "ifx"
DEFAULT_DOCKER_USER = "informix"
DEFAULT_DOCKER_DBACCESS = ""
DEFAULT_DOCKER_INFORMIXSERVER = "informix"
DEFAULT_DOCKER_CLIENT_LOCALE = "en_US.819"
DEFAULT_DOCKER_DB_LOCALE = "en_US.819"
DEFAULT_DOCKER_SERVER_LOCALE = "en_US.819"
DEFAULT_DOCKER_URL = (
    "informix+pyodbc://informix:in4mix@127.0.0.1/ifxalchemy_test"
    "?DELIMIDENT=Y"
    "&driver=IBM+INFORMIX+ODBC+DRIVER+%2864-bit%29"
    "&protocol=onsoctcp"
    "&server=informix"
    "&service=9088"
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

_SAFE_INFORMIX_IDENTIFIER = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_$]{0,127}$"
)
_SAFE_DOCKER_TOKEN = re.compile(r"^[A-Za-z0-9_.-]+$")

DATABASE_STATE_SQL = """
    SELECT FIRST 1
        d.name,
        d.is_ansi,
        d.is_logging
    FROM sysmaster:sysdatabases d
    WHERE LOWER(d.name) = LOWER(?)
"""


@dataclass(frozen=True)
class DatabaseState:
    name: str
    is_ansi: bool
    is_logging: bool


def env_bool(name: str, default: bool = False) -> bool:
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


def _clean_catalog_name(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _validated_informix_identifier(
    value: str,
    *,
    variable_name: str,
) -> str:
    candidate = value.strip()
    if not candidate:
        raise RuntimeError(f"{variable_name} no puede estar vacía")
    if _SAFE_INFORMIX_IDENTIFIER.fullmatch(candidate) is None:
        raise RuntimeError(
            f"{variable_name} contiene un identificador Informix no seguro: "
            f"{candidate!r}. Use letras ASCII, dígitos, '_', '$' y no "
            "comience por un dígito."
        )
    return candidate


def non_ansi_database_name() -> str:
    return _validated_informix_identifier(
        os.getenv(
            "IFXALCHEMY_NON_ANSI_DATABASE",
            DEFAULT_NON_ANSI_DATABASE,
        ),
        variable_name="IFXALCHEMY_NON_ANSI_DATABASE",
    )


def ansi_database_name() -> str:
    return _validated_informix_identifier(
        os.getenv(
            "IFXALCHEMY_ANSI_DATABASE",
            DEFAULT_ANSI_DATABASE,
        ),
        variable_name="IFXALCHEMY_ANSI_DATABASE",
    )


def _with_delimident(url: str) -> str:
    if "delimident=" in url.casefold():
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}DELIMIDENT=Y"


def _render_url(url: URL) -> str:
    return _with_delimident(
        url.render_as_string(hide_password=False)
    )


def _profile_url(
    *,
    database: str,
    explicit_variable: str,
    seed_variables: tuple[str, ...],
) -> str:
    explicit = os.getenv(explicit_variable, "").strip()
    if explicit:
        parsed = make_url(_with_delimident(explicit))
        configured = (parsed.database or "").strip()
        if configured.casefold() != database.casefold():
            raise RuntimeError(
                f"{explicit_variable} apunta a {configured!r}, pero debe "
                f"apuntar a {database!r}."
            )
        return _render_url(parsed)

    seed = ""
    for variable in seed_variables:
        candidate = os.getenv(variable, "").strip()
        if candidate:
            seed = candidate
            break

    parsed = make_url(_with_delimident(seed or DEFAULT_DOCKER_URL))
    return _render_url(parsed.set(database=database))


def non_ansi_test_dburi() -> str:
    """Return the URL reserved for package and integration tests.

    The normal profile never uses the ANSI profile URL as an explicit source.
    Legacy ``INFORMIX_SQLALCHEMY_URL`` remains a supported connection seed, but
    its database component is always replaced with the configured non-ANSI
    database name.
    """
    return _profile_url(
        database=non_ansi_database_name(),
        explicit_variable="INFORMIX_SQLALCHEMY_NON_ANSI_URL",
        seed_variables=(
            "INFORMIX_SQLALCHEMY_URL",
            "INFORMIX_SQLALCHEMY_SUITE_URL",
        ),
    )


def ansi_test_dburi() -> str:
    """Return the URL reserved for schema-sensitive official suites.

    The official-suite URL is accepted as a legacy connection seed. Its
    database component is replaced with the configured ANSI database name, so
    an old file that still names ``ifxalchemy_test`` cannot route the suite to
    the non-ANSI database.
    """
    return _profile_url(
        database=ansi_database_name(),
        explicit_variable="INFORMIX_SQLALCHEMY_ANSI_URL",
        seed_variables=(
            "INFORMIX_SQLALCHEMY_SUITE_URL",
            "INFORMIX_SQLALCHEMY_URL",
        ),
    )


def official_suite_dburi() -> str:
    """The official SQLAlchemy and Alembic suites always use the ANSI DB."""
    return ansi_test_dburi()


def _coerce_catalog_flag(
    database_name: str,
    column_name: str,
    raw_value: Any,
) -> bool:
    try:
        return bool(int(raw_value))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Informix devolvió {column_name} no reconocido para la base "
            f"{database_name!r}: {raw_value!r}"
        ) from exc


def _database_state_row(connection, database_name: str):
    return connection.exec_driver_sql(
        DATABASE_STATE_SQL,
        (database_name,),
    ).first()


def _database_state(
    connection,
    database_name: str,
) -> DatabaseState | None:
    row = _database_state_row(connection, database_name)
    if row is None:
        return None

    actual_name = _clean_catalog_name(row[0])
    if actual_name is None:
        raise RuntimeError(
            "sysmaster:sysdatabases devolvió un nombre de base vacío"
        )
    if actual_name.casefold() != database_name.casefold():
        raise RuntimeError(
            "sysmaster:sysdatabases devolvió una base inesperada: "
            f"{actual_name!r}; se esperaba {database_name!r}"
        )

    return DatabaseState(
        name=actual_name,
        is_ansi=_coerce_catalog_flag(actual_name, "is_ansi", row[1]),
        is_logging=_coerce_catalog_flag(
            actual_name,
            "is_logging",
            row[2],
        ),
    )


def _database_mode(connection, database_name: str) -> bool | None:
    """Backward-compatible helper used by existing tests and callers."""
    state = _database_state(connection, database_name)
    return None if state is None else state.is_ansi


def _database_creation_enabled() -> bool:
    if "IFXALCHEMY_CREATE_TEST_DATABASES_IF_MISSING" in os.environ:
        return env_bool(
            "IFXALCHEMY_CREATE_TEST_DATABASES_IF_MISSING",
            True,
        )
    return env_bool(
        "OFFICIAL_SUITE_CREATE_ANSI_IF_MISSING",
        True,
    )


def _database_dbspace(profile: str) -> str | None:
    specific_name = (
        "IFXALCHEMY_ANSI_DATABASE_DBSPACE"
        if profile == "ansi"
        else "IFXALCHEMY_NON_ANSI_DATABASE_DBSPACE"
    )
    raw = os.getenv(specific_name, "").strip()
    if not raw:
        raw = os.getenv(
            "OFFICIAL_SUITE_DATABASE_DBSPACE",
            "",
        ).strip()
    if not raw:
        return None
    return _validated_informix_identifier(
        raw,
        variable_name=specific_name,
    )


def _validated_docker_token(value: str, *, variable_name: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise RuntimeError(f"{variable_name} no puede estar vacía")
    if _SAFE_DOCKER_TOKEN.fullmatch(candidate) is None:
        raise RuntimeError(
            f"{variable_name} contiene un valor no seguro: {candidate!r}. "
            "Use letras ASCII, dígitos, '.', '_' o '-'."
        )
    return candidate


def _validated_docker_env_value(
    value: str,
    *,
    variable_name: str,
) -> str:
    candidate = value.strip()
    if not candidate:
        raise RuntimeError(f"{variable_name} no puede estar vacía")
    if "\x00" in candidate or "\n" in candidate or "\r" in candidate:
        raise RuntimeError(
            f"{variable_name} contiene caracteres de control no permitidos"
        )
    return candidate


def _validated_container_path(
    value: str,
    *,
    variable_name: str,
) -> str:
    candidate = _validated_docker_env_value(
        value,
        variable_name=variable_name,
    )
    if not candidate.startswith("/"):
        raise RuntimeError(
            f"{variable_name} debe ser una ruta absoluta dentro del "
            f"contenedor: {candidate!r}"
        )
    return candidate


def _dbaccess_container_environment(
    dbaccess_executable: str,
) -> dict[str, str]:
    """Build the complete Informix environment for non-login DB-Access.

    ``docker exec`` does not read the ``informix`` user's login profile.  The
    server image commonly defines INFORMIXDIR, INFORMIXSERVER and GLS locales
    there rather than in Docker's global environment.  Invoking DB-Access by
    absolute path is therefore not sufficient: without INFORMIXDIR its GLS
    loader fails with Informix error -23101.
    """
    configured_informixdir = os.getenv(
        "IFXALCHEMY_DOCKER_INFORMIXDIR",
        "",
    ).strip()
    if configured_informixdir:
        informixdir = _validated_container_path(
            configured_informixdir,
            variable_name="IFXALCHEMY_DOCKER_INFORMIXDIR",
        )
    else:
        executable_path = PurePosixPath(dbaccess_executable)
        if executable_path.name != "dbaccess" or executable_path.parent.name != "bin":
            raise RuntimeError(
                "No se pudo deducir INFORMIXDIR desde la ruta de DB-Access "
                f"{dbaccess_executable!r}. Configure "
                "IFXALCHEMY_DOCKER_INFORMIXDIR explícitamente."
            )
        informixdir = str(executable_path.parent.parent)

    informixserver = _validated_docker_env_value(
        os.getenv(
            "IFXALCHEMY_DOCKER_INFORMIXSERVER",
            DEFAULT_DOCKER_INFORMIXSERVER,
        ),
        variable_name="IFXALCHEMY_DOCKER_INFORMIXSERVER",
    )
    client_locale = _validated_docker_env_value(
        os.getenv(
            "IFXALCHEMY_DOCKER_CLIENT_LOCALE",
            DEFAULT_DOCKER_CLIENT_LOCALE,
        ),
        variable_name="IFXALCHEMY_DOCKER_CLIENT_LOCALE",
    )
    database_locale = _validated_docker_env_value(
        os.getenv(
            "IFXALCHEMY_DOCKER_DB_LOCALE",
            DEFAULT_DOCKER_DB_LOCALE,
        ),
        variable_name="IFXALCHEMY_DOCKER_DB_LOCALE",
    )
    server_locale = _validated_docker_env_value(
        os.getenv(
            "IFXALCHEMY_DOCKER_SERVER_LOCALE",
            DEFAULT_DOCKER_SERVER_LOCALE,
        ),
        variable_name="IFXALCHEMY_DOCKER_SERVER_LOCALE",
    )

    default_path = (
        f"{informixdir}/bin:/usr/local/sbin:/usr/local/bin:"
        "/usr/sbin:/usr/bin:/sbin:/bin"
    )
    default_library_path = (
        f"{informixdir}/lib:{informixdir}/lib/esql:"
        f"{informixdir}/lib/cli"
    )

    return {
        "INFORMIXDIR": informixdir,
        "INFORMIXSERVER": informixserver,
        "CLIENT_LOCALE": client_locale,
        "DB_LOCALE": database_locale,
        "SERVER_LOCALE": server_locale,
        "PATH": _validated_docker_env_value(
            os.getenv("IFXALCHEMY_DOCKER_PATH", default_path),
            variable_name="IFXALCHEMY_DOCKER_PATH",
        ),
        "LD_LIBRARY_PATH": _validated_docker_env_value(
            os.getenv(
                "IFXALCHEMY_DOCKER_LD_LIBRARY_PATH",
                default_library_path,
            ),
            variable_name="IFXALCHEMY_DOCKER_LD_LIBRARY_PATH",
        ),
        "TERM": _validated_docker_env_value(
            os.getenv("IFXALCHEMY_DOCKER_TERM", "dumb"),
            variable_name="IFXALCHEMY_DOCKER_TERM",
        ),
    }


def _docker_exec_environment_arguments(
    environment: dict[str, str],
) -> list[str]:
    arguments: list[str] = []
    for name in (
        "INFORMIXDIR",
        "INFORMIXSERVER",
        "CLIENT_LOCALE",
        "DB_LOCALE",
        "SERVER_LOCALE",
        "PATH",
        "LD_LIBRARY_PATH",
        "TERM",
    ):
        arguments.extend(("-e", f"{name}={environment[name]}"))
    return arguments


def _discover_dbaccess_path(
    *,
    docker_executable: str,
    container_name: str,
    container_user: str,
    timeout: int,
) -> str:
    """Return the absolute DB-Access path inside the Docker container.

    No login shell is started. An explicitly configured absolute path wins;
    otherwise Docker executes ``find`` directly inside the container and the
    discovered executable is later invoked by its absolute path.
    """
    configured = os.getenv(
        "IFXALCHEMY_DOCKER_DBACCESS",
        DEFAULT_DOCKER_DBACCESS,
    ).strip()
    if configured:
        if not configured.startswith("/"):
            raise RuntimeError(
                "IFXALCHEMY_DOCKER_DBACCESS debe ser una ruta absoluta "
                "dentro del contenedor, por ejemplo "
                "'/opt/ibm/informix/bin/dbaccess'."
            )
        return configured

    diagnostics: list[str] = []
    for find_executable in ("/usr/bin/find", "/bin/find"):
        command = [
            docker_executable,
            "exec",
            "-u",
            container_user,
            container_name,
            find_executable,
            "/opt",
            "-type",
            "f",
            "-name",
            "dbaccess",
            "-print",
        ]
        try:
            completed = subprocess.run(
                command,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=min(timeout, 30),
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"No se encontró Docker ({docker_executable!r}). Instálelo "
                "o configure IFXALCHEMY_DOCKER_EXECUTABLE."
            ) from exc
        except subprocess.TimeoutExpired:
            diagnostics.append(
                f"{find_executable}: búsqueda agotada tras {min(timeout, 30)} s"
            )
            continue

        paths = [
            line.strip()
            for line in completed.stdout.splitlines()
            if line.strip().startswith("/")
            and line.strip().endswith("/dbaccess")
        ]
        if paths:
            paths.sort(
                key=lambda value: (
                    0 if value.endswith("/informix/bin/dbaccess") else 1,
                    len(value),
                    value,
                )
            )
            return paths[0]

        diagnostic = "\n".join(
            part.strip()
            for part in (completed.stdout, completed.stderr)
            if part and part.strip()
        )
        diagnostics.append(
            f"{find_executable} devolvió {completed.returncode}"
            + (f": {diagnostic}" if diagnostic else "")
        )

    detail = "; ".join(diagnostics) if diagnostics else "sin diagnóstico"
    raise RuntimeError(
        "No se pudo localizar DB-Access dentro del contenedor "
        f"{container_name!r}. Configure IFXALCHEMY_DOCKER_DBACCESS con "
        "la ruta absoluta obtenida desde cmd.exe mediante: "
        f"docker exec -u {container_user} {container_name} "
        "/usr/bin/find /opt -type f -name dbaccess -print. "
        f"Diagnóstico: {detail}"
    )


def _create_database_with_docker(create_sql: str) -> None:
    """Run CREATE DATABASE through DB-Access without a current database.

    Informix ODBC rejects CREATE/DROP DATABASE on a connection already logged
    into a database such as sysmaster. DB-Access is therefore executed directly
    inside Docker by absolute path, with no PowerShell, Bash or login shell.
    """
    docker_executable = os.getenv(
        "IFXALCHEMY_DOCKER_EXECUTABLE",
        "docker",
    ).strip() or "docker"
    container_name = _validated_docker_token(
        os.getenv(
            "IFXALCHEMY_DOCKER_CONTAINER",
            DEFAULT_DOCKER_CONTAINER,
        ),
        variable_name="IFXALCHEMY_DOCKER_CONTAINER",
    )
    container_user = _validated_docker_token(
        os.getenv(
            "IFXALCHEMY_DOCKER_USER",
            DEFAULT_DOCKER_USER,
        ),
        variable_name="IFXALCHEMY_DOCKER_USER",
    )
    try:
        timeout = int(os.getenv("IFXALCHEMY_DOCKER_TIMEOUT", "120"))
    except ValueError as exc:
        raise RuntimeError(
            "IFXALCHEMY_DOCKER_TIMEOUT debe ser un entero positivo"
        ) from exc
    if timeout <= 0:
        raise RuntimeError(
            "IFXALCHEMY_DOCKER_TIMEOUT debe ser un entero positivo"
        )

    dbaccess_executable = _discover_dbaccess_path(
        docker_executable=docker_executable,
        container_name=container_name,
        container_user=container_user,
        timeout=timeout,
    )
    container_environment = _dbaccess_container_environment(
        dbaccess_executable
    )
    command = [
        docker_executable,
        "exec",
        "-i",
        "-u",
        container_user,
        *_docker_exec_environment_arguments(container_environment),
        container_name,
        dbaccess_executable,
        "-",
        "-",
    ]
    sql_input = create_sql.rstrip().rstrip(";") + ";\n"

    try:
        completed = subprocess.run(
            command,
            input=sql_input,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"No se encontró Docker ({docker_executable!r}). Instálelo o "
            "configure IFXALCHEMY_DOCKER_EXECUTABLE."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"La creación de la base Informix superó {timeout} segundos."
        ) from exc

    if completed.returncode != 0:
        diagnostic = "\n".join(
            part.strip()
            for part in (completed.stdout, completed.stderr)
            if part and part.strip()
        )
        locale_hint = ""
        if "-23101" in diagnostic or "Unable to load locale categories" in diagnostic:
            locale_hint = (
                "\nDB-Access no pudo cargar GLS. Revise que existan "
                f"{container_environment['INFORMIXDIR']}/gls y que "
                "IFXALCHEMY_DOCKER_CLIENT_LOCALE, "
                "IFXALCHEMY_DOCKER_DB_LOCALE e "
                "IFXALCHEMY_DOCKER_SERVER_LOCALE identifiquen locales "
                "instaladas en el contenedor."
            )
        raise RuntimeError(
            "DB-Access no pudo crear la base dentro del contenedor "
            f"{container_name!r} usando {dbaccess_executable!r} "
            f"(código {completed.returncode})."
            + (f"\n{diagnostic}" if diagnostic else "")
            + locale_hint
        )


def _read_database_state(
    admin_url: URL,
    database_name: str,
) -> DatabaseState | None:
    engine = create_engine(
        admin_url,
        isolation_level="AUTOCOMMIT",
        pool_pre_ping=True,
        connect_args={"timeout": 15},
    )
    try:
        with engine.connect() as connection:
            return _database_state(connection, database_name)
    finally:
        engine.dispose()


def _ensure_database(
    dburi: str,
    *,
    expected_database: str,
    expected_ansi: bool,
    profile: str,
) -> dict[str, Any]:
    expected_database = _validated_informix_identifier(
        expected_database,
        variable_name=f"{profile.upper()}_DATABASE",
    )
    parsed = make_url(dburi)
    configured = (parsed.database or "").strip()
    if configured.casefold() != expected_database.casefold():
        raise RuntimeError(
            f"La URL {profile!r} apunta a {configured!r}, pero se esperaba "
            f"{expected_database!r}."
        )

    creation_enabled = _database_creation_enabled()
    admin_database = _validated_informix_identifier(
        os.getenv(
            "IFXALCHEMY_ADMIN_DATABASE",
            os.getenv(
                "OFFICIAL_SUITE_ADMIN_DATABASE",
                DEFAULT_ADMIN_DATABASE,
            ),
        ),
        variable_name="IFXALCHEMY_ADMIN_DATABASE",
    )
    dbspace = _database_dbspace(profile)
    admin_url = parsed.set(database=admin_database)

    # ODBC is used only to inspect sysmaster. Close the connection before
    # creating a database: Informix requires CREATE/DROP DATABASE to run in a
    # server-only session rather than on a connection logged into sysmaster.
    state = _read_database_state(admin_url, expected_database)
    if state is not None:
        if state.is_ansi != expected_ansi:
            actual_mode = "ANSI" if state.is_ansi else "no ANSI"
            wanted_mode = "ANSI" if expected_ansi else "no ANSI"
            raise RuntimeError(
                f"La base {expected_database!r} ya existe en modo "
                f"{actual_mode}, pero el perfil {profile!r} requiere "
                f"modo {wanted_mode}. No se elimina ni se convierte "
                "automáticamente."
            )
        if not state.is_logging:
            raise RuntimeError(
                f"La base {expected_database!r} existe sin logging. "
                "Las pruebas requieren transacciones; créela con "
                "WITH LOG o WITH LOG MODE ANSI."
            )
        created = False
    elif not creation_enabled:
        return {
            "database": expected_database,
            "profile": profile,
            "created": False,
            "exists": False,
            "is_ansi_database": None,
            "is_logging": None,
            "creation_disabled": True,
            "safe_url": parsed.render_as_string(hide_password=True),
        }
    else:
        location = f" IN {dbspace}" if dbspace else ""
        logging_clause = (
            "WITH LOG MODE ANSI"
            if expected_ansi
            else "WITH LOG"
        )
        create_sql = (
            f"CREATE DATABASE {expected_database}"
            f"{location} {logging_clause}"
        )
        _create_database_with_docker(create_sql)
        created = True
        state = _read_database_state(admin_url, expected_database)

    if state is None:
        raise RuntimeError(
            f"No se pudo confirmar la creación de {expected_database!r}. "
            "Revise la salida de Docker/DB-Access y el nombre del contenedor."
        )
    if state.is_ansi != expected_ansi or not state.is_logging:
        raise RuntimeError(
            f"La base {expected_database!r} no quedó en el modo esperado: "
            f"is_ansi={int(state.is_ansi)}, "
            f"is_logging={int(state.is_logging)}."
        )

    return {
        "database": state.name,
        "profile": profile,
        "created": created,
        "exists": True,
        "is_ansi_database": state.is_ansi,
        "is_logging": state.is_logging,
        "creation_disabled": False,
        "admin_database": admin_database,
        "dbspace": dbspace,
        "safe_url": parsed.render_as_string(hide_password=True),
    }


def ensure_non_ansi_test_database(
    dburi: str | None = None,
) -> dict[str, Any]:
    return _ensure_database(
        dburi or non_ansi_test_dburi(),
        expected_database=non_ansi_database_name(),
        expected_ansi=False,
        profile="non_ansi",
    )


def ensure_ansi_test_database(
    dburi: str | None = None,
) -> dict[str, Any]:
    return _ensure_database(
        dburi or ansi_test_dburi(),
        expected_database=ansi_database_name(),
        expected_ansi=True,
        profile="ansi",
    )


def ensure_official_suite_ansi_database(
    dburi: str | None = None,
) -> dict[str, Any]:
    """Backward-compatible name for ANSI suite provisioning."""
    return ensure_ansi_test_database(dburi)


def ensure_required_test_databases() -> dict[str, dict[str, Any]]:
    """Provision and certify the two Docker test databases."""
    return {
        "non_ansi": ensure_non_ansi_test_database(),
        "ansi": ensure_ansi_test_database(),
    }


def _load_environment_file(
    path: Path,
    *,
    required: bool,
    example_name: str,
) -> Path | None:
    """Load a local test environment without overwriting explicit variables.

    Environment variables supplied by the shell or CI take precedence over
    dotenv files. This prevents test order from switching a previously selected
    database profile halfway through a pytest session.
    """
    if not path.is_file():
        if required:
            raise RuntimeError(
                f"No existe {path}. Copie {example_name} y revise la "
                "configuración."
            )
        return None

    load_dotenv(path, override=False)
    return path


def load_non_ansi_test_environment(
    *,
    required: bool = False,
) -> Path | None:
    """Load ``.env.informix`` for normal package/integration tests."""
    return _load_environment_file(
        NON_ANSI_ENV_FILE,
        required=required,
        example_name=".env.informix.example",
    )


def load_official_suite_environment(
    *,
    required: bool = True,
) -> Path | None:
    """Load ``.env.official-suites`` for SQLAlchemy/Alembic runners."""
    return _load_environment_file(
        OFFICIAL_ENV_FILE,
        required=required,
        example_name=".env.official-suites.example",
    )


def _authorized_official_suite_target(
    dburi: str | None = None,
):
    if not env_bool(
        "ALLOW_OFFICIAL_SUITE_DESTRUCTIVE_TESTS",
        False,
    ):
        raise RuntimeError(
            "ALLOW_OFFICIAL_SUITE_DESTRUCTIVE_TESTS debe ser true"
        )

    expected = ansi_database_name()
    forbidden = FORBIDDEN_DEFAULTS | split_names(
        os.getenv("FORBIDDEN_DATABASE_NAMES", "")
    )
    if expected.casefold() in forbidden:
        raise RuntimeError(
            f"La base ANSI esperada {expected!r} está prohibida"
        )
    if expected.casefold() == non_ansi_database_name().casefold():
        raise RuntimeError(
            "Las bases ANSI y no ANSI deben tener nombres diferentes."
        )

    parsed = make_url(dburi or official_suite_dburi())
    configured = (parsed.database or "").strip()
    if configured.casefold() != expected.casefold():
        raise RuntimeError(
            f"La URL de la suite apunta a {configured!r}, pero la base ANSI "
            f"autorizada es {expected!r}."
        )
    return expected, parsed


def resolve_junit_file(raw_path: str, default_name: str) -> Path:
    value = raw_path.strip()
    path = Path(value) if value else PROJECT_ROOT / "artifacts" / default_name
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


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
    default_owner = _clean_catalog_name(
        connection.exec_driver_sql(
            "SELECT USER FROM systables WHERE tabid = 1"
        ).scalar_one()
    )
    owners = _unique_owner_names(default_owner, *OFFICIAL_SUITE_OWNERS)
    inventory = {
        owner: {"tables": [], "views": [], "sequences": []}
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
        rows = connection.exec_driver_sql(sql_text, (owner,)).fetchall()
        for row in rows:
            name = _clean_catalog_name(row[0])
            tabtype = _clean_catalog_name(row[1])
            if name is None or tabtype is None:
                continue
            kind = CATALOG_KIND_BY_TABTYPE.get(tabtype.upper())
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


def verify_test_database(
    dburi: str,
    *,
    expected_database: str,
    expected_ansi: bool,
) -> dict[str, Any]:
    parsed = make_url(dburi)
    configured = (parsed.database or "").strip()
    if configured.casefold() != expected_database.casefold():
        raise RuntimeError(
            f"La URL apunta a {configured!r}, pero se esperaba "
            f"{expected_database!r}."
        )

    engine = create_engine(
        parsed,
        pool_pre_ping=True,
        connect_args={"timeout": 15},
    )
    try:
        with engine.connect() as connection:
            actual = str(
                connection.execute(
                    text(
                        "SELECT DBINFO('dbname') FROM systables "
                        "WHERE tabid = 1"
                    )
                ).scalar_one()
            ).strip()
            if actual.casefold() != expected_database.casefold():
                raise RuntimeError(
                    f"Informix conectó a {actual!r}, pero se esperaba "
                    f"{expected_database!r}."
                )
            state = _database_state(connection, actual)
            if state is None:
                raise RuntimeError(
                    f"sysmaster no contiene la base conectada {actual!r}."
                )
            if state.is_ansi != expected_ansi:
                expected_label = "ANSI" if expected_ansi else "no ANSI"
                raise RuntimeError(
                    f"La base {actual!r} no está en modo {expected_label}."
                )
            if not state.is_logging:
                raise RuntimeError(
                    f"La base {actual!r} no tiene logging activo."
                )
            return {
                "database": actual,
                "is_ansi_database": state.is_ansi,
                "is_logging": state.is_logging,
                "safe_url": parsed.render_as_string(hide_password=True),
            }
    finally:
        engine.dispose()


def verify_official_suite_database(
    dburi: str | None = None,
    *,
    require_empty: bool | None = None,
) -> dict[str, Any]:
    expected, parsed = _authorized_official_suite_target(dburi)
    if require_empty is None:
        require_empty = env_bool(
            "OFFICIAL_SUITE_REQUIRE_EMPTY",
            True,
        )

    engine = create_engine(
        parsed,
        pool_pre_ping=True,
        connect_args={"timeout": 15},
    )
    try:
        with engine.connect() as connection:
            actual = str(
                connection.execute(
                    text(
                        "SELECT DBINFO('dbname') FROM systables "
                        "WHERE tabid = 1"
                    )
                ).scalar_one()
            ).strip()
            if actual.casefold() != expected.casefold():
                raise RuntimeError(
                    f"Informix conectó a {actual!r}, pero se esperaba "
                    f"{expected!r}"
                )
            state = _database_state(connection, actual)
            if state is None or not state.is_ansi or not state.is_logging:
                raise RuntimeError(
                    f"La base exclusiva {actual!r} debe existir con "
                    "WITH LOG MODE ANSI."
                )
            inventory = _collect_official_suite_inventory(connection)

        dirty_inventory = _non_empty_inventory(inventory)
        if require_empty and dirty_inventory:
            raise RuntimeError(
                "La base exclusiva contiene objetos residuales de la suite "
                f"oficial. Inventario={dirty_inventory}"
            )

        default_owner = next(iter(inventory), None)
        default_objects = inventory.get(default_owner, {}) if default_owner else {}
        return {
            "database": actual,
            "is_ansi_database": True,
            "is_logging": True,
            "default_owner": default_owner,
            "owners": tuple(inventory),
            "inventory": inventory,
            "dirty_inventory": dirty_inventory,
            "has_objects": bool(dirty_inventory),
            "tables": list(default_objects.get("tables", [])),
            "views": list(default_objects.get("views", [])),
            "sequences": list(default_objects.get("sequences", [])),
            "require_empty": require_empty,
            "safe_url": parsed.render_as_string(hide_password=True),
        }
    finally:
        engine.dispose()
