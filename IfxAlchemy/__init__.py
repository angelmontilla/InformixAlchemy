# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2008-2019 IBM Corporation
# Copyright (c) 2026 Angel Montilla
#
# Originally derived from IfxAlchemy / OpenInformix.
# Modified by Angel Montilla to adapt IfxAlchemy to SQLAlchemy 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from importlib.metadata import PackageNotFoundError, version as distribution_version
from pathlib import Path
import sys

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def _source_tree_version() -> str | None:
    """Return the adjacent project version when imported from a source checkout.

    A developer can have an older IfxAlchemy distribution installed while importing
    the package directly from a newer checkout.  In that situation distribution
    metadata belongs to the installed copy, not to the source code being executed.
    """
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    try:
        with pyproject.open("rb") as file:
            return str(tomllib.load(file)["project"]["version"])
    except (FileNotFoundError, KeyError, OSError, tomllib.TOMLDecodeError):
        return None


def _runtime_version() -> str:
    """Resolve the version from the active source tree or installed metadata."""
    source_version = _source_tree_version()
    if source_version is not None:
        return source_version

    try:
        return distribution_version("IfxAlchemy")
    except PackageNotFoundError:
        return "0+unknown"


__version__ = _runtime_version()

# Imports of the modules required for the pyodbc dialect and data types
from . import pyodbc, base

# Alembic is optional.  When it is installed, importing this package must
# register the Informix DefaultImpl before MigrationContext.configure() is
# called.  This prevents Alembic from failing with KeyError("informix").
try:
    from . import alembic as _alembic  # noqa: F401
except ModuleNotFoundError as error:
    # Do not make Alembic a mandatory runtime dependency.  Only suppress a
    # missing Alembic package; errors in this package's own integration must
    # remain visible.
    if not error.name or error.name.split(".", 1)[0] != "alembic":
        raise

# Default dialect: pyodbc
dialect = pyodbc.IfxDialect_pyodbc

from .complex import (
    CreateDistinctType,
    CreateRowType,
    DISTINCT,
    DropDistinctType,
    DropRowType,
    LIST,
    MULTISET,
    ROW,
    RowField,
    RowValue,
    SET,
    parse_complex_value,
)



from .indexes import (
    AlterIndexCluster,
    DisableIndex,
    EnableIndex,
    SetIndexMode,
    SetIndexVisibility,
)

from .optimizer import (
    AllRows,
    AvoidIndex,
    FirstRows,
    JoinOrder,
    OptimizerDirective,
    UseIndex,
)

# Informix-specific executable DML constructs
from .dml import InformixMerge, merge
from .document import bson_get, bson_size, bson_update, gen_bson
from .temporal import YearMonthInterval

# Typed Informix fragmentation models and ALTER FRAGMENT constructs
from .fragmentation import (
    AddFragment,
    AttachFragment,
    AttachedIndexFragmentation,
    DetachFragment,
    DropFragment,
    ExpressionFragmentation,
    Fragment,
    InitFragment,
    InitFragmentation,
    ListFragmentation,
    ModifyFragment,
    RangeFragmentation,
    RangeIntervalFragmentation,
    RoundRobinFragmentation,
)

# Informix-specific executable DDL constructs
from .ddl import (
    CreateSynonym,
    DropSynonym,
    ModifyTableExtents,
    SetTableLockMode,
    SynonymName,
    SynonymTarget,
)

# Data types supported by the Informix dialect
from .base import (
    BIGINT,
    BIGSERIAL,
    BLOB,
    BOOLEAN,
    BSON,
    CHAR,
    CLOB,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    GRAPHIC,
    INTEGER,
    INTERVAL,
    JSON,
    LONGVARCHAR,
    LVARCHAR,
    NUMERIC,
    SMALLINT,
    REAL,
    SERIAL,
    SERIAL8,
    TIME,
    TIMESTAMP,
    VARCHAR,
    VARGRAPHIC,
)

# List of public elements exported by this module
__all__ = (
    "BIGINT",
    "BIGSERIAL",
    "BLOB",
    "BOOLEAN",
    "BSON",
    "CHAR",
    "CLOB",
    "CreateSynonym",
    "AddFragment",
    "AttachFragment",
    "AttachedIndexFragmentation",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DOUBLE",
    "DropSynonym",
    "DetachFragment",
    "DropFragment",
    "ExpressionFragmentation",
    "Fragment",
    "GRAPHIC",
    "INTEGER",
    "INTERVAL",
    "InformixMerge",
    "JSON",
    "InitFragment",
    "InitFragmentation",
    "ListFragmentation",
    "LONGVARCHAR",
    "LVARCHAR",
    "NUMERIC",
    "ModifyTableExtents",
    "ModifyFragment",
    "SMALLINT",
    "REAL",
    "RangeFragmentation",
    "RangeIntervalFragmentation",
    "RoundRobinFragmentation",
    "SERIAL",
    "SERIAL8",
    "SetTableLockMode",
    "SynonymName",
    "SynonymTarget",
    "TIME",
    "TIMESTAMP",
    "VARCHAR",
    "VARGRAPHIC",
    "bson_get",
    "bson_size",
    "bson_update",
    "CreateDistinctType",
    "CreateRowType",
    "DISTINCT",
    "DropDistinctType",
    "DropRowType",
    "LIST",
    "MULTISET",
    "ROW",
    "RowField",
    "RowValue",
    "SET",
    "AllRows",
    "AvoidIndex",
    "FirstRows",
    "JoinOrder",
    "OptimizerDirective",
    "UseIndex",
    "YearMonthInterval",
    "AlterIndexCluster",
    "DisableIndex",
    "EnableIndex",
    "SetIndexMode",
    "SetIndexVisibility",
    "dialect",
    "gen_bson",
    "merge",
    "parse_complex_value",
)
