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

__version__ = "1.1.0"

# Imports of the modules required for the pyodbc dialect and data types
from . import pyodbc, base

# Default dialect: pyodbc
dialect = pyodbc.IfxDialect_pyodbc

# Informix-specific executable DDL constructs
from .ddl import ModifyTableExtents, SetTableLockMode

# Data types supported by the Informix dialect
from .base import (
    BIGINT,
    BIGSERIAL,
    BLOB,
    BOOLEAN,
    CHAR,
    CLOB,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    GRAPHIC,
    INTEGER,
    LONGVARCHAR,
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
    "CHAR",
    "CLOB",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DOUBLE",
    "GRAPHIC",
    "INTEGER",
    "LONGVARCHAR",
    "NUMERIC",
    "ModifyTableExtents",
    "SMALLINT",
    "REAL",
    "SERIAL",
    "SERIAL8",
    "SetTableLockMode",
    "TIME",
    "TIMESTAMP",
    "VARCHAR",
    "VARGRAPHIC",
    "dialect",
)
