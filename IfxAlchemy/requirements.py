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

"""requirements.py

Suite capability flags for the Informix fork targeting modern
SQLAlchemy 2.0.x compatibility.

This module tells the SQLAlchemy test/provisioning helpers which
optional behaviors are currently supported, unsupported, or
intentionally out of scope for this dialect.

"""
from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):

    @property
    def has_temp_table(self):
        """target dialect supports checking a single temp table name"""

        return exclusions.open()

    @property
    def temp_table_names(self):
        """Informix ODBC does not expose a reliable connection-local
        listing API for temporary tables in this dialect.
        """

        return exclusions.closed()

    @property
    def temp_table_reflection(self):
        """The dialect only guarantees has_table() for connection-local temp
        tables; full SQLAlchemy temp-table reflection scenarios are not
        supported on the current Informix backend.
        """

        return exclusions.closed()

    @property
    def temporary_views(self):
        """Temporary views are outside this dialect's Informix contract."""

        return exclusions.closed()

    @property
    def on_update_cascade(self):
        """Informix foreign keys do not support ON UPDATE CASCADE."""

        return exclusions.closed()

    # Informix can store fractional DATETIME qualifiers, but the current
    # generic SQLAlchemy DateTime mapping intentionally exposes second
    # precision only. Informix-specific qualifier metadata is preserved on
    # reflected types.
    @property
    def time_microseconds(self):
        """Informix preserves at most five fractional digits.

        Python datetime.time supports six microsecond digits. The dialect
        offers deterministic FRACTION(5) truncation, but it cannot promise
        exact six-digit microsecond round trips.
        """

        return exclusions.closed()

    @property
    def datetime_microseconds(self):
        """Informix preserves at most five fractional digits.

        Python datetime.datetime supports six microsecond digits. The
        dialect offers deterministic FRACTION(5) truncation, but it cannot
        promise exact six-digit microsecond round trips.
        """

        return exclusions.closed()

    @property
    def unbounded_varchar(self):
        """Informix VARCHAR requires an explicit length in DDL."""

        return exclusions.closed()

    @property
    def schemas(self):
        """SQLAlchemy schema-qualified ownership is outside this contract."""

        return exclusions.closed()

    @property
    def temporary_tables(self):
        """Informix supports known connection-local temporary tables."""

        return exclusions.open()

    @property
    def views(self):
        """Informix views are reflected by the supported pyodbc dialect."""

        return exclusions.open()

    @property
    def materialized_views(self):
        """Informix materialized views are outside this dialect contract."""

        return exclusions.closed()

    @property
    def check_constraint_reflection(self):
        """Informix CHECK constraint reflection is not implemented yet."""

        return exclusions.closed()

    @property
    def inline_check_constraint_reflection(self):
        """Informix inline CHECK constraint reflection is not implemented."""

        return exclusions.closed()

    @property
    def table_reflection(self):
        """Basic table reflection is part of the supported contract."""

        return exclusions.open()

    @property
    def foreign_key_constraint_reflection(self):
        """Foreign-key reflection is part of the supported contract."""

        return exclusions.open()

    @property
    def unique_constraint_reflection(self):
        """Unique-constraint reflection is part of the supported contract."""

        return exclusions.open()

    @property
    def index_reflection(self):
        """Index reflection is part of the supported contract."""

        return exclusions.open()

    @property
    def reflects_pk_names(self):
        """Informix devuelve el nombre de la clave primaria."""

        return exclusions.open()

    @property
    def reflect_table_options(self):
        """La reflexión de opciones físicas de tabla no está implementada."""

        return exclusions.closed()

    @property
    def unicode_data(self):
        """El contrato actual no garantiza Unicode arbitrario extremo a extremo."""

        return exclusions.closed()

    @property
    def unicode_data_no_special_types(self):
        """VARCHAR/TEXT no garantizan todos los caracteres Unicode del test."""

        return exclusions.closed()

    @property
    def time(self):
        """Time se representa como DATETIME HOUR TO SECOND."""

        return exclusions.open()

    @property
    def time_implicit_bound(self):
        """Un parámetro TIME aislado carece de contexto de tipo fiable en ODBC."""

        return exclusions.closed()

    @property
    def date_implicit_bound(self):
        """Un parámetro DATE aislado no se tipa de forma fiable."""

        return exclusions.closed()

    @property
    def datetime_implicit_bound(self):
        """Un parámetro DATETIME aislado no se tipa de forma fiable."""

        return exclusions.closed()

    @property
    def standalone_null_binds_whereclause(self):
        """Un NULL sin columna asociada no tiene tipo ODBC determinable."""

        return exclusions.closed()

    @property
    def implicit_decimal_binds(self):
        """DECIMAL seleccionado como parámetro aislado no está garantizado."""

        return exclusions.closed()

    @property
    def literal_float_coercion(self):
        """FLOAT seleccionado como parámetro aislado no está garantizado."""

        return exclusions.closed()

    @property
    def expressions_against_unbounded_text(self):
        """Informix TEXT es un LOB y no admite comparaciones ordinarias."""

        return exclusions.closed()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        """Informix no admite el SQL parentetizado generado en estas ramas."""

        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        """No se garantiza SELECT parentetizado dentro de UNION."""

        return exclusions.closed()

    @property
    def sql_expression_limit_offset(self):
        """FIRST/SKIP requieren valores enteros compatibles, no expresiones."""

        return exclusions.closed()

    @property
    def group_by_complex_expression(self):
        """No se garantiza GROUP BY sobre expresiones compuestas."""

        return exclusions.closed()

    @property
    def insert_from_select(self):
        """No se garantiza INSERT FROM SELECT con defaults SQLAlchemy."""

        return exclusions.closed()

    @property
    def window_functions(self):
        """Target database must support window functions."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        """target backend supports Decimal() objects using E notation
        to represent very small values."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values."""
        return exclusions.closed()

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """
        return exclusions.fails_if(lambda: True, "Current Informix test backend rejects DECIMAL(38, 12)")

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return exclusions.open()
