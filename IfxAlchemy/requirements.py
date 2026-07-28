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


def _supports_isolated_owner_namespaces(config) -> bool:
    dialect = config.db.dialect

    return bool(
        getattr(
            dialect,
            "is_ansi_database",
            False,
        )
    )


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
    def foreign_key_constraint_option_reflection_ondelete(self):
        """Informix reflects ``ON DELETE CASCADE`` from SYSREFERENCES.

        ``SYSREFERENCES.delrule`` stores ``C`` for cascading deletes and
        ``R`` for both the default rule and restrict semantics. The reflector
        publishes ``CASCADE`` for ``C`` and omits ``R`` because the catalog
        cannot distinguish an explicit ``RESTRICT`` from the default.
        """

        return exclusions.open()

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

        return exclusions.only_if(
            _supports_isolated_owner_namespaces,
            (
                "Informix provides separate namespaces by "
                "owner only in databases created in ANSI mode. In non-"
                "ANSI databases, the simple names of tables, views, sequences, and "
                "synonyms must be unique throughout the database."
            ),
        )

    @property
    def cross_schema_fk_reflection(self):
        """Allow reflection of foreign keys between different owners."""

        return exclusions.only_if(
            _supports_isolated_owner_namespaces,
            (
                "The SQLAlchemy suite's cross-schema reflection requires "
                "an Informix ANSI-mode database with separate namespaces "
                "for each owner."
            ),
        )

    @property
    def schema_create_delete(self):
        """Informix schemas are authorization-based object ownership.

        The dialect does not promise portable CREATE SCHEMA / DROP SCHEMA
        lifecycle management.
        """

        return exclusions.closed()

    @property
    def table_ddl_if_exists(self):
        """Informix supports idempotent table DDL.

        ``CREATE TABLE IF NOT EXISTS`` and ``DROP TABLE IF EXISTS`` are
        native Informix syntax. SQLAlchemy's generic DDL compiler already
        renders both forms in the correct keyword order for this dialect.
        """

        return exclusions.open()


    @property
    def index_ddl_if_exists(self):
        """Informix supports idempotent index DDL.

        ``CREATE INDEX IF NOT EXISTS`` and ``DROP INDEX IF EXISTS`` are
        native Informix syntax. SQLAlchemy's generic DDL compiler already
        renders both forms in the required keyword order for this dialect.
        """

        return exclusions.open()


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
        """Informix CHECK constraint reflection is supported."""

        return exclusions.open()

    @property
    def inline_check_constraint_reflection(self):
        """Informix inline CHECK constraint reflection is supported."""

        return exclusions.open()

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
    def indexes_check_column_order(self):
        """Informix preserves composite-index key order in SYSINDEXES.

        The ``part1`` through ``part16`` catalog columns represent index
        components in key order.  The reflector consumes them sequentially
        and therefore returns ``column_names`` in the order declared by the
        original ``CREATE INDEX`` statement.
        """

        return exclusions.open()

    @property
    def reflects_pk_names(self):
        """Informix devuelve el nombre de la clave primaria."""

        return exclusions.open()

    @property
    def reflect_table_options(self):
        """Reflect native Informix table storage and locking metadata.

        ``SYSTABLES`` exposes the lock level, first and next extent sizes,
        and page size for base tables.  Views participate in the generic
        multi-reflection API and correctly return an empty option mapping.
        """

        return exclusions.open()

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
    def intersect(self):
        """Informix supports the native ``INTERSECT`` set operator."""

        return exclusions.open()

    @property
    def except_(self):
        """Informix supports the native ``EXCEPT`` set operator."""

        return exclusions.open()

    @property
    def ctes(self):
        """Informix supports non-recursive and recursive CTE statements.

        Informix uses the ``WITH`` preamble for both forms; the compiler
        intentionally omits the optional ``RECURSIVE`` keyword.
        """

        return exclusions.open()

    @property
    def ctes_with_update_delete(self):
        """Informix supports a CTE preceding UPDATE and DELETE.

        The dialect also protects affected ODBC/server combinations by
        rendering bind values inside a DML CTE as post-compile literals.
        Values outside the CTE remain ordinary DBAPI parameters.
        """

        return exclusions.open()

    @property
    def update_from(self):
        """Provide SQLAlchemy's multi-table UPDATE behavior on Informix.

        Informix does not need ``UPDATE .. FROM`` syntax for this contract;
        the compiler rewrites the additional FROM objects to a correlated
        ``EXISTS`` predicate while preserving the observable result.
        """

        return exclusions.open()

    @property
    def delete_from(self):
        """Provide SQLAlchemy's multi-table DELETE behavior on Informix.

        Additional FROM objects are rewritten to a correlated ``EXISTS``
        predicate instead of emitting unsupported ``DELETE .. USING`` SQL.
        """

        return exclusions.open()

    @property
    def boolean_col_expressions(self):
        """Boolean predicates can be selected as result columns.

        Informix receives a numeric ``CASE WHEN`` projection and SQLAlchemy
        retains Boolean result processing for the returned value.
        """

        return exclusions.open()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        """Support ordered or limited SELECT branches inside a UNION.

        Informix rejects the generic parenthesized branch syntax. The
        compiler preserves branch-local ordering and row limits through a
        derived-table SELECT, which satisfies the SQLAlchemy behavior.
        """

        return exclusions.open()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        """Support ordered SELECT branches inside a UNION.

        The compiler uses a derived table instead of an unsupported directly
        parenthesized SELECT branch.
        """

        return exclusions.open()

    @property
    def order_by_label_with_expression(self):
        """Allow projected labels to participate in ORDER BY expressions."""

        return exclusions.open()

    @property
    def fetch_first(self):
        """Implement SQLAlchemy FETCH through Informix FIRST/ROW_NUMBER."""

        return exclusions.open()

    @property
    def fetch_no_order_by(self):
        """Informix FIRST can limit a result without an ORDER BY clause."""

        return exclusions.open()

    @property
    def fetch_expression(self):
        """Support expression-valued FETCH and OFFSET through ROW_NUMBER."""

        return exclusions.open()

    @property
    def sql_expression_limit_offset(self):
        """FIRST/SKIP requieren valores enteros compatibles, no expresiones."""

        return exclusions.closed()

    @property
    def group_by_complex_expression(self):
        """Support composed GROUP BY expressions through projected aliases.

        Informix rejects some arithmetic expressions when repeated directly
        in ``GROUP BY``.  The compiler renders a projected SQLAlchemy label by
        alias in ``GROUP BY`` while preserving ordinary expression rendering
        for labels that are not part of the SELECT list.
        """

        return exclusions.open()

    @property
    def empty_inserts(self):
        """Support a singleton empty INSERT for SERIAL-backed tables.

        Informix has no portable ``DEFAULT VALUES`` form for this dialect.
        The compiler instead emits an explicit zero for the table's
        autoincrement SERIAL/SERIAL8/BIGSERIAL column, which instructs the
        server to generate the next serial value.  Executemany remains a
        separate capability and is intentionally not opened here.
        """

        return exclusions.open()

    @property
    def empty_inserts_executemany(self):
        """Support executemany with empty parameter dictionaries.

        The singleton empty-insert rewrite produces one reusable statement
        that binds zero to the SERIAL/SERIAL8/BIGSERIAL autoincrement
        column.  Each empty parameter dictionary therefore requests a new
        server-generated serial value while preserving normal executemany
        behavior.
        """

        return exclusions.open()

    @property
    def insert_from_select(self):
        """Informix supports ``INSERT INTO ... SELECT ...`` statements.

        The target column list can omit a SERIAL/BIGSERIAL column so that
        Informix generates its value. SQLAlchemy also expands Python and SQL
        column defaults into the target and SELECT lists for this construct.
        """

        return exclusions.open()

    @property
    def dbapi_lastrowid(self):
        """Expose generated serial values through ``CursorResult.lastrowid``.

        The IBM Informix ODBC cursor does not provide a portable native
        ``cursor.lastrowid`` attribute. The dialect implements the public
        SQLAlchemy behavior in its execution context by querying
        ``DBINFO('sqlca.sqlerrd1')`` immediately after a singleton INSERT.
        SQLAlchemy's compliance suite guards that observable behavior with
        the ``dbapi_lastrowid`` requirement.
        """

        return exclusions.open()

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
        """Informix preserves large and small scientific-notation decimals.

        SQLAlchemy 2.0.51 uses this requirement for both its very-small
        ``DECIMAL(18, 14)`` cases and its very-large ``DECIMAL(25, 2)``
        cases.  Both precisions are below Informix's 32-digit DECIMAL
        limit, and the pyodbc dialect returns them as ``Decimal`` values.
        """

        return exclusions.open()

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

    @property
    def server_defaults(self):
        """Informix supports literal and temporal column server defaults."""

        return exclusions.open()

    @property
    def expression_server_defaults(self):
        """Informix DEFAULT does not accept arbitrary arithmetic expressions."""

        return exclusions.closed()
