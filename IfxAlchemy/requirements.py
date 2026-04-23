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
SQLAlchemy 2.x compatibility.

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
        """target dialect supports listing of temporary table names"""

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
        """target database supports temporary views"""

        return exclusions.closed()

    @property
    def on_update_cascade(self):
        """"target database must support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return exclusions.closed()

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return exclusions.closed()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()

    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        return exclusions.closed()

    #@property
    #def offset(self):
    #    return exclusions.closed()

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
        return exclusions.fails_if(lambda: True,
                    "Throws error SQL0604N, regarding Decimal(38, 12)"
            )

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return exclusions.open()
