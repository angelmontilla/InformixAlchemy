from __future__ import annotations

from alembic.testing.requirements import (
    SuiteRequirements
    as AlembicSuiteRequirements,
)
from sqlalchemy.testing import exclusions

from IfxAlchemy.requirements import (
    Requirements as InformixRequirements,
)


class Requirements(
    InformixRequirements,
    AlembicSuiteRequirements,
):
    """
    Requirements used exclusively by the external
    Alembic suite.

    InformixRequirements preserves the dialect's
    capabilities and limitations.

    AlembicSuiteRequirements adds properties
    that do not exist in the regular SQLAlchemy
    suite, for example:

    - comments
    - alter_column
    - computed_columns
    - identity_columns
    - foreign_key_name_reflection
    - Alembic-specific foreign key options
    """

    @property
    def fk_onupdate(self):
        """
        Informix does not support ON UPDATE referential
        actions under the current contract.
        """
        return exclusions.closed()

    @property
    def fk_ondelete_restrict(self):
        """Informix foreign keys expose CASCADE or the default delete rule.

        ``SYSREFERENCES.delrule`` cannot distinguish an explicit RESTRICT
        clause from the default rule, and the dialect intentionally accepts
        only the portable Informix ``ON DELETE CASCADE`` extension.
        """

        return exclusions.closed()

    @property
    def fk_ondelete_noaction(self):
        """Informix does not compile an explicit ON DELETE NO ACTION clause."""

        return exclusions.closed()

    @property
    def fk_onupdate_restrict(self):
        """Informix has no ON UPDATE referential-action syntax."""

        return exclusions.closed()
