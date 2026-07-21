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
    Requisitos usados exclusivamente por la
    suite externa de Alembic.

    InformixRequirements conserva las capacidades
    y limitaciones del dialecto.

    AlembicSuiteRequirements añade propiedades
    que no existen en la suite normal de
    SQLAlchemy, por ejemplo:

    - comments
    - alter_column
    - computed_columns
    - identity_columns
    - foreign_key_name_reflection
    - opciones FK propias de Alembic
    """

    @property
    def fk_onupdate(self):
        """
        Informix no soporta acciones referenciales
        ON UPDATE en el contrato actual.
        """
        return exclusions.closed()
