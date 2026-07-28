from unittest import mock

import pytest
from sqlalchemy.engine.reflection import ObjectKind
from sqlalchemy.engine.reflection import ObjectScope

from IfxAlchemy.reflection import IfxReflector


class _FakeIdentifierPreparer:
    def _requires_quotes(self, value):
        return False


class _FakeDialect:
    ischema_names = {}
    identifier_preparer = _FakeIdentifierPreparer()
    default_schema_name = "informix"


MULTI_METHODS = (
    "get_multi_columns",
    "get_multi_pk_constraint",
    "get_multi_foreign_keys",
    "get_multi_indexes",
    "get_multi_unique_constraints",
)

MULTI_SCOPES = (
    ObjectScope.DEFAULT,
    ObjectScope.TEMPORARY,
    ObjectScope.ANY,
)

MULTI_KINDS = (
    ObjectKind.TABLE,
    ObjectKind.VIEW,
    ObjectKind.MATERIALIZED_VIEW,
    ObjectKind.ANY,
    ObjectKind.ANY_VIEW,
    ObjectKind.TABLE | ObjectKind.VIEW,
    ObjectKind.TABLE | ObjectKind.MATERIALIZED_VIEW,
)


@pytest.mark.parametrize(
    "method_name",
    MULTI_METHODS,
)
@pytest.mark.parametrize(
    "use_filter",
    (
        False,
        True,
    ),
)
@pytest.mark.parametrize(
    "scope",
    MULTI_SCOPES,
)
@pytest.mark.parametrize(
    "kind",
    MULTI_KINDS,
)
def test_multi_reflection_propagates_schema(
    method_name,
    use_filter,
    scope,
    kind,
):
    """5 × 2 × 3 × 7 = 210 combinaciones."""

    reflector = IfxReflector(
        _FakeDialect()
    )

    schema = "test_schema"

    filter_names = (
        ["users"]
        if use_filter
        else None
    )

    expected = [
        (
            (
                schema,
                "users",
            ),
            [],
        )
    ]

    with mock.patch.object(
        reflector,
        "_multi_reflect",
        return_value=iter(expected),
    ) as multi_reflect:
        result = list(
            getattr(
                reflector,
                method_name,
            )(
                object(),
                schema=schema,
                filter_names=filter_names,
                scope=scope,
                kind=kind,
            )
        )

    assert result == expected

    call = multi_reflect.call_args

    assert call.kwargs["schema"] == schema
    assert call.kwargs["filter_names"] == filter_names
    assert call.kwargs["scope"] == scope
    assert call.kwargs["kind"] == kind
