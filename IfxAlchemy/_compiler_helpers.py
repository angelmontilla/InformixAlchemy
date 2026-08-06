"""Small, side-effect-free helpers used by the Informix SQL compiler.

The functions in this module are private implementation details. Keeping them
separate from :mod:`IfxAlchemy.base` makes the default-expression rules easier
to test without importing the complete dialect compiler implementation.
"""
from __future__ import annotations

import re

_IFX_SIGNED_NUMERIC_DEFAULT_RE = re.compile(
    r"^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE][+-]?\d+)?$"
)
_IFX_QUOTED_STRING_DEFAULT_RE = re.compile(
    r"^'(?:''|[^'])*'$",
    re.DOTALL,
)
_IFX_ISO_TEMPORAL_DEFAULT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"
    r"(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?$"
)


def strip_outer_parentheses(value: object) -> str:
    """Remove balanced parentheses that enclose the complete SQL value."""
    text = str(value).strip()

    while len(text) >= 2 and text[0] == "(" and text[-1] == ")":
        depth = 0
        in_string = False
        encloses_whole_value = True
        index = 0

        while index < len(text):
            character = text[index]

            if in_string:
                if character == "'":
                    if index + 1 < len(text) and text[index + 1] == "'":
                        index += 2
                        continue
                    in_string = False
                index += 1
                continue

            if character == "'":
                in_string = True
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0 and index != len(text) - 1:
                    encloses_whole_value = False
                    break
            index += 1

        if not encloses_whole_value or depth != 0 or in_string:
            break

        text = text[1:-1].strip()

    return text


def _is_scalar_default_literal(text: str) -> bool:
    return any(
        pattern.fullmatch(text)
        for pattern in (
            _IFX_SIGNED_NUMERIC_DEFAULT_RE,
            _IFX_QUOTED_STRING_DEFAULT_RE,
            _IFX_ISO_TEMPORAL_DEFAULT_RE,
        )
    )


def _mask_quoted_literals(text: str) -> str:
    characters = list(text)
    in_string = False
    index = 0

    while index < len(characters):
        character = characters[index]
        if character != "'":
            if in_string:
                characters[index] = " "
            index += 1
            continue

        characters[index] = " "
        if in_string and index + 1 < len(characters):
            if characters[index + 1] == "'":
                characters[index + 1] = " "
                index += 2
                continue

        in_string = not in_string
        index += 1

    return "".join(characters)


def _nearest_non_space_character(text: str, start: int, step: int) -> str | None:
    index = start
    while 0 <= index < len(text) and text[index].isspace():
        index += step
    return text[index] if 0 <= index < len(text) else None


def _is_non_binary_sign(text: str, index: int) -> bool:
    previous_character = _nearest_non_space_character(text, index - 1, -1)
    next_character = _nearest_non_space_character(text, index + 1, 1)
    return (
        previous_character is None
        or previous_character in "(,"
        or (
            previous_character in {"e", "E"}
            and next_character is not None
            and next_character.isdigit()
        )
    )


def contains_arithmetic_default(default_sql: object) -> bool:
    """Return whether SQL contains a binary arithmetic operator outside strings."""
    text = strip_outer_parentheses(default_sql)
    if not text or _is_scalar_default_literal(text):
        return False

    unquoted_text = _mask_quoted_literals(text)
    if any(operator in unquoted_text for operator in "*/%"):
        return True

    return any(
        character in "+-" and not _is_non_binary_sign(unquoted_text, index)
        for index, character in enumerate(unquoted_text)
    )


def normalize_boolean_default(value: object) -> str | None:
    """Return SQLAlchemy's canonical spelling for an Informix BOOLEAN default."""
    candidate = strip_outer_parentheses(value)
    if (
        len(candidate) >= 2
        and candidate[0] == candidate[-1]
        and candidate[0] in {"'", '"'}
    ):
        candidate = candidate[1:-1].strip()

    normalized = candidate.casefold()
    if normalized in {"t", "true", "1"}:
        return "true"
    if normalized in {"f", "false", "0"}:
        return "false"
    return None
