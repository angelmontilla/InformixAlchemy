from __future__ import annotations

import datetime
from typing import Any

from sqlalchemy import cast
from sqlalchemy import types as sa_types


IFX_MAX_FRACTION_DIGITS = 5
PYTHON_MICROSECOND_DIGITS = 6


def validate_fraction_digits(fraction_digits: int) -> int:
    """Validate an Informix DATETIME fractional precision.

    Informix accepts FRACTION(1) through FRACTION(5). A value of zero
    means that the type ends at SECOND and stores no fractional part.
    """

    if isinstance(fraction_digits, bool):
        raise TypeError("fraction_digits must be an integer from 0 to 5")

    if not isinstance(fraction_digits, int):
        raise TypeError("fraction_digits must be an integer from 0 to 5")

    if not 0 <= fraction_digits <= IFX_MAX_FRACTION_DIGITS:
        raise ValueError("fraction_digits must be between 0 and 5")

    return fraction_digits


def microsecond_quantum(fraction_digits: int) -> int:
    """Return the smallest representable unit in Python microseconds.

    Examples:

        FRACTION(5) -> 10 microseconds
        FRACTION(3) -> 1,000 microseconds
        no fraction -> 1,000,000 microseconds
    """

    fraction_digits = validate_fraction_digits(fraction_digits)

    return 10 ** (PYTHON_MICROSECOND_DIGITS - fraction_digits)


def quantize_temporal_value(
    value: datetime.time | datetime.datetime,
    fraction_digits: int,
) -> datetime.time | datetime.datetime:
    """Truncate a Python temporal value to Informix precision.

    The operation is idempotent:

        quantize(quantize(value)) == quantize(value)

    Truncation is intentional. It prevents a value near the end of a
    second from being rounded into the following second, minute or day.
    """

    quantum = microsecond_quantum(fraction_digits)

    quantized_microsecond = (
        value.microsecond // quantum
    ) * quantum

    if quantized_microsecond == value.microsecond:
        return value

    return value.replace(microsecond=quantized_microsecond)


def _fraction_text(
    microsecond: int,
    fraction_digits: int,
) -> str:
    """Convert Python microseconds to an Informix fraction string."""

    if fraction_digits == 0:
        return ""

    divisor = microsecond_quantum(fraction_digits)
    fraction_value = microsecond // divisor

    return f".{fraction_value:0{fraction_digits}d}"


def format_ifx_time(
    value: datetime.time,
    fraction_digits: int,
) -> str:
    """Return a canonical Informix time-of-day representation."""

    normalized = quantize_temporal_value(
        value,
        fraction_digits,
    )

    return (
        f"{normalized.hour:02d}:"
        f"{normalized.minute:02d}:"
        f"{normalized.second:02d}"
        f"{_fraction_text(normalized.microsecond, fraction_digits)}"
    )


def format_ifx_datetime(
    value: datetime.datetime,
    fraction_digits: int,
) -> str:
    """Return a canonical Informix date-and-time representation."""

    normalized = quantize_temporal_value(
        value,
        fraction_digits,
    )

    return (
        f"{normalized.year:04d}-"
        f"{normalized.month:02d}-"
        f"{normalized.day:02d} "
        f"{normalized.hour:02d}:"
        f"{normalized.minute:02d}:"
        f"{normalized.second:02d}"
        f"{_fraction_text(normalized.microsecond, fraction_digits)}"
    )


def _decode_ascii_temporal(value: bytes) -> str:
    try:
        return value.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Informix returned a non-ASCII temporal value"
        ) from exc


class IFXTime(sa_types.Time):
    """Informix time-of-day with deterministic fractional precision.

    Python can represent six microsecond digits. Informix supports at
    most five fractional digits. Values are truncated before binding so
    that precision loss is controlled by the dialect rather than by the
    server or ODBC driver.
    """

    cache_ok = True

    def __init__(
        self,
        timezone: bool = False,
        fraction_digits: int = IFX_MAX_FRACTION_DIGITS,
    ) -> None:
        if timezone:
            raise NotImplementedError(
                "Informix DATETIME does not preserve timezone information"
            )

        super().__init__(timezone=False)

        self.fraction_digits = validate_fraction_digits(
            fraction_digits
        )

    def bind_expression(self, bindvalue):
        """Give Informix an explicit type for every bound value.

        The bind processor emits canonical text. The CAST prevents
        ambiguous parameter typing in expressions such as SELECT ?.
        """

        return cast(bindvalue, self)

    def bind_processor(self, dialect):
        fraction_digits = self.fraction_digits

        def process(value: Any):
            if value is None:
                return None

            if not isinstance(value, datetime.time):
                raise TypeError(
                    "Informix time values require datetime.time; "
                    f"received {type(value).__name__}"
                )

            if value.utcoffset() is not None:
                raise ValueError(
                    "Timezone-aware datetime.time values are not "
                    "supported by Informix DATETIME"
                )

            return format_ifx_time(
                value,
                fraction_digits,
            )

        return process

    def result_processor(self, dialect, coltype):
        fraction_digits = self.fraction_digits

        def process(value: Any):
            if value is None:
                return None

            if isinstance(value, datetime.datetime):
                temporal_value = value.time()

            elif isinstance(value, datetime.time):
                temporal_value = value

            elif isinstance(value, bytes):
                temporal_value = datetime.time.fromisoformat(
                    _decode_ascii_temporal(value).strip()
                )

            elif isinstance(value, str):
                temporal_value = datetime.time.fromisoformat(
                    value.strip()
                )

            else:
                raise TypeError(
                    "Informix returned an incompatible time value: "
                    f"{value!r} ({type(value).__name__})"
                )

            return quantize_temporal_value(
                temporal_value,
                fraction_digits,
            )

        return process


class IFXDateTime(sa_types.DateTime):
    """Informix date-time with deterministic fractional precision."""

    cache_ok = True

    def __init__(
        self,
        timezone: bool = False,
        fraction_digits: int = IFX_MAX_FRACTION_DIGITS,
    ) -> None:
        if timezone:
            raise NotImplementedError(
                "Informix DATETIME does not preserve timezone information"
            )

        super().__init__(timezone=False)

        self.fraction_digits = validate_fraction_digits(
            fraction_digits
        )

    def bind_expression(self, bindvalue):
        return cast(bindvalue, self)

    def bind_processor(self, dialect):
        fraction_digits = self.fraction_digits

        def process(value: Any):
            if value is None:
                return None

            if not isinstance(value, datetime.datetime):
                raise TypeError(
                    "Informix datetime values require datetime.datetime; "
                    f"received {type(value).__name__}"
                )

            if value.utcoffset() is not None:
                raise ValueError(
                    "Timezone-aware datetime.datetime values are not "
                    "supported by Informix DATETIME"
                )

            return format_ifx_datetime(
                value,
                fraction_digits,
            )

        return process

    def result_processor(self, dialect, coltype):
        fraction_digits = self.fraction_digits

        def process(value: Any):
            if value is None:
                return None

            if isinstance(value, datetime.datetime):
                temporal_value = value

            elif isinstance(value, bytes):
                temporal_value = datetime.datetime.fromisoformat(
                    _decode_ascii_temporal(value).strip()
                )

            elif isinstance(value, str):
                temporal_value = datetime.datetime.fromisoformat(
                    value.strip()
                )

            else:
                raise TypeError(
                    "Informix returned an incompatible datetime value: "
                    f"{value!r} ({type(value).__name__})"
                )

            return quantize_temporal_value(
                temporal_value,
                fraction_digits,
            )

        return process
