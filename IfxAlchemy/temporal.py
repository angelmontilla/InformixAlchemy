from __future__ import annotations

import datetime
import math
import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import cast
from sqlalchemy import types as sa_types


IFX_MAX_FRACTION_DIGITS = 5
PYTHON_MICROSECOND_DIGITS = 6

_INTERVAL_YEAR_MONTH_FIELDS = ("YEAR", "MONTH")
_INTERVAL_DAY_TIME_FIELDS = (
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "FRACTION",
)
_INTERVAL_FIELD_CODES = {
    0: "YEAR",
    2: "MONTH",
    4: "DAY",
    6: "HOUR",
    8: "MINUTE",
    10: "SECOND",
    11: "FRACTION",
    12: "FRACTION",
    13: "FRACTION",
    14: "FRACTION",
    15: "FRACTION",
}
_INTERVAL_SUBFIELD_LIMITS = {
    "MONTH": 11,
    "HOUR": 23,
    "MINUTE": 59,
    "SECOND": 59,
}
_INTERVAL_UNIT_MICROSECONDS = {
    "DAY": 86_400_000_000,
    "HOUR": 3_600_000_000,
    "MINUTE": 60_000_000,
    "SECOND": 1_000_000,
    "FRACTION": 1,
}


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
    """Return the smallest representable unit in Python microseconds."""

    fraction_digits = validate_fraction_digits(fraction_digits)
    return 10 ** (PYTHON_MICROSECOND_DIGITS - fraction_digits)


def quantize_temporal_value(
    value: datetime.time | datetime.datetime,
    fraction_digits: int,
) -> datetime.time | datetime.datetime:
    """Truncate a Python temporal value to Informix precision."""

    quantum = microsecond_quantum(fraction_digits)
    quantized_microsecond = (value.microsecond // quantum) * quantum

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

    normalized = quantize_temporal_value(value, fraction_digits)
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

    normalized = quantize_temporal_value(value, fraction_digits)
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


def _validate_interval_precision(
    value: int | None,
    *,
    name: str,
    minimum: int,
    maximum: int,
) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer or None")
    if not minimum <= value <= maximum:
        raise ValueError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return value


def _normalize_interval_field(value: str, *, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip().upper()
    if normalized not in {
        *_INTERVAL_YEAR_MONTH_FIELDS,
        *_INTERVAL_DAY_TIME_FIELDS,
    }:
        raise ValueError(f"Unsupported Informix INTERVAL field: {value!r}")
    return normalized


def interval_field_from_catalog_code(code: int) -> str:
    try:
        return _INTERVAL_FIELD_CODES[int(code)]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            f"Unsupported Informix INTERVAL qualifier code: {code!r}"
        ) from exc


def _interval_family(start_field: str, end_field: str) -> str:
    if (
        start_field in _INTERVAL_YEAR_MONTH_FIELDS
        and end_field in _INTERVAL_YEAR_MONTH_FIELDS
    ):
        fields = _INTERVAL_YEAR_MONTH_FIELDS
        family = "year-month"
    elif (
        start_field in _INTERVAL_DAY_TIME_FIELDS
        and end_field in _INTERVAL_DAY_TIME_FIELDS
    ):
        fields = _INTERVAL_DAY_TIME_FIELDS
        family = "day-time"
    else:
        raise ValueError(
            "Informix INTERVAL cannot mix year-month and day-time fields"
        )

    if fields.index(start_field) > fields.index(end_field):
        raise ValueError(
            "Informix INTERVAL start_field must not be smaller than end_field"
        )
    return family


def default_interval_leading_precision(start_field: str) -> int:
    return 4 if start_field == "YEAR" else 2


def interval_fractional_precision_from_catalog_code(code: int) -> int | None:
    code = int(code)
    return code - 10 if 11 <= code <= 15 else None


def _interval_suffix_character_count(
    start_field: str,
    end_field: str,
    fractional_precision: int,
) -> int:
    """Return characters after the leading field in canonical ODBC text."""

    family = _interval_family(start_field, end_field)
    if family == "year-month":
        return 0 if start_field == end_field else 3

    fields = _INTERVAL_DAY_TIME_FIELDS
    start_index = fields.index(start_field)
    end_index = fields.index(end_field)
    suffix = 0
    for index in range(start_index + 1, end_index + 1):
        field = fields[index]
        if field == "HOUR" and start_field == "DAY":
            suffix += 1 + 2
        elif field in {"MINUTE", "SECOND"}:
            suffix += 1 + 2
        elif field == "FRACTION":
            suffix += 1 + fractional_precision
    return suffix


def interval_leading_precision_from_character_size(
    start_field: str,
    end_field: str,
    column_size: int | None,
    fractional_precision: int | None,
) -> int | None:
    """Recover exact leading precision from ODBC SQLColumns metadata.

    ODBC reports interval ``COLUMN_SIZE`` as the character representation
    length. Removing the known separators, subordinate fields and fractional
    scale leaves the leading-field precision.
    """

    if column_size is None:
        return None
    try:
        size = int(column_size)
    except (TypeError, ValueError):
        return None
    if size <= 0:
        return None

    scale = fractional_precision or 0
    precision = size - _interval_suffix_character_count(
        start_field,
        end_field,
        scale,
    )
    maximum = 5 if start_field == "FRACTION" else 9
    if not 1 <= precision <= maximum:
        return None
    return precision


def _interval_fixed_digit_count(
    start_field: str,
    end_field: str,
    fractional_precision: int,
) -> int:
    family = _interval_family(start_field, end_field)
    if family == "year-month":
        return 0 if start_field == end_field else 2

    fields = _INTERVAL_DAY_TIME_FIELDS
    fixed = 0
    for field in fields[
        fields.index(start_field) + 1 : fields.index(end_field) + 1
    ]:
        fixed += fractional_precision if field == "FRACTION" else 2
    return fixed


def infer_interval_leading_precision_from_storage_length(
    start_field: str,
    end_field: str,
    storage_length: int | None,
    fractional_precision: int | None,
) -> tuple[int | None, bool]:
    """Infer interval precision from ``SYSCOLUMNS.collength``.

    Informix stores only physical length in the upper byte. Because two
    adjacent decimal precisions can occupy the same number of bytes, this
    fallback cannot always be textually exact. It prefers the default when
    compatible and otherwise the largest compatible precision. The boolean
    reports whether the result was unambiguous. Exact reflection is obtained
    when the ODBC SQLColumns metadata is available.
    """

    if storage_length is None:
        return None, False
    try:
        length = int(storage_length)
    except (TypeError, ValueError):
        return None, False
    if length <= 0:
        return None, False

    scale = fractional_precision or 0
    fixed = _interval_fixed_digit_count(start_field, end_field, scale)
    maximum = 5 if start_field == "FRACTION" else 9
    candidates = [
        precision
        for precision in range(1, maximum + 1)
        if math.ceil((precision + fixed) / 2) + 1 == length
    ]
    if not candidates:
        return None, False

    default = default_interval_leading_precision(start_field)
    if default in candidates:
        return None, len(candidates) == 1
    return max(candidates), len(candidates) == 1


@dataclass(frozen=True, slots=True)
class YearMonthInterval:
    """Calendar interval represented as an exact signed month count."""

    total_months: int

    def __post_init__(self) -> None:
        if isinstance(self.total_months, bool) or not isinstance(
            self.total_months, int
        ):
            raise TypeError("total_months must be an integer")

    @classmethod
    def from_years_months(
        cls,
        years: int = 0,
        months: int = 0,
    ) -> YearMonthInterval:
        if any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in (years, months)
        ):
            raise TypeError("years and months must be integers")
        if years and months and (years < 0) != (months < 0):
            raise ValueError("years and months must have the same sign")
        return cls(years * 12 + months)

    @property
    def sign(self) -> int:
        return -1 if self.total_months < 0 else 1

    @property
    def years(self) -> int:
        return self.sign * (abs(self.total_months) // 12)

    @property
    def months(self) -> int:
        return self.sign * (abs(self.total_months) % 12)

    def __int__(self) -> int:
        return self.total_months


class INTERVAL(sa_types.TypeEngine):
    """Native Informix INTERVAL preserving qualifiers and precisions.

    Year-month values are exposed as :class:`YearMonthInterval`. Day-time
    values are exposed as :class:`datetime.timedelta`. Bind values can also be
    supplied as canonical Informix strings; they are validated and normalized
    before reaching the ODBC driver.
    """

    __visit_name__ = "INTERVAL"
    cache_ok = True

    def __init__(
        self,
        start_field: str = "YEAR",
        end_field: str = "YEAR",
        leading_precision: int | None = None,
        fractional_precision: int | None = None,
    ) -> None:
        self.start_field = _normalize_interval_field(
            start_field,
            name="start_field",
        )
        self.end_field = _normalize_interval_field(
            end_field,
            name="end_field",
        )
        self.family = _interval_family(self.start_field, self.end_field)

        if self.start_field == "FRACTION" and leading_precision is not None:
            raise ValueError(
                "FRACTION TO FRACTION uses fractional_precision, not "
                "leading_precision"
            )

        self.leading_precision = _validate_interval_precision(
            leading_precision,
            name="leading_precision",
            minimum=1,
            maximum=9,
        )
        self.fractional_precision = _validate_interval_precision(
            fractional_precision,
            name="fractional_precision",
            minimum=1,
            maximum=IFX_MAX_FRACTION_DIGITS,
        )

        if self.end_field != "FRACTION" and self.fractional_precision is not None:
            raise ValueError(
                "fractional_precision is only valid when end_field is FRACTION"
            )

        self._informix_precision_exact = True
        self._informix_precision_source = "declared"
        self._informix_physical_length = None

    @classmethod
    def from_catalog(
        cls,
        *,
        first_code: int,
        last_code: int,
        storage_length: int | None,
        odbc_column_size: int | None = None,
        odbc_decimal_digits: int | None = None,
    ) -> INTERVAL:
        start_field = interval_field_from_catalog_code(first_code)
        end_field = interval_field_from_catalog_code(last_code)
        catalog_fraction = interval_fractional_precision_from_catalog_code(
            last_code
        )
        if end_field == "FRACTION":
            try:
                odbc_fraction = int(odbc_decimal_digits)
            except (TypeError, ValueError):
                odbc_fraction = None
            if odbc_fraction is not None and not 1 <= odbc_fraction <= 5:
                odbc_fraction = None
            fractional_precision = odbc_fraction or catalog_fraction or 3
        else:
            fractional_precision = None

        exact_precision = (
            None
            if start_field == "FRACTION"
            else interval_leading_precision_from_character_size(
                start_field,
                end_field,
                odbc_column_size,
                fractional_precision,
            )
        )
        if exact_precision is not None:
            default = default_interval_leading_precision(start_field)
            leading_precision = (
                None if exact_precision == default else exact_precision
            )
            precision_exact = True
            precision_source = "odbc-sqlcolumns"
        elif start_field == "FRACTION":
            leading_precision = None
            precision_exact = True
            precision_source = "catalog-fraction-qualifier"
        else:
            leading_precision, precision_exact = (
                infer_interval_leading_precision_from_storage_length(
                    start_field,
                    end_field,
                    storage_length,
                    fractional_precision,
                )
            )
            precision_source = "syscolumns-collength"

        value = cls(
            start_field=start_field,
            end_field=end_field,
            leading_precision=leading_precision,
            fractional_precision=fractional_precision,
        )
        value._informix_precision_exact = precision_exact
        value._informix_precision_source = precision_source
        value._informix_physical_length = storage_length
        return value

    @property
    def effective_leading_precision(self) -> int:
        return self.leading_precision or default_interval_leading_precision(
            self.start_field
        )

    @property
    def effective_fractional_precision(self) -> int:
        if self.end_field != "FRACTION":
            return 0
        return self.fractional_precision or 3

    @property
    def python_type(self):
        if self.family == "year-month":
            return YearMonthInterval
        return datetime.timedelta

    def qualifier_sql(self) -> str:
        start = self.start_field
        if self.leading_precision is not None:
            start += f"({self.leading_precision})"

        end = self.end_field
        if end == "FRACTION" and self.fractional_precision is not None:
            end += f"({self.fractional_precision})"
        return f"{start} TO {end}"

    def __repr__(self) -> str:
        args = [repr(self.start_field), repr(self.end_field)]
        if self.leading_precision is not None:
            args.append(f"leading_precision={self.leading_precision!r}")
        if self.fractional_precision is not None:
            args.append(
                f"fractional_precision={self.fractional_precision!r}"
            )
        return f"INTERVAL({', '.join(args)})"

    def get_dbapi_type(self, dbapi):
        """Bind canonical interval strings as ODBC SQL_VARCHAR."""
        return getattr(dbapi, "SQL_VARCHAR", None)

    def bind_expression(self, bindvalue):
        return cast(bindvalue, self)

    def coerce_compared_value(self, op, value):
        if isinstance(
            value,
            (datetime.timedelta, YearMonthInterval, str, bytes),
        ):
            return self
        return super().coerce_compared_value(op, value)

    def _coerce_year_month(self, value: Any) -> YearMonthInterval:
        if isinstance(value, YearMonthInterval):
            result = value
        elif isinstance(value, int) and not isinstance(value, bool):
            result = YearMonthInterval(value)
        elif isinstance(value, bytes):
            result = self._parse_year_month(
                _decode_ascii_temporal(value).strip()
            )
        elif isinstance(value, str):
            result = self._parse_year_month(value.strip())
        else:
            raise TypeError(
                "Informix year-month INTERVAL values require "
                "YearMonthInterval, an integer month count, str, or bytes; "
                f"received {type(value).__name__}"
            )
        self._format_year_month(result)
        return result

    def _parse_year_month(self, text: str) -> YearMonthInterval:
        sign = -1 if text.startswith("-") else 1
        if text[:1] in {"+", "-"}:
            text = text[1:]
        if not text:
            raise ValueError("Empty Informix year-month INTERVAL value")

        if self.start_field == "YEAR" and self.end_field == "MONTH":
            match = re.fullmatch(r"(\d+)-(\d{1,2})", text)
            if match is None:
                raise ValueError(
                    "Expected Informix INTERVAL YEAR TO MONTH value"
                )
            years, months = map(int, match.groups())
            if months > 11:
                raise ValueError("Trailing INTERVAL month must be 0 through 11")
            result = years * 12 + months
        elif self.start_field == self.end_field == "YEAR":
            if re.fullmatch(r"\d+", text) is None:
                raise ValueError("Expected Informix INTERVAL YEAR value")
            result = int(text) * 12
        elif self.start_field == self.end_field == "MONTH":
            if re.fullmatch(r"\d+", text) is None:
                raise ValueError("Expected Informix INTERVAL MONTH value")
            result = int(text)
        else:
            raise ValueError(
                f"Unsupported year-month qualifier {self.qualifier_sql()}"
            )
        return YearMonthInterval(sign * result)

    def _format_year_month(self, value: YearMonthInterval) -> str:
        total = value.total_months
        sign = "-" if total < 0 else ""
        absolute = abs(total)

        if self.start_field == "YEAR":
            years, months = divmod(absolute, 12)
            if self.end_field == "YEAR" and months:
                raise ValueError(
                    "INTERVAL YEAR TO YEAR cannot represent partial years"
                )
            leading = years
            text = str(years)
            if self.end_field == "MONTH":
                text += f"-{months:02d}"
        else:
            leading = absolute
            text = str(absolute)

        if len(str(leading)) > self.effective_leading_precision:
            raise OverflowError(
                "Informix INTERVAL leading field exceeds declared precision"
            )
        return sign + text

    def _interval_pattern(self) -> re.Pattern[str]:
        fields = _INTERVAL_DAY_TIME_FIELDS
        selected = fields[
            fields.index(self.start_field) : fields.index(self.end_field) + 1
        ]
        parts = [r"(?P<SIGN>[+-]?)"]
        if self.start_field == "FRACTION":
            parts.append(r"\.(?P<FRACTION>\d+)")
            return re.compile("".join(parts))

        parts.append(fr"(?P<{self.start_field}>\d+)")
        previous = self.start_field
        for field in selected[1:]:
            if field == "HOUR" and previous == "DAY":
                delimiter = r" "
            elif field == "FRACTION":
                delimiter = r"\."
            else:
                delimiter = r":"
            digits = r"\d+" if field == "FRACTION" else r"\d{1,2}"
            parts.append(fr"{delimiter}(?P<{field}>{digits})")
            previous = field
        return re.compile("".join(parts))

    def _parse_day_time(self, text: str) -> datetime.timedelta:
        match = self._interval_pattern().fullmatch(text.strip())
        if match is None:
            raise ValueError(
                "Value does not match Informix INTERVAL "
                f"{self.qualifier_sql()}: {text!r}"
            )
        values = match.groupdict()
        sign = -1 if values.pop("SIGN") == "-" else 1
        total_microseconds = 0
        for field, raw in values.items():
            if raw is None:
                continue
            if field == "FRACTION":
                scale = self.effective_fractional_precision
                normalized = raw[:scale].ljust(scale, "0")
                amount = int(normalized) * 10 ** (6 - scale)
            else:
                amount = int(raw)
                if field != self.start_field:
                    limit = _INTERVAL_SUBFIELD_LIMITS[field]
                    if amount > limit:
                        raise ValueError(
                            f"Trailing INTERVAL {field.lower()} must be "
                            f"0 through {limit}"
                        )
            total_microseconds += amount * _INTERVAL_UNIT_MICROSECONDS[field]
        value = datetime.timedelta(microseconds=sign * total_microseconds)
        self._format_day_time(value)
        return value

    def _quantized_total_microseconds(
        self,
        value: datetime.timedelta,
    ) -> int:
        total = (
            value.days * 86_400_000_000
            + value.seconds * 1_000_000
            + value.microseconds
        )
        if self.end_field == "FRACTION":
            quantum = microsecond_quantum(
                self.effective_fractional_precision
            )
        else:
            quantum = _INTERVAL_UNIT_MICROSECONDS[self.end_field]
        sign = -1 if total < 0 else 1
        return sign * ((abs(total) // quantum) * quantum)

    def _format_day_time(self, value: datetime.timedelta) -> str:
        if not isinstance(value, datetime.timedelta):
            raise TypeError("day-time INTERVAL requires datetime.timedelta")

        total = self._quantized_total_microseconds(value)
        sign = "-" if total < 0 else ""
        remainder = abs(total)
        fields = _INTERVAL_DAY_TIME_FIELDS
        selected = fields[
            fields.index(self.start_field) : fields.index(self.end_field) + 1
        ]
        rendered: dict[str, int | str] = {}

        if self.start_field == "FRACTION":
            if remainder >= 1_000_000:
                raise OverflowError(
                    "FRACTION TO FRACTION cannot represent a whole second"
                )
            scale = self.effective_fractional_precision
            fraction = remainder // 10 ** (6 - scale)
            return f"{sign}.{fraction:0{scale}d}"

        for field in selected:
            if field == "FRACTION":
                scale = self.effective_fractional_precision
                rendered[field] = (
                    remainder // 10 ** (6 - scale)
                )
                remainder = 0
                continue
            unit = _INTERVAL_UNIT_MICROSECONDS[field]
            amount, remainder = divmod(remainder, unit)
            rendered[field] = amount

        leading = int(rendered[self.start_field])
        if len(str(leading)) > self.effective_leading_precision:
            raise OverflowError(
                "Informix INTERVAL leading field exceeds declared precision"
            )

        text = str(leading)
        previous = self.start_field
        for field in selected[1:]:
            amount = rendered[field]
            if field == "HOUR" and previous == "DAY":
                text += f" {int(amount):02d}"
            elif field == "FRACTION":
                scale = self.effective_fractional_precision
                text += f".{int(amount):0{scale}d}"
            else:
                text += f":{int(amount):02d}"
            previous = field
        return sign + text

    def _coerce_day_time(self, value: Any) -> datetime.timedelta:
        if isinstance(value, datetime.timedelta):
            result = value
        elif isinstance(value, bytes):
            result = self._parse_day_time(
                _decode_ascii_temporal(value).strip()
            )
        elif isinstance(value, str):
            result = self._parse_day_time(value.strip())
        else:
            raise TypeError(
                "Informix day-time INTERVAL values require "
                "datetime.timedelta, str, or bytes; "
                f"received {type(value).__name__}"
            )
        canonical = self._format_day_time(result)
        return self._parse_day_time(canonical)

    def normalize_python_value(self, value: Any):
        if value is None:
            return None
        if self.family == "year-month":
            return self._coerce_year_month(value)
        return self._coerce_day_time(value)

    def format_bind_value(self, value: Any) -> str | None:
        if value is None:
            return None
        if self.family == "year-month":
            return self._format_year_month(self._coerce_year_month(value))
        return self._format_day_time(self._coerce_day_time(value))

    def bind_processor(self, dialect):
        _ = dialect
        return self.format_bind_value

    def result_processor(self, dialect, coltype):
        _ = dialect, coltype
        return self.normalize_python_value

    def literal_processor(self, dialect):
        _ = dialect

        def process(value: Any) -> str:
            canonical = self.format_bind_value(value)
            if canonical is None:
                return "NULL"
            return f"INTERVAL ({canonical}) {self.qualifier_sql()}"

        return process


class IFXTime(sa_types.Time):
    """Informix time-of-day with deterministic fractional precision."""

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
        self.fraction_digits = validate_fraction_digits(fraction_digits)

    def bind_expression(self, bindvalue):
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
                    "Timezone-aware datetime.time values are not supported "
                    "by Informix DATETIME"
                )
            return format_ifx_time(value, fraction_digits)

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
                temporal_value = datetime.time.fromisoformat(value.strip())
            else:
                raise TypeError(
                    "Informix returned an incompatible time value: "
                    f"{value!r} ({type(value).__name__})"
                )
            return quantize_temporal_value(temporal_value, fraction_digits)

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
        self.fraction_digits = validate_fraction_digits(fraction_digits)

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
            return format_ifx_datetime(value, fraction_digits)

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
                temporal_value = datetime.datetime.fromisoformat(value.strip())
            else:
                raise TypeError(
                    "Informix returned an incompatible datetime value: "
                    f"{value!r} ({type(value).__name__})"
                )
            return quantize_temporal_value(temporal_value, fraction_digits)

        return process
