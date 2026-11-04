"""Five-field cron expressions, evaluated in a time zone (DST-safe), without dependencies.

Supported: ``*``, lists (``1,15``), ranges (``1-5``), steps (``*/10``, ``8-18/2``), month and weekday
names (``JAN``, ``MON-FRI``), Sunday as 0 or 7, and the macros ``@yearly``, ``@annually``,
``@monthly``, ``@weekly``, ``@daily``, ``@midnight`` and ``@hourly``.

As in classic cron, when both day-of-month and day-of-week are restricted a day matches if
*either* does. Times are matched on the local wall clock of the schedule's time zone: a wall
time skipped by a DST jump never fires, and a wall time that occurs twice fires once (the first).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
MONTHS = {
    m: i
    for i, m in enumerate(
        ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], start=1
    )
}
DAYS = {d: i for i, d in enumerate(["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"])}
#: (name, low, high, aliases)
FIELDS = [
    ("minute", 0, 59, {}),
    ("hour", 0, 23, {}),
    ("day of month", 1, 31, {}),
    ("month", 1, 12, MONTHS),
    ("day of week", 0, 7, DAYS),
]
SEARCH_YEARS = 8  # long enough for "29 Feb"; an expression that never matches fails fast


class CronError(ValueError):
    pass


def _value(text: str, low: int, high: int, aliases: dict[str, int], field: str) -> int:
    key = text.upper()
    if key in aliases:
        return aliases[key]
    if not text.isdigit():
        raise CronError(f"bad {field} value {text!r}")
    n = int(text)
    if not low <= n <= high:
        raise CronError(f"{field} value {n} is outside {low}-{high}")
    return n


def _field(
    text: str, low: int, high: int, aliases: dict[str, int], field: str
) -> tuple[frozenset[int], bool]:
    """Parse one field into its set of values; the flag says whether it was ``*``."""
    values: set[int] = set()
    star = False
    for part in text.split(","):
        if not part:
            raise CronError(f"empty item in {field} {text!r}")
        base, _, step_text = part.partition("/")
        step = 1
        if step_text:
            if not step_text.isdigit() or int(step_text) == 0:
                raise CronError(f"bad step {step_text!r} in {field}")
            step = int(step_text)
        if base == "*":
            start, end = low, high
            star = star or not step_text
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = _value(a, low, high, aliases, field), _value(b, low, high, aliases, field)
            if start > end:
                raise CronError(f"range {base!r} in {field} runs backwards")
        else:
            start = _value(base, low, high, aliases, field)
            end = high if step_text else start
        values.update(range(start, end + 1, step))
    return frozenset(values), star


@dataclass(frozen=True)
class Cron:
    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]  # 0 = Sunday
    any_day: bool
    any_weekday: bool

    @classmethod
    def parse(cls, expression: str) -> Cron:
        text = MACROS.get(expression.strip().lower(), expression.strip())
        parts = text.split()
        if len(parts) != 5:
            raise CronError(f"cron needs 5 fields (minute hour day month weekday), got {expression!r}")
        parsed = [_field(p, lo, hi, al, name) for p, (name, lo, hi, al) in zip(parts, FIELDS, strict=True)]
        weekdays = frozenset(d % 7 for d in parsed[4][0])
        cron = cls(
            expression=expression,
            minutes=parsed[0][0],
            hours=parsed[1][0],
            days=parsed[2][0],
            months=parsed[3][0],
            weekdays=weekdays,
            any_day=parsed[2][1],
            any_weekday=parsed[4][1],
        )
        cron.next_after(datetime(2000, 1, 1, tzinfo=UTC))  # raises if it can never fire
        return cron

    def _day_matches(self, d: datetime) -> bool:
        dom = d.day in self.days
        dow = (d.weekday() + 1) % 7 in self.weekdays
        if self.any_day and self.any_weekday:
            return True
        if self.any_day:
            return dow
        if self.any_weekday:
            return dom
        return dom or dow

    def next_after(self, after: datetime, tz: str | ZoneInfo = "UTC") -> datetime:
        """The first firing time strictly after ``after`` (an aware datetime), as an aware UTC datetime."""
        zone = tz if isinstance(tz, ZoneInfo) else ZoneInfo(tz)
        local = after.astimezone(zone).replace(second=0, microsecond=0, tzinfo=None) + timedelta(minutes=1)
        limit = local + timedelta(days=366 * SEARCH_YEARS)
        while local < limit:
            if local.month not in self.months:
                year, month = (local.year + 1, 1) if local.month == 12 else (local.year, local.month + 1)
                local = datetime(year, month, 1)
                continue
            if not self._day_matches(local):
                local = datetime(local.year, local.month, local.day) + timedelta(days=1)
                continue
            if local.hour not in self.hours:
                local = datetime(local.year, local.month, local.day, local.hour) + timedelta(hours=1)
                continue
            if local.minute not in self.minutes:
                local += timedelta(minutes=1)
                continue
            aware = local.replace(tzinfo=zone)
            instant = aware.astimezone(UTC)
            # Skip wall times that do not exist (spring-forward gap): they do not round-trip.
            if instant.astimezone(zone).replace(tzinfo=None) != local:
                local += timedelta(minutes=1)
                continue
            if instant > after.astimezone(UTC):
                return instant
            local += timedelta(minutes=1)
        raise CronError(f"cron {self.expression!r} never fires")

    def next_timestamp(self, after: float, tz: str = "UTC") -> float:
        return self.next_after(datetime.fromtimestamp(after, UTC), tz).timestamp()


def validate_timezone(tz: str) -> str:
    try:
        ZoneInfo(tz)
    except Exception as exc:  # ZoneInfoNotFoundError or ValueError
        raise CronError(f"unknown time zone {tz!r}") from exc
    return tz
