"""Cron parsing and next-fire computation, including time zones and DST."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from ninelives.cron import Cron, CronError, validate_timezone


def nxt(expr: str, after: str, tz: str = "UTC") -> str:
    start = datetime.fromisoformat(after)
    if start.tzinfo is None:
        start = start.replace(tzinfo=ZoneInfo(tz))
    out = Cron.parse(expr).next_after(start, tz).astimezone(ZoneInfo(tz))
    return out.strftime("%Y-%m-%d %H:%M %a")


@pytest.mark.parametrize(
    ("expr", "after", "expected"),
    [
        ("* * * * *", "2026-03-01 10:00:30", "2026-03-01 10:01 Sun"),
        ("*/15 * * * *", "2026-03-01 10:14", "2026-03-01 10:15 Sun"),
        ("*/15 * * * *", "2026-03-01 10:15", "2026-03-01 10:30 Sun"),  # strictly after
        ("0 9 * * MON-FRI", "2026-03-06 09:00", "2026-03-09 09:00 Mon"),  # Friday 9:00 -> Monday
        ("30 8-18/2 * * *", "2026-03-01 18:31", "2026-03-02 08:30 Mon"),
        ("0 0 1 * *", "2026-01-31 12:00", "2026-02-01 00:00 Sun"),
        ("0 0 29 2 *", "2026-03-01 00:00", "2028-02-29 00:00 Tue"),  # leap day
        ("0 12 * JAN,jul *", "2026-02-01 00:00", "2026-07-01 12:00 Wed"),
        ("0 0 * * 7", "2026-03-02 00:00", "2026-03-08 00:00 Sun"),  # 7 is Sunday too
        ("0 0 * * sun", "2026-03-02 00:00", "2026-03-08 00:00 Sun"),
        ("0 0 13 * FRI", "2026-03-01 00:00", "2026-03-06 00:00 Fri"),  # day-of-month OR day-of-week
        ("0 0 13 * FRI", "2026-03-07 00:00", "2026-03-13 00:00 Fri"),
        ("0 0 13 * *", "2026-03-01 00:00", "2026-03-13 00:00 Fri"),
        ("5,10 * * * *", "2026-03-01 10:06", "2026-03-01 10:10 Sun"),
        ("@hourly", "2026-03-01 10:59", "2026-03-01 11:00 Sun"),
        ("@daily", "2026-12-31 23:59", "2027-01-01 00:00 Fri"),
        ("@weekly", "2026-03-01 00:00", "2026-03-08 00:00 Sun"),
        ("@monthly", "2026-03-01 00:00", "2026-04-01 00:00 Wed"),
        ("@yearly", "2026-03-01 00:00", "2027-01-01 00:00 Fri"),
        ("*/7 * * * *", "2026-03-01 10:56", "2026-03-01 11:00 Sun"),  # steps restart each hour
    ],
)
def test_next_fire_time(expr: str, after: str, expected: str) -> None:
    assert nxt(expr, after) == expected


def test_spring_forward_gap_is_skipped() -> None:
    # Europe/Berlin jumps from 02:00 to 03:00 on 2026-03-29: 02:30 does not exist that day.
    assert nxt("30 2 * * *", "2026-03-28 03:00", "Europe/Berlin") == "2026-03-30 02:30 Mon"
    assert nxt("0 * * * *", "2026-03-29 01:30", "Europe/Berlin") == "2026-03-29 03:00 Sun"


def test_fall_back_hour_fires_once() -> None:
    # America/New_York repeats 01:00-02:00 on 2026-11-01.
    cron = Cron.parse("30 1 * * *")
    tz = ZoneInfo("America/New_York")
    first = cron.next_after(datetime(2026, 11, 1, 0, 0, tzinfo=tz), "America/New_York")
    second = cron.next_after(first, "America/New_York")
    assert first.astimezone(tz).strftime("%d %H:%M") == "01 01:30"
    assert second.astimezone(tz).strftime("%d %H:%M") == "02 01:30"


def test_local_time_zone_is_honoured() -> None:
    cron = Cron.parse("0 9 * * *")
    after = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
    assert cron.next_after(after, "Asia/Kolkata") == datetime(2026, 7, 1, 3, 30, tzinfo=UTC)
    assert cron.next_after(after, "America/Los_Angeles") == datetime(2026, 7, 1, 16, 0, tzinfo=UTC)
    assert cron.next_timestamp(after.timestamp(), "UTC") == datetime(2026, 7, 1, 9, 0, tzinfo=UTC).timestamp()


@pytest.mark.parametrize(
    ("expr", "message"),
    [
        ("* * * *", "5 fields"),
        ("60 * * * *", "outside 0-59"),
        ("* 24 * * *", "outside 0-23"),
        ("* * 0 * *", "outside 1-31"),
        ("* * * 13 *", "outside 1-12"),
        ("* * * * 8", "outside 0-7"),
        ("*/0 * * * *", "bad step"),
        ("5-1 * * * *", "backwards"),
        ("a * * * *", "bad minute"),
        ("1,,2 * * * *", "empty item"),
        ("0 0 31 2 *", "never fires"),
        ("0 0 30 FEB *", "never fires"),
    ],
)
def test_invalid_expressions(expr: str, message: str) -> None:
    with pytest.raises(CronError, match=message):
        Cron.parse(expr)


def test_time_zone_validation() -> None:
    assert validate_timezone("Europe/Paris") == "Europe/Paris"
    with pytest.raises(CronError, match="unknown time zone"):
        validate_timezone("Mars/Olympus")
