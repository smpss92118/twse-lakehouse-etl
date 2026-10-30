"""Trading-day gate from the official TWSE holiday schedule.

The schedule also lists trading days (e.g. "國曆新年開始交易日", "農曆春節前最後交易日"), so a listed
date is only a closure when its name does not mark it as a trading day. Ad-hoc closures such as
typhoon days are never in this schedule; they are caught later by the data-wait + LLM fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

TRADING_DAY_MARKERS = ("開始交易", "最後交易")
SATURDAY = 5


@dataclass(frozen=True)
class DayStatus:
    is_trading_day: bool
    reason: str


def parse_schedule(doc: dict) -> dict[date, str]:
    """Return {closed_date: holiday_name} from a holidaySchedule JSON payload."""
    if doc.get("stat", "").lower() != "ok":
        raise ValueError(f"holiday schedule unavailable: {doc.get('stat')!r}")
    closed: dict[date, str] = {}
    for day, name, *_ in doc["data"]:
        if any(marker in name for marker in TRADING_DAY_MARKERS):
            continue
        closed[date.fromisoformat(day)] = name.strip()
    return closed


def day_status(d: date, closed: dict[date, str]) -> DayStatus:
    if d.weekday() >= SATURDAY:
        return DayStatus(False, "weekend")
    if d in closed:
        return DayStatus(False, closed[d])
    return DayStatus(True, "regular trading day")
