from __future__ import annotations

from datetime import date

import pytest
from conftest import load_fixture

from twse_etl.trading_calendar import day_status, parse_schedule

CLOSED = parse_schedule(load_fixture("HOLIDAY", date(2026, 1, 1)))


@pytest.mark.parametrize(
    ("day", "is_trading"),
    [
        (date(2026, 1, 2), True),  # listed as "國曆新年開始交易日": a trading day, not a holiday
        (date(2026, 2, 11), True),  # "農曆春節前最後交易日"
        (date(2026, 2, 12), False),  # "市場無交易，僅辦理結算交割作業"
        (date(2026, 10, 26), False),  # make-up holiday on a Monday
        (date(2026, 10, 24), False),  # Saturday
        (date(2026, 10, 23), True),  # typhoon closure: never in the schedule, caught by the data wait
    ],
)
def test_trading_day(day, is_trading):
    assert day_status(day, CLOSED).is_trading_day is is_trading
