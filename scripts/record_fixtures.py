"""Record raw TWSE payloads as gzip fixtures.

Fixtures are byte-for-byte TWSE responses, so the `fixture` source replays exactly what the live
API returned. Used by CI (no network) and as the fallback when TWSE rate-limits. Files that exist
are kept; only missing ones are fetched.

    uv run python scripts/record_fixtures.py
"""

from __future__ import annotations

import argparse
import gzip
from datetime import date
from pathlib import Path

from twse_etl import sources
from twse_etl.config import REPO_ROOT, Settings

TRADE_DATES = [
    "2026-03-16",
    "2026-03-17",  # 2330 ex-dividend: MI_INDEX shows change sign "X"
    "2026-10-22",
    "2026-10-23",
    "2026-10-27",  # previous trading day is 10-23; 10-26 is a make-up holiday; 1516, 6142 resume after reduction
    "2026-10-28",
    "2026-10-29",
]
# Corporate actions the main window lacks, for unit tests of splits and capital reductions.
EVENT_DATES = [
    "2025-06-10",  # 0050's last close before its 1:4 split halt
    "2025-06-17",  # previous trading day of the resumption (0050 still halted)
    "2025-06-18",  # 0050 resumes; not marked X, so close - change = opening reference price
    "2025-06-23",  # 2371 resumes after a capital reduction with a cash refund; marked X
]
# MI_5MINS answers "did the market open?" after the data-wait cutoff: the 10-23 typhoon closure
# ("no data") and a normal trading day (09:00-13:30 records).
INTRADAY_DATES = ["2026-10-23", "2026-10-29"]
HOLIDAY_YEARS = [2024, 2026]


def _record(fixture_dir: Path, settings: Settings, report: str, d: date) -> None:
    path = sources.fixture_path(fixture_dir, report, d)
    if path.exists():
        return
    payload = sources.fetch(report, d, settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(payload.body, mtime=0))
    print(f"{report:9s} {d}  stat={payload.json().get('stat')!r:12}  {len(payload.body):>8} bytes")


def record_twse(fixture_dir: Path) -> None:
    settings = Settings.from_env().with_overrides(source="twse")
    jobs = [(r, d) for d in TRADE_DATES for r in ("MI_INDEX", "T86", *sources.RANGE_REPORTS)]
    jobs += [(r, d) for d in EVENT_DATES for r in ("MI_INDEX", *sources.RANGE_REPORTS)]
    jobs += [("MI_5MINS", d) for d in INTRADAY_DATES]
    for report, d in jobs:
        _record(fixture_dir, settings, report, date.fromisoformat(d))
    for year in HOLIDAY_YEARS:
        _record(fixture_dir, settings, "HOLIDAY", date(year, 1, 1))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture-dir", type=Path, default=REPO_ROOT / "fixtures" / "twse")
    record_twse(parser.parse_args().fixture_dir)


if __name__ == "__main__":
    main()
