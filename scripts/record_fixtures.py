"""Record raw TWSE payloads (and third-party corporate actions) as gzip fixtures.

Fixtures are byte-for-byte TWSE responses, so the `fixture` source replays exactly what the live
API returned. Used by CI (no network) and as the fallback when TWSE rate-limits.

    uv run python scripts/record_fixtures.py
    uv run python scripts/record_fixtures.py --vendor-src ../data_center/finmind_data/corporate_actions/source
"""

from __future__ import annotations

import argparse
import gzip
import json
from datetime import date
from pathlib import Path

from twse_etl import sources
from twse_etl.config import REPO_ROOT, Settings

TRADE_DATES = [
    "2026-03-16",
    "2026-03-17",  # 2330 ex-dividend: MI_INDEX shows change sign "X"
    "2026-10-22",
    "2026-10-23",
    "2026-10-27",  # previous trading day is 10-23; 10-26 is a make-up holiday
    "2026-10-28",
    "2026-10-29",
]
HOLIDAY_YEARS = [2024, 2026]


def record_twse(fixture_dir: Path) -> None:
    settings = Settings.from_env().with_overrides(source="twse")
    jobs = [(r, date.fromisoformat(d)) for d in TRADE_DATES for r in ("MI_INDEX", "T86", "TWT49U")]
    jobs += [("HOLIDAY", date(y, 1, 1)) for y in HOLIDAY_YEARS]
    for report, d in jobs:
        path = sources.fixture_path(fixture_dir, report, d)
        if path.exists():
            continue
        payload = sources.fetch(report, d, settings)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(payload.body, mtime=0))
        print(f"{report:9s} {d}  stat={payload.json().get('stat')!r:12}  {len(payload.body):>8} bytes")


def record_vendor(vendor_src: Path, vendor_dir: Path) -> None:
    """Keep only fixture-date rows from a FinMind TaiwanStockDividendResult export."""
    wanted = set(TRADE_DATES)
    rows: dict[tuple[str, str], dict] = {}
    for f in sorted(vendor_src.glob("TaiwanStockDividendResult_*.json")):
        for row in json.loads(f.read_text()).get("data", []):
            if row.get("date") in wanted:
                rows[(row["date"], row["stock_id"])] = row
    vendor_dir.mkdir(parents=True, exist_ok=True)
    out = vendor_dir / "TaiwanStockDividendResult_fixture.json"
    data = sorted(rows.values(), key=lambda r: (r["date"], r["stock_id"]))
    doc = {"msg": "fixture subset of FinMind TaiwanStockDividendResult", "data": data}
    out.write_text(json.dumps(doc, ensure_ascii=False, indent=1))
    print(f"vendor rows: {len(rows)} -> {out}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture-dir", type=Path, default=REPO_ROOT / "fixtures" / "twse")
    parser.add_argument("--vendor-src", type=Path, help="FinMind export dir (optional)")
    parser.add_argument("--vendor-dir", type=Path, default=REPO_ROOT / "fixtures" / "vendor")
    args = parser.parse_args()
    record_twse(args.fixture_dir)
    if args.vendor_src:
        record_vendor(args.vendor_src, args.vendor_dir)


if __name__ == "__main__":
    main()
