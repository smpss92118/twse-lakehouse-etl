"""End to end on recorded payloads: calendar gate, typhoon fallback, idempotency, corrections."""

from __future__ import annotations

import json
from datetime import date, timedelta

import pytest
from conftest import FAKE_LLM, TYPHOON_VERDICT, load_fixture, make_settings

from twse_etl import iceberg_io, pipeline, storage
from twse_etl.validate import DataQualityError

DAYS = [date(2026, 10, 22) + timedelta(days=i) for i in range(8)]  # 10-22 .. 10-29


@pytest.fixture(scope="module")
def loaded(tmp_path_factory):
    """One backfill shared by the read-only tests below."""
    mp = pytest.MonkeyPatch()
    mp.setenv("HOLIDAY_LLM_CMD", FAKE_LLM)
    mp.setenv("FAKE_LLM_VERDICT", json.dumps(TYPHOON_VERDICT, ensure_ascii=False))
    settings = make_settings(tmp_path_factory.mktemp("lake"))
    results = {d: pipeline.run_day(d, settings) for d in DAYS}
    mp.undo()
    return settings, results


def test_backfill_skips_closures_and_loads_trading_days(loaded):
    _, results = loaded
    skipped = {d.isoformat(): r["skipped"] for d, r in results.items() if "skipped" in r}
    assert skipped == {
        "2026-10-23": TYPHOON_VERDICT["reason"],  # not in the calendar: data wait + LLM
        "2026-10-24": "weekend",
        "2026-10-25": "weekend",
        "2026-10-26": "臺灣光復暨金門古寧頭大捷紀念日",  # official calendar
    }
    for r in results.values():
        if "row_counts" in r:
            assert r["row_counts"]["daily_market_wide"] == r["row_counts"]["daily_quotes"]
            assert all(c["passed"] for c in r["reconcile"])


def test_typhoon_closure_is_recorded_for_human_confirmation(loaded):
    settings, _ = loaded
    record = json.loads(storage.read_bytes(storage.join(settings.data_uri, "closures", "2026-10-23.json")))
    assert record["status"] == "pending_human_confirmation"
    assert record["evidence_url"].startswith("https://")


def test_ex_dividend_on_typhoon_day_is_flagged_on_next_trading_day(loaded):
    settings, _ = loaded
    wide = iceberg_io.read(iceberg_io.get_table(settings, "daily_market_wide"), date(2026, 10, 27))
    flagged = set(wide.filter(wide["is_ex_event"])["stock_id"].to_list())
    assert {"2028", "6931", "00907"} <= flagged


def test_rerun_same_day_creates_no_snapshot(loaded):
    settings, _ = loaded
    tbl = iceberg_io.get_table(settings, "daily_quotes")
    before = len(tbl.snapshots())
    result = pipeline.run_day(date(2026, 10, 29), settings)
    tbl.refresh()
    assert len(tbl.snapshots()) == before
    assert all(p["skipped"] for p in result["published"].values())


def test_correction_replaces_day_without_duplicates_and_keeps_old_version(settings):
    d = date(2026, 10, 29)
    pipeline.run_day(d, settings)
    tbl = iceberg_io.get_table(settings, "daily_quotes")
    original = iceberg_io.current_version(tbl, d)

    doc = load_fixture("MI_INDEX", d)
    table = next(t for t in doc["tables"] if "證券代號" in (t.get("fields") or []))
    row = next(r for r in table["data"] if r[0] == "2330")
    row[table["fields"].index("成交股數")] = "12,345,678"
    pipeline.run_day(d, settings, run_id="correction-1", bodies={
        "MI_INDEX": json.dumps(doc, ensure_ascii=False).encode(),
        "T86": json.dumps(load_fixture("T86", d), ensure_ascii=False).encode(),
        "TWT49U": json.dumps(load_fixture("TWT49U", d), ensure_ascii=False).encode(),
    })  # fmt: skip

    tbl.refresh()
    now = iceberg_io.read(tbl, d)
    assert now.height == now["stock_id"].n_unique() == 1388
    new_vol = now.filter(now["stock_id"] == "2330")["volume_shares"][0]
    old = iceberg_io.read(tbl, d, snapshot_id=original.snapshot_id)
    old_vol = old.filter(old["stock_id"] == "2330")["volume_shares"][0]
    assert (old_vol, new_vol) == (18_403_916, 12_345_678)


@pytest.mark.parametrize(
    ("verdict", "exit_code"),
    [
        ({"is_holiday": False, "reason": "no closure found", "evidence_url": None}, 0),
        ({"is_holiday": True, "reason": "typhoon", "evidence_url": None}, 0),  # no evidence
        ("not json", 0),
        (None, 1),  # LLM unavailable
    ],
)
def test_missing_data_without_evidenced_closure_is_an_anomaly(settings, fake_llm, verdict, exit_code):
    fake_llm(verdict, exit_code)
    with pytest.raises(pipeline.MissingDataError):
        pipeline.run_day(date(2026, 10, 23), settings)


def test_audit_failure_leaves_iceberg_untouched(settings):
    d = date(2026, 10, 29)
    t86 = load_fixture("T86", d)
    t86["data"] = t86["data"][:800]  # partial publish that slipped past the wait
    with pytest.raises(DataQualityError, match="institutional_coverage"):
        pipeline.run_day(d, settings, bodies={
            "MI_INDEX": json.dumps(load_fixture("MI_INDEX", d), ensure_ascii=False).encode(),
            "T86": json.dumps(t86, ensure_ascii=False).encode(),
            "TWT49U": json.dumps(load_fixture("TWT49U", d), ensure_ascii=False).encode(),
        })  # fmt: skip
    assert iceberg_io.get_table(settings, "daily_quotes").current_snapshot() is None
