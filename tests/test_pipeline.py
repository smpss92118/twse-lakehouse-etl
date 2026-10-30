"""End to end on recorded payloads: calendar gate, typhoon fallback, idempotency, corrections."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pyarrow as pa
import pytest
from conftest import FAKE_LLM, FIXTURES, TYPHOON_VERDICT, load_fixture, make_settings

from twse_etl import contracts, iceberg_io, pipeline, storage, transform
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


def test_corporate_actions_land_on_the_day_they_take_effect(loaded):
    settings, _ = loaded
    wide = iceberg_io.read(iceberg_io.get_table(settings, "daily_market_wide"), date(2026, 10, 27))
    flagged = set(wide.filter(wide["is_ex_event"])["stock_id"].to_list())
    assert {"2028", "6931", "00907"} <= flagged  # ex-dividend dated on the 10-23 typhoon closure
    reductions = wide.filter(pl.col("corporate_action") == "capital_reduction").sort("stock_id")
    assert reductions["stock_id"].to_list() == ["1516", "6142"]  # resume after a reduction (TWTAUU)
    assert reductions["adj_factor"].to_list() == pytest.approx([24.73 / 18.55, 9.47 / 8.71])


def test_contract_columns_are_added_to_a_table_created_by_an_older_release(settings):
    """Schema evolution on deploy: old data files are not rewritten, their rows read the new column as null."""
    contract = contracts.CORPORATE_ACTIONS_SCHEMA
    old = pa.schema(list(contract)[: contract.get_field_index("source_report")])  # before adjusted prices
    catalog = iceberg_io.get_catalog(settings)
    catalog.create_namespace_if_not_exists(settings.namespace)
    tbl = catalog.create_table(f"{settings.namespace}.corporate_actions", schema=old)
    row = {"trade_date": date(2026, 10, 29), "stock_id": "2330", "run_id": "v1", "source_sha256": "-"}
    tbl.append(pa.Table.from_pylist([{**row, "ingested_at": datetime(2026, 10, 29, tzinfo=UTC)}], schema=old))

    tbl = iceberg_io.get_table(settings, "corporate_actions")
    assert tbl.schema().column_names == contracts.CORPORATE_ACTIONS_SCHEMA.names
    assert iceberg_io.read(tbl, date(2026, 10, 29))["source_report"].to_list() == [None]


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


def test_late_report_on_an_open_market_is_never_turned_into_a_closure(tmp_path, monkeypatch):
    """Quotes are out, T86 is not: the LLM must not be asked, whatever it would say."""
    fixtures = tmp_path / "fixtures"
    for report, key in (("HOLIDAY", "2026"), ("MI_INDEX", "20261029")):
        (fixtures / report).mkdir(parents=True)
        shutil.copy(FIXTURES / report / f"{key}.json.gz", fixtures / report)
    settings = make_settings(tmp_path / "lake").with_overrides(fixture_dir=fixtures)
    monkeypatch.setenv("HOLIDAY_LLM_CMD", FAKE_LLM)
    monkeypatch.setenv("FAKE_LLM_VERDICT", json.dumps(TYPHOON_VERDICT, ensure_ascii=False))

    with pytest.raises(pipeline.MissingDataError, match="market was open"):
        pipeline.run_day(date(2026, 10, 29), settings)
    assert not pipeline.is_closure_recorded(date(2026, 10, 29), settings)


def test_skipped_day_is_reported_as_missing_not_as_bad_prices(settings):
    pipeline.run_day(date(2026, 10, 22), settings)
    with pytest.raises(DataQualityError) as exc:
        pipeline.run_day(date(2026, 10, 28), settings)  # 10-27 was never loaded
    assert [f.name for f in exc.value.failures] == ["previous_trading_day_loaded"]
    assert "load 2026-10-27 first" in str(exc.value)


def test_parser_fix_rewrites_the_day_although_twse_bytes_are_unchanged(settings, monkeypatch):
    d = date(2026, 10, 29)
    pipeline.run_day(d, settings)
    buggy = transform.parse_quotes

    def fixed(doc, trade_date):  # stands in for a bug fix that changes one parsed value
        df = buggy(doc, trade_date)
        return df.with_columns(
            pl.when(pl.col("stock_id") == "2330").then(pl.col("volume_shares") + 1).otherwise(pl.col("volume_shares"))
        )

    monkeypatch.setattr(transform, "parse_quotes", fixed)
    published = pipeline.run_day(d, settings)["published"]
    assert not published["daily_quotes"]["skipped"]  # same source sha256, different content
    assert published["institutional_flows"]["skipped"]
    assert not published["daily_market_wide"]["skipped"]  # Gold follows the new quotes snapshot


def test_audit_failure_leaves_iceberg_untouched(settings):
    d = date(2026, 10, 29)
    t86 = load_fixture("T86", d)
    t86["data"] = t86["data"][:800]  # partial publish that slipped past the wait
    with pytest.raises(DataQualityError, match="institutional_coverage"):
        pipeline.run_day(d, settings, bodies={
            "MI_INDEX": json.dumps(load_fixture("MI_INDEX", d), ensure_ascii=False).encode(),
            "T86": json.dumps(t86, ensure_ascii=False).encode(),
        })  # fmt: skip
    assert iceberg_io.get_table(settings, "daily_quotes").current_snapshot() is None
