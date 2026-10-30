"""Pipeline steps as plain functions over primitives (dates, URIs, ids).

The Airflow DAG calls these one per task; the CLI and the tests call them in sequence through
run_day(). Nothing here imports Airflow.

    python -m twse_etl.pipeline run --date 2026-10-29
    python -m twse_etl.pipeline backfill --start 2026-10-22 --end 2026-10-29
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from datetime import UTC, date, datetime, timedelta

import polars as pl

from twse_etl import (
    contracts,
    corporate_actions,
    gold,
    holiday_fallback,
    iceberg_io,
    sources,
    storage,
    trading_calendar,
    transform,
)
from twse_etl.alerts import alert
from twse_etl.config import Settings
from twse_etl.validate import (
    Check,
    audit_cross_table,
    audit_institutional,
    audit_price_continuity,
    audit_quotes,
    enforce,
)

log = logging.getLogger("twse_etl.pipeline")

REPORT_TABLES = {"MI_INDEX": "daily_quotes", "T86": "institutional_flows", "TWT49U": "corporate_actions"}
REPORT_SCHEMAS = {
    "MI_INDEX": contracts.QUOTES_SCHEMA,
    "T86": contracts.INSTITUTIONAL_SCHEMA,
    "TWT49U": contracts.CORPORATE_ACTIONS_SCHEMA,
}
GOLD_TABLE = "daily_market_wide"
WAIT_REPORTS = ("MI_INDEX", "T86")


class IncompleteDataError(RuntimeError):
    """TWSE has not (fully) published yet. Retriable."""


class MissingDataError(RuntimeError):
    """Trading day, cutoff passed, no data, and no evidenced closure. Not retriable."""


def _safe(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


# ---- gate and wait -------------------------------------------------------------------------


def trading_day_status(d: date, settings: Settings) -> trading_calendar.DayStatus:
    if d.weekday() >= trading_calendar.SATURDAY:
        return trading_calendar.DayStatus(False, "weekend")
    schedule = sources.fetch("HOLIDAY", d, settings).json()
    return trading_calendar.day_status(d, trading_calendar.parse_schedule(schedule))


def reports_ready(d: date, settings: Settings) -> dict[str, int]:
    """Row counts per report; 0 means not (fully) published."""
    counts = {}
    for report in WAIT_REPORTS:
        doc = sources.fetch(report, d, settings).json()
        counts[report] = transform.row_count(report, doc) if transform.is_complete(report, doc, d) else 0
    return counts


def resolve_missing_data(d: date, settings: Settings) -> str:
    """Called after the data-wait cutoff. Returns the closure reason, or raises MissingDataError."""
    try:
        verdict = holiday_fallback.ask_llm(d)
    except holiday_fallback.HolidayCheckError as exc:
        alert("data_missing_llm_unavailable", trade_date=d, error=str(exc))
        raise MissingDataError(f"{d}: no TWSE data and closure check failed: {exc}") from exc

    if verdict.is_holiday and verdict.evidence_url:
        record = {**verdict.to_dict(), "trade_date": d.isoformat(), "status": "pending_human_confirmation"}
        uri = storage.join(settings.data_uri, "closures", f"{d}.json")
        storage.write_bytes(uri, json.dumps(record, ensure_ascii=False, indent=1).encode())
        alert("unscheduled_closure_detected", trade_date=d, reason=verdict.reason, evidence=verdict.evidence_url)
        return verdict.reason

    alert("data_missing_on_trading_day", trade_date=d, llm_verdict=verdict.to_dict())
    raise MissingDataError(f"{d}: trading day per calendar, no TWSE data, no evidenced closure")


# ---- extract / stage / audit / publish ------------------------------------------------------


def previous_trading_day(d: date, settings: Settings) -> date | None:
    return iceberg_io.latest_date_before(iceberg_io.get_table(settings, "daily_quotes"), d)


def ca_window_start(d: date, settings: Settings) -> date:
    """Corporate actions cover (previous loaded trading day, d] so closure-day events are kept."""
    prev = previous_trading_day(d, settings)
    return prev + timedelta(days=1) if prev else d


def extract(report: str, d: date, settings: Settings, body: bytes | None = None) -> str:
    """Fetch one report into Bronze. `body` injects a payload (used to simulate a TWSE correction)."""
    if body is not None:
        payload = sources.RawPayload(report, d, body, "injected://correction", datetime.now(UTC))
    else:
        start = ca_window_start(d, settings) if report == "TWT49U" else None
        payload = sources.fetch(report, d, settings, start=start)
    if report in WAIT_REPORTS and not transform.is_complete(report, payload.json(), d):
        raise IncompleteDataError(f"{report} {d}: {transform.row_count(report, payload.json())} rows")
    uri = sources.land_bronze(payload, settings)
    log.info("bronze %s %s sha256=%s -> %s", report, d, payload.sha256[:12], uri)
    return uri


def stage(report: str, d: date, bronze_uri: str, settings: Settings, run_id: str) -> dict:
    """Clean into a typed parquet file (the 'Write' of Write-Audit-Publish)."""
    payload = sources.load_bronze(bronze_uri)
    doc = payload.json()
    if report == "MI_INDEX":
        df = transform.parse_quotes(doc, d)
        extra = transform.unexpected_fields(transform.find_quote_table(doc)["fields"], contracts.QUOTE_FIELDS)
    elif report == "T86":
        df = transform.parse_institutional(doc, d)
        extra = transform.unexpected_fields(doc["fields"], contracts.INSTITUTIONAL_FIELDS)
    else:
        start = ca_window_start(d, settings)
        vendor = corporate_actions.load_vendor(settings.vendor_dir, start, d)
        df = corporate_actions.attach_vendor(transform.parse_corporate_actions(doc), vendor)
        extra = []
    if extra:
        alert("schema_drift_new_columns", report=report, trade_date=d, columns=extra)

    schema = REPORT_SCHEMAS[report]
    df = df.with_columns(
        pl.lit(run_id).alias("run_id"),
        pl.lit(payload.sha256).alias("source_sha256"),
        pl.lit(datetime.now(UTC)).dt.cast_time_unit("us").alias("ingested_at"),
    )
    table = df.select(schema.names).to_arrow().cast(schema)
    uri = storage.join(settings.data_uri, "staging", report, f"trade_date={d}", f"{_safe(run_id)}.parquet")
    storage.write_parquet(uri, table)
    log.info("staged %s %s rows=%d -> %s", report, d, table.num_rows, uri)
    return {"uri": uri, "sha256": payload.sha256, "rows": table.num_rows}


def _load_staged(staged: dict[str, dict]) -> dict[str, pl.DataFrame]:
    return {report: pl.from_arrow(storage.read_parquet(info["uri"])) for report, info in staged.items()}


def _prev_close(d: date, settings: Settings) -> pl.DataFrame | None:
    prev = previous_trading_day(d, settings)
    if prev is None:
        return None
    quotes = iceberg_io.read(iceberg_io.get_table(settings, "daily_quotes"), prev)
    return quotes.select("stock_id", "close")


def audit(d: date, staged: dict[str, dict], settings: Settings) -> dict:
    """All checks run before any table is touched; one blocking failure stops the whole day."""
    frames = _load_staged(staged)
    quotes, institutional, actions = frames["MI_INDEX"], frames["T86"], frames["TWT49U"]
    prev_close = _prev_close(d, settings)
    checks: list[Check] = [
        *audit_quotes(quotes, d),
        *audit_price_continuity(quotes, prev_close, actions),
        *audit_institutional(institutional, d),
        *audit_cross_table(quotes, institutional),
        *corporate_actions.audit_corporate_actions(actions, quotes, prev_close, d),
    ]
    enforce(checks, trade_date=d)
    return {
        "checks": [ch.to_dict() for ch in checks],
        "unexplained_x": corporate_actions.unexplained_x(quotes, actions),
    }


def publish(d: date, staged: dict[str, dict], settings: Settings, run_id: str) -> dict[str, dict]:
    catalog = iceberg_io.get_catalog(settings)
    window_start = ca_window_start(d, settings)
    results = {}
    for report, name in REPORT_TABLES.items():
        tbl = iceberg_io.get_table(settings, name, catalog)
        props = {"run_id": run_id, "source_sha256": staged[report]["sha256"]}
        start = window_start if report == "TWT49U" else d
        result = iceberg_io.publish(tbl, storage.read_parquet(staged[report]["uri"]), d, props, start)
        log.info("publish %s %s -> %s", name, d, result)
        results[name] = result.to_dict()
    return results


def build_gold(d: date, settings: Settings, run_id: str) -> dict:
    catalog = iceberg_io.get_catalog(settings)
    tables = {name: iceberg_io.get_table(settings, name, catalog) for name in (*REPORT_TABLES.values(), GOLD_TABLE)}
    q_snap = iceberg_io.current_version(tables["daily_quotes"], d)
    i_snap = iceberg_io.current_version(tables["institutional_flows"], d)
    ca_snap = iceberg_io.current_version(tables["corporate_actions"], d)
    if q_snap is None or i_snap is None:
        raise RuntimeError(f"{d}: Silver partitions missing; publish must run first")

    window_start = ca_window_start(d, settings)
    wide = gold.build(
        iceberg_io.read(tables["daily_quotes"], d),
        iceberg_io.read(tables["institutional_flows"], d),
        iceberg_io.read(tables["corporate_actions"], window_start, d),
        quotes_snapshot_id=q_snap.snapshot_id,
        institutional_snapshot_id=i_snap.snapshot_id,
        run_id=run_id,
        built_at=datetime.now(UTC),
    )
    lineage = f"gold-v{gold.GOLD_VERSION}:{q_snap.snapshot_id}:{i_snap.snapshot_id}:{ca_snap and ca_snap.snapshot_id}"
    props = {
        "run_id": run_id,
        "source_sha256": hashlib.sha256(lineage.encode()).hexdigest(),
        "quotes_snapshot_id": str(q_snap.snapshot_id),
        "institutional_snapshot_id": str(i_snap.snapshot_id),
    }
    data = wide.to_arrow().cast(contracts.GOLD_SCHEMA)
    result = iceberg_io.publish(tables[GOLD_TABLE], data, d, props)
    log.info("gold %s -> %s", d, result)
    return result.to_dict()


def reconcile(d: date, settings: Settings, run_id: str, staged: dict[str, dict], audit_report: dict) -> dict:
    """Row-count check on what is actually committed, then a manifest a strategy analyst can cite:
    'signals for D used daily_market_wide snapshot S'."""
    catalog = iceberg_io.get_catalog(settings)
    tables = {name: iceberg_io.get_table(settings, name, catalog) for name in (*REPORT_TABLES.values(), GOLD_TABLE)}
    counts, snapshots = {}, {}
    for name in ("daily_quotes", "institutional_flows", GOLD_TABLE):
        snap = iceberg_io.current_version(tables[name], d)
        snapshots[name] = snap.snapshot_id if snap else None
        counts[name] = iceberg_io.read(tables[name], d).height
    wide = iceberg_io.read(tables[GOLD_TABLE], d)
    reported = wide.filter(pl.col("institutional_reported")).height

    checks = [
        Check("silver_quotes_rows_match_staged", counts["daily_quotes"] == staged["MI_INDEX"]["rows"],
              f"silver={counts['daily_quotes']} staged={staged['MI_INDEX']['rows']}"),
        Check("silver_institutional_rows_match_staged", counts["institutional_flows"] == staged["T86"]["rows"],
              f"silver={counts['institutional_flows']} staged={staged['T86']['rows']}"),
        Check("gold_rows_equal_quotes", counts[GOLD_TABLE] == counts["daily_quotes"],
              f"gold={counts[GOLD_TABLE]} quotes={counts['daily_quotes']}"),
        Check("gold_flows_equal_institutional", reported == counts["institutional_flows"],
              f"gold rows with flows={reported} institutional={counts['institutional_flows']}"),
    ]  # fmt: skip
    manifest = {
        "trade_date": d.isoformat(),
        "run_id": run_id,
        "snapshots": snapshots,
        "row_counts": counts,
        "reconcile": [ch.to_dict() for ch in checks],
        "audit": audit_report,
        "written_at": datetime.now(UTC).isoformat(),
    }
    uri = storage.join(settings.data_uri, "manifests", f"trade_date={d}", f"{_safe(run_id)}.json")
    storage.write_bytes(uri, json.dumps(manifest, ensure_ascii=False, indent=1, default=str).encode())
    enforce(checks, trade_date=d, manifest=uri)
    log.info("reconciled %s counts=%s manifest=%s", d, counts, uri)
    return manifest


# ---- whole day (CLI / tests / demo) ---------------------------------------------------------


def run_day(d: date, settings: Settings, run_id: str | None = None, bodies: dict[str, bytes] | None = None) -> dict:
    run_id = run_id or f"manual__{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}"
    bodies = bodies or {}
    status = trading_day_status(d, settings)
    if not status.is_trading_day:
        log.info("%s skipped: %s", d, status.reason)
        return {"trade_date": d.isoformat(), "skipped": status.reason}
    if not bodies and not all(reports_ready(d, settings).values()):
        return {"trade_date": d.isoformat(), "skipped": resolve_missing_data(d, settings)}

    staged = {}
    for report in REPORT_TABLES:
        bronze = extract(report, d, settings, body=bodies.get(report))
        staged[report] = stage(report, d, bronze, settings, run_id)
    audit_report = audit(d, staged, settings)
    published = publish(d, staged, settings, run_id)
    published[GOLD_TABLE] = build_gold(d, settings, run_id)
    manifest = reconcile(d, settings, run_id, staged, audit_report)
    return {**manifest, "published": published}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="python -m twse_etl.pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run")
    run.add_argument("--date", type=date.fromisoformat, required=True)
    back = sub.add_parser("backfill")
    back.add_argument("--start", type=date.fromisoformat, required=True)
    back.add_argument("--end", type=date.fromisoformat, required=True)
    for p in (run, back):
        p.add_argument("--source", choices=["twse", "fixture"])
    args = parser.parse_args()

    settings = Settings.from_env()
    if args.source:
        settings = settings.with_overrides(source=args.source)
    days = (
        [args.date]
        if args.cmd == "run"
        else [args.start + timedelta(days=i) for i in range((args.end - args.start).days + 1)]
    )
    for d in days:
        result = run_day(d, settings)
        summary = result.get("skipped") or result["row_counts"]
        print(json.dumps({"trade_date": d.isoformat(), "result": summary}, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
