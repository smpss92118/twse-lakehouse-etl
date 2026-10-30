"""Iceberg versioning demos (PDF part 2.2): three required, two optional.

    snapshots  list snapshots: id, commit time, operation, run_id, rows added/deleted   (required)
    as-of      table content at a snapshot id or a point in time                      (required)
    correct    simulate a TWSE correction: re-ingest a day with one value changed      (required)
    rollback   return the table to the state before the last run                       (optional)
    add-column add a column mid-way; old snapshots and old rows stay queryable         (optional)

Run inside the Airflow container (same catalog the DAG writes to), e.g.
    make demo-snapshots TABLE=daily_quotes
or locally against the dev lake:
    uv run python scripts/demo_versioning.py snapshots --table daily_quotes
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
from pyiceberg.table.snapshots import Operation, ancestors_of
from pyiceberg.types import StringType

from twse_etl import adjust, iceberg_io, pipeline
from twse_etl.config import Settings

TPE = ZoneInfo("Asia/Taipei")
pl.Config.set_tbl_rows(40)
pl.Config.set_tbl_cols(12)
pl.Config.set_tbl_width_chars(180)
pl.Config.set_fmt_str_lengths(40)


def _local(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, TPE).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def snapshot_frame(tbl) -> pl.DataFrame:
    head = tbl.current_snapshot()
    lineage = {s.snapshot_id for s in ancestors_of(head, tbl.metadata)} if head else set()
    rows = []
    for snap in tbl.snapshots():
        summary = snap.summary.additional_properties if snap.summary else {}
        rows.append(
            {
                "snapshot_id": snap.snapshot_id,
                "committed_at (Asia/Taipei)": _local(snap.timestamp_ms),
                "operation": snap.summary.operation.value if snap.summary else "",
                "trade_date": summary.get("trade_date", ""),
                "run_id": summary.get("run_id", ""),
                "added": int(summary.get("added-records", 0)),
                "deleted": int(summary.get("deleted-records", 0)),
                "total": int(summary.get("total-records", 0)),
                "state": "CURRENT"
                if head and snap.snapshot_id == head.snapshot_id
                else ("in lineage" if snap.snapshot_id in lineage else "rolled back"),
            }
        )
    return pl.DataFrame(rows)


def cmd_snapshots(args, settings: Settings) -> None:
    tbl = iceberg_io.get_table(settings, args.table)
    frame = snapshot_frame(tbl)
    if args.date:
        frame = frame.filter(pl.col("trade_date") == args.date.isoformat())
    print(f"twse.{args.table}: {frame.height} snapshots (one overwrite = delete + append in one atomic commit)")
    print(frame)


def _resolve_snapshot(tbl, snapshot_id: int | None, at: str | None) -> int:
    if snapshot_id:
        sid = snapshot_id
    else:
        ts = datetime.fromisoformat(at)
        ts = ts if ts.tzinfo else ts.replace(tzinfo=TPE)
        snap = tbl.snapshot_as_of_timestamp(int(ts.timestamp() * 1000))
        if snap is None:
            raise SystemExit(f"table did not exist yet at {ts}")
        sid = snap.snapshot_id
    # An overwrite commits delete + append atomically; nobody ever read the state between the two.
    snap = tbl.snapshot_by_id(sid)
    if snap is not None and snap.summary and snap.summary.operation == Operation.DELETE:
        run_id = snap.summary.get("run_id")
        children = (s for s in tbl.snapshots() if s.parent_snapshot_id == sid)
        half = next((s for s in children if s.summary.get("run_id") == run_id), None)
        if half is not None:
            print(f"note: {sid} is the delete half of an overwrite; readers saw its append half {half.snapshot_id}")
            sid = half.snapshot_id
    return sid


def cmd_as_of(args, settings: Settings) -> None:
    tbl = iceberg_io.get_table(settings, args.table)
    sid = _resolve_snapshot(tbl, args.snapshot_id, args.at)
    df = tbl.scan(snapshot_id=sid).to_polars()
    print(
        f"twse.{args.table} as of snapshot {sid}: {df.height} rows, dates {df['trade_date'].unique().sort().to_list()}"
    )
    if args.date:
        df = df.filter(pl.col("trade_date") == args.date)
    if args.stock:
        df = df.filter(pl.col("stock_id").is_in(args.stock))
    cols = [
        c
        for c in ("trade_date", "stock_id", "stock_name", "close", "volume_shares", "total_net", "run_id")
        if c in df.columns
    ]
    print(df.select(cols).sort("trade_date", "stock_id").head(args.limit))


def cmd_correct(args, settings: Settings) -> None:
    """Re-ingest a day where TWSE 'corrected' one stock's traded volume. Same code path as a
    scheduled run; the other reports parse to identical content, so they produce no new snapshot."""
    d, stock = args.date, args.stock
    quotes_tbl = iceberg_io.get_table(settings, "daily_quotes")
    before = iceberg_io.current_version(quotes_tbl, d)

    doc = json.loads(pipeline.latest_bronze(settings, "MI_INDEX", d))
    table = next(t for t in doc["tables"] if "證券代號" in (t.get("fields") or []))
    col = table["fields"].index("成交股數")
    row = next(r for r in table["data"] if r[0].strip() == stock)
    old_value = row[col]
    row[col] = f"{args.volume:,}"
    print(f"simulated TWSE correction: {d} {stock} 成交股數 {old_value} -> {row[col]}")

    bodies = {
        "MI_INDEX": json.dumps(doc, ensure_ascii=False).encode(),
        "T86": pipeline.latest_bronze(settings, "T86", d),
        pipeline.CORPORATE_ACTIONS: pipeline.latest_bronze(settings, pipeline.CORPORATE_ACTIONS, d),
    }
    result = pipeline.run_day(d, settings, run_id=f"correction__{datetime.now(TPE):%Y%m%dT%H%M%S}", bodies=bodies)
    for name, res in result["published"].items():
        outcome = "skipped (identical content)" if res["skipped"] else f"new snapshot {res['snapshot_id']}"
        print(f"  {name:20s} {outcome}")

    quotes_tbl.refresh()
    after = iceberg_io.current_version(quotes_tbl, d)
    rows_now = iceberg_io.read(quotes_tbl, d)
    print(f"\nrows for {d}: {rows_now.height}, distinct stock_id: {rows_now['stock_id'].n_unique()}  (no duplicates)")
    for label, snap in (("before", before), ("after ", after)):
        v = iceberg_io.read(quotes_tbl, d, snapshot_id=snap.snapshot_id).filter(pl.col("stock_id") == stock)
        print(f"{label} snapshot {snap.snapshot_id}: volume_shares={v['volume_shares'][0]:,}  run_id={v['run_id'][0]}")


def _before_run(tbl, run_id: str) -> int | None:
    """Newest ancestor not written by run_id, i.e. the table as it was before that run."""
    head = tbl.current_snapshot()
    if head is None or head.summary.get("run_id") != run_id:
        return None  # that run did not change this table (identical content is skipped)
    return next((s.snapshot_id for s in ancestors_of(head, tbl.metadata) if s.summary.get("run_id") != run_id), None)


def cmd_rollback(args, settings: Settings) -> None:
    """Undo the last run everywhere it wrote, Gold included: rolling back Silver alone would leave
    the merged table the strategy team reads on the bad version."""
    if args.snapshot_id:
        if not args.table:
            raise SystemExit("--snapshot-id needs --table")
        targets = {args.table: args.snapshot_id}
    else:
        names = [args.table] if args.table else list(iceberg_io.SCHEMAS)
        anchor = iceberg_io.get_table(settings, args.table or pipeline.GOLD_TABLE).current_snapshot()
        if anchor is None:
            raise SystemExit("nothing to roll back")
        run_id = anchor.summary.get("run_id")
        print(f"rolling back run {run_id}")
        targets = {name: _before_run(iceberg_io.get_table(settings, name), run_id) for name in names}
    for name, target in targets.items():
        tbl = iceberg_io.get_table(settings, name)
        if target is None:
            print(f"  twse.{name:20s} unchanged (not written by that run, or no earlier snapshot)")
            continue
        before = tbl.current_snapshot().snapshot_id
        tbl.manage_snapshots().rollback_to_snapshot(target).commit()
        print(f"  twse.{name:20s} current snapshot {before} -> {tbl.current_snapshot().snapshot_id}")
    print("data files are untouched; rolled-back snapshots stay in history until expire_snapshots")


def cmd_adjusted(args, settings: Settings) -> None:
    """Adjusted prices from printed prices x the stored per-day factors; nothing stored is rewritten."""
    tbl = iceberg_io.get_table(settings, pipeline.GOLD_TABLE)
    wide = iceberg_io.read(tbl, args.start, args.end, snapshot_id=args.snapshot_id)
    out = adjust.adjust(wide.filter(pl.col("stock_id") == args.stock), args.method)
    version = f"snapshot {args.snapshot_id}" if args.snapshot_id else "current version"
    print(f"{args.stock} {args.start}..{args.end}, {args.method}-adjusted, {version}")
    cols = ["trade_date", "close", "change_sign", "price_change", "corporate_action", "adj_factor", "close_adj"]
    print(out.select(c for c in cols if c in out.columns))


def cmd_add_column(args, settings: Settings) -> None:
    tbl = iceberg_io.get_table(settings, args.table)
    old_snapshot = tbl.current_snapshot().snapshot_id
    if args.name not in tbl.schema().column_names:
        with tbl.update_schema() as update:
            update.add_column(args.name, StringType(), doc="added by schema-evolution demo")
    print(f"twse.{args.table} schema now: {tbl.schema().column_names[-4:]} ...")
    current = tbl.scan(limit=3).to_polars()
    print(f"current read, old rows -> {args.name} is null:")
    print(current.select("trade_date", "stock_id", args.name))
    old = tbl.scan(snapshot_id=old_snapshot, limit=3).to_polars()
    print(f"time travel to snapshot {old_snapshot} (before the change) still works; no file was rewritten,")
    print("old data files are read through the new schema, so the added column comes back as null:")
    print(old.select("trade_date", "stock_id", "close", args.name))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    tables = list(iceberg_io.SCHEMAS)

    p = sub.add_parser("snapshots")
    p.add_argument("--table", choices=tables, default="daily_quotes")
    p.add_argument("--date", type=date.fromisoformat)

    p = sub.add_parser("as-of")
    p.add_argument("--table", choices=tables, default="daily_quotes")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--snapshot-id", type=int)
    g.add_argument("--at", help="ISO time, Asia/Taipei if no offset, e.g. '2026-10-30 18:00'")
    p.add_argument("--date", type=date.fromisoformat)
    p.add_argument("--stock", nargs="*")
    p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("correct")
    p.add_argument("--date", type=date.fromisoformat, required=True)
    p.add_argument("--stock", default="2330")
    p.add_argument("--volume", type=int, default=12_345_678)

    p = sub.add_parser("rollback")
    p.add_argument("--table", choices=tables, help="default: every table the last run wrote, Gold included")
    p.add_argument("--snapshot-id", type=int)

    p = sub.add_parser("adjusted")
    p.add_argument("--stock", required=True)
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--method", choices=["backward", "forward"], default="backward")
    p.add_argument("--snapshot-id", type=int, help="read Gold as of this snapshot (point-in-time)")

    p = sub.add_parser("add-column")
    p.add_argument("--table", choices=tables, default="daily_quotes")
    p.add_argument("--name", default="data_quality_note")

    args = parser.parse_args()
    handlers = {
        "snapshots": cmd_snapshots,
        "as-of": cmd_as_of,
        "correct": cmd_correct,
        "rollback": cmd_rollback,
        "adjusted": cmd_adjusted,
        "add-column": cmd_add_column,
    }
    handlers[args.cmd](args, Settings.from_env())


if __name__ == "__main__":
    main()
