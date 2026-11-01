"""Day vs year partitioning at this pipeline's volume (README, "Partition").

~1,390 rows per trading day, written the way the pipeline writes: one overwrite(trade_date == d)
commit per day. Then a year-partitioned copy is compacted (one file per year), which is what Trino's
`ALTER TABLE ... EXECUTE optimize` does; PyIceberg 0.12 has no compaction action of its own.

Writing through a year() transform needs pyiceberg-core, which the pipeline itself does not use:

    uv run --with "pyiceberg[pyiceberg-core]" python scripts/bench_partitions.py 500
"""

from __future__ import annotations

import json
import random
import shutil
import statistics
import sys
import time
from datetime import date, timedelta
from functools import partial

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform, YearTransform
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType

from twse_etl.config import REPO_ROOT

STOCKS = 1390
WEEKEND = 5
VALUES = ["open", "high", "low", "close", "change", "adj_factor", "foreign_net", "trust_net", "dealer_net"]
SCHEMA = Schema(
    NestedField(1, "trade_date", DateType(), required=False),
    NestedField(2, "stock_id", StringType(), required=False),
    NestedField(3, "volume", LongType(), required=False),
    *[NestedField(4 + i, name, DoubleType(), required=False) for i, name in enumerate(VALUES)],
)


def trading_days(n: int) -> list[date]:
    days, d = [], date(2024, 1, 2)
    while len(days) < n:
        if d.weekday() < WEEKEND:
            days.append(d)
        d += timedelta(days=1)
    return days


def one_day(rng: random.Random, day: date) -> pa.Table:
    ids = [f"{i:04d}" for i in range(1101, 1101 + STOCKS)]
    data = {
        "trade_date": pa.array([day] * STOCKS, pa.date32()),
        "stock_id": pa.array(ids),
        "volume": pa.array([rng.randrange(10**8) for _ in ids], pa.int64()),
    }
    data |= {name: pa.array([round(rng.uniform(10, 1000), 2) for _ in ids]) for name in VALUES}
    return pa.table(data)


def scan_rows(catalog: Catalog, ident: str, row_filter) -> int:
    tbl = catalog.load_table(ident)  # loaded fresh: reading metadata and manifests is part of every query
    scan = tbl.scan(row_filter=row_filter) if row_filter is not None else tbl.scan()
    return scan.to_arrow().num_rows


def best_ms(read, reps: int = 3) -> float:
    times = []
    for _ in range(reps):
        start = time.perf_counter()
        read()
        times.append(time.perf_counter() - start)
    return round(min(times) * 1000, 1)


def main(n_days: int) -> None:
    root = REPO_ROOT / ".local" / "bench"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    catalog = load_catalog("bench", type="sql", uri=f"sqlite:///{root}/catalog.db", warehouse=root.as_uri())
    catalog.create_namespace("b")
    rng, days = random.Random(0), trading_days(n_days)

    def create(name: str, transform) -> Table:
        tbl = catalog.create_table(f"b.{name}", schema=SCHEMA)
        with tbl.update_spec() as spec:
            spec.add_field("trade_date", transform, "p")
        return catalog.load_table(f"b.{name}")

    commit_ms = {}
    for name, transform in (("by_day", IdentityTransform()), ("by_year", YearTransform())):
        tbl, lat = create(name, transform), []
        for day in days:
            start = time.perf_counter()
            tbl.overwrite(one_day(rng, day), overwrite_filter=EqualTo("trade_date", day))
            lat.append((time.perf_counter() - start) * 1000)
        commit_ms[name] = (round(statistics.mean(lat[:10]), 1), round(statistics.mean(lat[-10:]), 1))

    rows = catalog.load_table("b.by_year").scan().to_arrow()
    create("by_year_compacted", YearTransform()).append(rows.sort_by([("stock_id", "ascending")]))

    window = And(GreaterThanOrEqual("trade_date", days[-min(60, n_days)]), LessThanOrEqual("trade_date", days[-1]))
    filters = {
        "all rows (backtest)": None,
        "one stock, all history": EqualTo("stock_id", "2330"),
        "one day": EqualTo("trade_date", days[n_days // 2]),
        "last 60 days": window,
    }
    report = {"trading_days": n_days, "rows_per_day": STOCKS, "commit_ms_first10_last10": commit_ms}
    for name in ("by_day", "by_year", "by_year_compacted"):
        tbl = catalog.load_table(f"b.{name}")
        sizes = tbl.inspect.files()["file_size_in_bytes"].to_pylist()
        reads = {label: best_ms(partial(scan_rows, catalog, f"b.{name}", f)) for label, f in filters.items()}
        report[name] = {
            "files": len(sizes),
            "manifests": len(tbl.current_snapshot().manifests(tbl.io)),
            "median_file_kb": round(statistics.median(sizes) / 1024, 1),
            "read_ms": reads,
        }
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 500)
