"""Iceberg catalog access and the single write path: replace one trade_date partition.

PyIceberg commits an overwrite as two snapshots (delete, then append) inside one atomic metadata
commit, so readers of the current table never see a half-replaced day. The append snapshot carries
run_id / trade_date / source_sha256, which is what versioning queries key on.
"""

from __future__ import annotations

from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.expressions import And, BooleanExpression, EqualTo, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Snapshot, ancestors_of

from twse_etl import contracts as c
from twse_etl.config import Settings

SCHEMAS = {
    "daily_quotes": c.QUOTES_SCHEMA,
    "institutional_flows": c.INSTITUTIONAL_SCHEMA,
    "corporate_actions": c.CORPORATE_ACTIONS_SCHEMA,
    "daily_market_wide": c.GOLD_SCHEMA,
}


@dataclass(frozen=True)
class PublishResult:
    table: str
    snapshot_id: int | None
    rows: int
    skipped: bool

    def to_dict(self) -> dict:
        return asdict(self)


def _as_uri(location: str) -> str:
    return location if "://" in location else Path(location).resolve().as_uri()


def get_catalog(settings: Settings) -> Catalog:
    """The only place that knows where the catalog lives. AWS: ICEBERG_CATALOG_TYPE=glue."""
    if settings.catalog_type == "glue":
        return load_catalog("twse", type="glue", warehouse=settings.warehouse_uri)
    if settings.catalog_uri.startswith("sqlite:///"):
        Path(settings.catalog_uri.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("twse", uri=settings.catalog_uri, warehouse=_as_uri(settings.warehouse_uri))


def get_table(settings: Settings, name: str, catalog: Catalog | None = None) -> Table:
    catalog = catalog or get_catalog(settings)
    ident = f"{settings.namespace}.{name}"
    catalog.create_namespace_if_not_exists(settings.namespace)
    if not catalog.table_exists(ident):
        with suppress(TableAlreadyExistsError):  # another task created it concurrently
            with catalog.create_table_transaction(ident, schema=SCHEMAS[name]) as txn:
                with txn.update_spec() as spec:
                    spec.add_identity("trade_date")
    return catalog.load_table(ident)


def date_filter(start: date, end: date | None = None) -> BooleanExpression:
    end = end or start
    if start == end:
        return EqualTo("trade_date", start.isoformat())
    return And(GreaterThanOrEqual("trade_date", start.isoformat()), LessThanOrEqual("trade_date", end.isoformat()))


def current_version(tbl: Table, trade_date: date) -> Snapshot | None:
    """Latest append snapshot, in the current lineage, written by a run for this trade_date."""
    head = tbl.current_snapshot()
    if head is None:
        return None
    for snap in ancestors_of(head, tbl.metadata):
        summary = snap.summary
        if summary and summary.operation == Operation.APPEND and summary.get("trade_date") == trade_date.isoformat():
            return snap
    return None


def publish(
    tbl: Table,
    data: pa.Table,
    trade_date: date,
    props: dict[str, str],
    window_start: date | None = None,
) -> PublishResult:
    """Idempotent replace of [window_start, trade_date]. Re-publishing identical source bytes is a
    no-op (no new snapshot); different bytes (a TWSE correction) produce a new snapshot."""
    name = tbl.name()[-1]
    prior = current_version(tbl, trade_date)
    if prior is not None and prior.summary.get("source_sha256") == props["source_sha256"]:
        return PublishResult(name, prior.snapshot_id, data.num_rows, skipped=True)

    row_filter = date_filter(window_start or trade_date, trade_date)
    snapshot_props = {**props, "trade_date": trade_date.isoformat()}
    if data.num_rows == 0:
        tbl.delete(row_filter, snapshot_properties=snapshot_props)
    else:
        tbl.overwrite(data, overwrite_filter=row_filter, snapshot_properties=snapshot_props)
    head = tbl.current_snapshot()
    return PublishResult(name, head.snapshot_id if head else None, data.num_rows, skipped=False)


def read(tbl: Table, start: date, end: date | None = None, snapshot_id: int | None = None) -> pl.DataFrame:
    if tbl.current_snapshot() is None:
        return pl.from_arrow(tbl.schema().as_arrow().empty_table())
    return tbl.scan(row_filter=date_filter(start, end), snapshot_id=snapshot_id).to_polars()


def latest_date_before(tbl: Table, d: date) -> date | None:
    """Metadata-only lookup of the previous loaded trading day."""
    if tbl.current_snapshot() is None:
        return None
    dates = [row["partition"]["trade_date"] for row in tbl.inspect.partitions().to_pylist()]
    earlier = [x for x in dates if x is not None and x < d]
    return max(earlier) if earlier else None
