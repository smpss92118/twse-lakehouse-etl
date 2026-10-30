"""Iceberg catalog access and the single write path: replace one trade_date partition.

PyIceberg commits an overwrite as two snapshots (delete, then append) inside one atomic metadata
commit, so readers of the current table never see a half-replaced day. Both snapshots carry
run_id / trade_date / content_sha256, which is what versioning queries key on.
"""

from __future__ import annotations

import hashlib
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.expressions import And, BooleanExpression, EqualTo, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Snapshot, ancestors_of

from twse_etl import contracts as c
from twse_etl.config import Settings

SCHEMAS = {
    "daily_quotes": c.QUOTES_SCHEMA,
    "institutional_flows": c.INSTITUTIONAL_SCHEMA,
    "corporate_actions": c.CORPORATE_ACTIONS_SCHEMA,
    "daily_market_wide": c.GOLD_SCHEMA,
    "review_decisions": c.REVIEW_DECISIONS_SCHEMA,
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
    """The only place that knows where the catalog lives. AWS: ICEBERG_CATALOG_TYPE=glue.

    load_catalog also merges PYICEBERG_CATALOG__TWSE__* environment variables, so object-store
    settings (e.g. PYICEBERG_CATALOG__TWSE__S3__ENDPOINT) are configuration, not code."""
    if settings.catalog_type == "glue":
        return load_catalog("twse", type="glue", warehouse=settings.warehouse_uri)
    if settings.catalog_uri.startswith("sqlite:///"):
        Path(settings.catalog_uri.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    return load_catalog("twse", type="sql", uri=settings.catalog_uri, warehouse=_as_uri(settings.warehouse_uri))


def get_table(settings: Settings, name: str, catalog: Catalog | None = None) -> Table:
    catalog = catalog or get_catalog(settings)
    ident = f"{settings.namespace}.{name}"
    catalog.create_namespace_if_not_exists(settings.namespace)
    if not catalog.table_exists(ident):
        with suppress(TableAlreadyExistsError):  # another task created it concurrently
            with catalog.create_table_transaction(ident, schema=SCHEMAS[name]) as txn:
                with txn.update_spec() as spec:
                    spec.add_identity("trade_date")
    tbl = catalog.load_table(ident)
    if set(SCHEMAS[name].names) - set(tbl.schema().column_names):
        # Schema evolution: columns added to the contract are appended as optional columns.
        # Old data files are not rewritten; their rows read the new columns as null.
        with tbl.update_schema() as update:
            update.union_by_name(SCHEMAS[name])
    return tbl


def date_filter(start: date, end: date | None = None) -> BooleanExpression:
    end = end or start
    if start == end:
        return EqualTo("trade_date", start.isoformat())
    return And(GreaterThanOrEqual("trade_date", start.isoformat()), LessThanOrEqual("trade_date", end.isoformat()))


def current_version(tbl: Table, trade_date: date) -> Snapshot | None:
    """Latest snapshot, in the current lineage, written by a run for this trade_date: the append half
    of an overwrite, or the delete when the day was published empty."""
    head = tbl.current_snapshot()
    if head is None:
        return None
    for snap in ancestors_of(head, tbl.metadata):
        if snap.summary and snap.summary.get("trade_date") == trade_date.isoformat():
            return snap
    return None


# Per-run bookkeeping: differs on every run even when the data is identical.
VOLATILE_COLUMNS = ("run_id", "ingested_at", "built_at", "source_sha256")


def content_fingerprint(data: pa.Table) -> str:
    """sha256 of what a reader would see, ignoring row order and bookkeeping columns."""
    df = pl.from_arrow(data.drop_columns([col for col in VOLATILE_COLUMNS if col in data.column_names]))
    df = df.sort(df.columns, nulls_last=True)
    return hashlib.sha256(df.write_csv().encode()).hexdigest()


def publish(
    tbl: Table,
    data: pa.Table,
    trade_date: date,
    props: dict[str, str],
    window_start: date | None = None,
) -> PublishResult:
    """Idempotent replace of [window_start, trade_date], keyed on the content, not the source bytes:
    re-publishing identical content is a no-op (no new snapshot). Anything that changes the content
    gets a new snapshot, whether TWSE corrected the data or we fixed the parser after a bad load."""
    name = tbl.name()[-1]
    fingerprint = content_fingerprint(data)
    prior = current_version(tbl, trade_date)
    if prior is not None and prior.summary.get("content_sha256") == fingerprint:
        return PublishResult(name, prior.snapshot_id, data.num_rows, skipped=True)

    row_filter = date_filter(window_start or trade_date, trade_date)
    snapshot_props = {**props, "trade_date": trade_date.isoformat(), "content_sha256": fingerprint}
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


def read_all(tbl: Table) -> pl.DataFrame:
    if tbl.current_snapshot() is None:
        return pl.from_arrow(tbl.schema().as_arrow().empty_table())
    return tbl.scan().to_polars()


def loaded_dates(tbl: Table) -> list[date]:
    """Metadata-only: the trade_date partitions that currently hold data."""
    if tbl.current_snapshot() is None:
        return []
    return sorted(row["partition"]["trade_date"] for row in tbl.inspect.partitions().to_pylist())


def latest_date_before(tbl: Table, d: date) -> date | None:
    """Metadata-only lookup of the previous loaded trading day."""
    earlier = [x for x in loaded_dates(tbl) if x < d]
    return max(earlier) if earlier else None
