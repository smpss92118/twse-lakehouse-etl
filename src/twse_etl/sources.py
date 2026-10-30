"""TWSE endpoints, raw payload capture and the append-only Bronze layer."""

from __future__ import annotations

import gzip
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import requests

from twse_etl import storage
from twse_etl.config import Settings

NO_DATA_STAT = "很抱歉，沒有符合條件的資料!"

ENDPOINTS = {
    "MI_INDEX": "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
    "T86": "https://www.twse.com.tw/rwd/zh/fund/T86",
    "TWT49U": "https://www.twse.com.tw/rwd/zh/exRight/TWT49U",  # ex-rights / ex-dividend results
    "TWT49UDetail": "https://www.twse.com.tw/rwd/zh/exRight/TWT49UDetail",  # per-event dividend terms
    "TWTAUU": "https://www.twse.com.tw/rwd/zh/reducation/TWTAUU",  # capital reduction resumption prices
    "TWTAVUDetail": "https://www.twse.com.tw/rwd/zh/reducation/TWTAVUDetail",  # per-event reduction terms
    "TWTCAU": "https://www.twse.com.tw/rwd/zh/split/TWTCAU",  # ETF split / reverse split resumption prices
    "HOLIDAY": "https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule",
}

# Corporate-action tables, fetched as a date range so events dated on a closure day are kept.
RANGE_REPORTS = ("TWT49U", "TWTAUU", "TWTCAU")
# Per-event terms (cash, share ratios, subscription) behind TWT49U and TWTAUU rows, keyed by the
# row's 詳細資料 cell "<stock_id>,<yyyymmdd>".
DETAIL_REPORTS = {"TWT49U": "TWT49UDetail", "TWTAUU": "TWTAVUDetail"}

_HEADERS = {"User-Agent": "Mozilla/5.0 (twse-etl; +https://github.com)", "Accept": "application/json"}
_MIN_INTERVAL_S = 3.0  # TWSE blocks bursts of more than ~3 requests per 5 seconds
_last_request_at = 0.0


@dataclass(frozen=True)
class RawPayload:
    report: str
    trade_date: date
    body: bytes
    source_url: str
    fetched_at: datetime

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()

    def json(self) -> dict:
        return json.loads(self.body)


def detail_keys(doc: dict) -> list[str]:
    """'<stock_id>,<yyyymmdd>' keys of the per-event detail pages linked from a range report."""
    if doc.get("stat") != "OK" or "詳細資料" not in (doc.get("fields") or []):
        return []
    col = doc["fields"].index("詳細資料")
    return [",".join(part.strip() for part in row[col].split(",")) for row in doc.get("data", [])]


def _params(report: str, d: date, start: date, key: str | None) -> dict[str, str]:
    ymd = d.strftime("%Y%m%d")
    if report in RANGE_REPORTS:
        return {"startDate": start.strftime("%Y%m%d"), "endDate": ymd, "response": "json"}
    if report in DETAIL_REPORTS.values():
        stock, day = key.split(",")
        return {"STK_NO": stock, "T1" if report == "TWT49UDetail" else "FILE_DATE": day, "response": "json"}
    return {
        "MI_INDEX": {"date": ymd, "type": "ALLBUT0999", "response": "json"},
        "T86": {"date": ymd, "selectType": "ALLBUT0999", "response": "json"},
        "HOLIDAY": {"date": f"{d.year}0101", "response": "json"},
    }[report]


def fixture_path(fixture_dir: Path, report: str, d: date, key: str | None = None) -> Path:
    if key:
        return fixture_dir / report / f"{key.replace(',', '_')}.json.gz"
    name = str(d.year) if report == "HOLIDAY" else d.strftime("%Y%m%d")
    return fixture_dir / report / f"{name}.json.gz"


def _fixture_doc(fixture_dir: Path, report: str, d: date, key: str | None = None) -> dict:
    path = fixture_path(fixture_dir, report, d, key)
    if not path.exists():
        return {"stat": NO_DATA_STAT}
    return json.loads(gzip.decompress(path.read_bytes()))


def fetch(report: str, d: date, settings: Settings, start: date | None = None, key: str | None = None) -> RawPayload:
    """One HTTP GET (or fixture read). A missing fixture behaves like 'not published yet'.

    `start` only applies to RANGE_REPORTS, fetched as a range so that events dated on a market
    closure (e.g. a typhoon day) are picked up by the next trading day. `key` selects one event of
    a DETAIL_REPORTS page.
    """
    start = start or d
    if settings.source == "fixture":
        if report in RANGE_REPORTS:  # recorded per day; a range is the union of daily files
            days = [date.fromordinal(o) for o in range(start.toordinal(), d.toordinal() + 1)]
            docs = [_fixture_doc(settings.fixture_dir, report, day) for day in days]
            data = [row for doc in docs for row in doc.get("data", [])]
            fields = next((doc["fields"] for doc in docs if doc.get("fields")), [])
            doc = {"stat": "OK", "fields": fields, "data": data} if data else {"stat": NO_DATA_STAT}
        else:
            doc = _fixture_doc(settings.fixture_dir, report, d, key)
        body = json.dumps(doc, ensure_ascii=False).encode()
        return RawPayload(report, d, body, f"fixture://{report}/{key or f'{start}..{d}'}", datetime.now(UTC))

    global _last_request_at
    wait = _MIN_INTERVAL_S - (time.monotonic() - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    resp = requests.get(ENDPOINTS[report], params=_params(report, d, start, key), headers=_HEADERS, timeout=30)
    _last_request_at = time.monotonic()
    resp.raise_for_status()
    return RawPayload(report, d, resp.content, resp.url, datetime.now(UTC))


def land_bronze(payload: RawPayload, settings: Settings) -> str:
    """Append-only: every fetch gets its own file, so a TWSE correction never overwrites history."""
    name = f"{payload.fetched_at:%Y%m%dT%H%M%S%fZ}_{payload.sha256[:12]}.json"
    uri = storage.join(settings.data_uri, "bronze", payload.report, f"trade_date={payload.trade_date}", name)
    storage.write_bytes(uri, payload.body)
    meta = {
        "report": payload.report,
        "trade_date": payload.trade_date.isoformat(),
        "source_url": payload.source_url,
        "fetched_at": payload.fetched_at.isoformat(),
        "sha256": payload.sha256,
    }
    storage.write_bytes(uri + ".meta.json", json.dumps(meta, ensure_ascii=False).encode())
    return uri


def load_bronze(uri: str) -> RawPayload:
    meta = json.loads(storage.read_bytes(uri + ".meta.json"))
    return RawPayload(
        report=meta["report"],
        trade_date=date.fromisoformat(meta["trade_date"]),
        body=storage.read_bytes(uri),
        source_url=meta["source_url"],
        fetched_at=datetime.fromisoformat(meta["fetched_at"]),
    )
