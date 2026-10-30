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
    "TWT49U": "https://www.twse.com.tw/rwd/zh/exRight/TWT49U",
    "HOLIDAY": "https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule",
}

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


def _params(report: str, d: date, start: date) -> dict[str, str]:
    ymd = d.strftime("%Y%m%d")
    return {
        "MI_INDEX": {"date": ymd, "type": "ALLBUT0999", "response": "json"},
        "T86": {"date": ymd, "selectType": "ALLBUT0999", "response": "json"},
        "TWT49U": {"startDate": start.strftime("%Y%m%d"), "endDate": ymd, "response": "json"},
        "HOLIDAY": {"date": f"{d.year}0101", "response": "json"},
    }[report]


def fixture_path(fixture_dir: Path, report: str, d: date) -> Path:
    key = str(d.year) if report == "HOLIDAY" else d.strftime("%Y%m%d")
    return fixture_dir / report / f"{key}.json.gz"


def _fixture_doc(fixture_dir: Path, report: str, d: date) -> dict:
    path = fixture_path(fixture_dir, report, d)
    if not path.exists():
        return {"stat": NO_DATA_STAT}
    return json.loads(gzip.decompress(path.read_bytes()))


def fetch(report: str, d: date, settings: Settings, start: date | None = None) -> RawPayload:
    """One HTTP GET (or fixture read). A missing fixture behaves like 'not published yet'.

    `start` only applies to TWT49U, which is fetched as a range so that events dated on a
    market closure (e.g. a typhoon day) are picked up by the next trading day.
    """
    start = start or d
    if settings.source == "fixture":
        if report == "TWT49U":  # recorded per day; a range is the union of daily files
            days = [date.fromordinal(o) for o in range(start.toordinal(), d.toordinal() + 1)]
            docs = [_fixture_doc(settings.fixture_dir, report, day) for day in days]
            data = [row for doc in docs for row in doc.get("data", [])]
            fields = next((doc["fields"] for doc in docs if doc.get("fields")), [])
            doc = {"stat": "OK", "fields": fields, "data": data} if data else {"stat": NO_DATA_STAT}
        else:
            doc = _fixture_doc(settings.fixture_dir, report, d)
        body = json.dumps(doc, ensure_ascii=False).encode()
        return RawPayload(report, d, body, f"fixture://{report}/{start}..{d}", datetime.now(UTC))

    global _last_request_at
    wait = _MIN_INTERVAL_S - (time.monotonic() - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    resp = requests.get(ENDPOINTS[report], params=_params(report, d, start), headers=_HEADERS, timeout=30)
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
