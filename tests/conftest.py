from __future__ import annotations

import gzip
import json
import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from twse_etl import pipeline, sources
from twse_etl.config import REPO_ROOT, Settings

FIXTURES = REPO_ROOT / "fixtures" / "twse"
FAKE_LLM = f"{sys.executable} {REPO_ROOT / 'tests' / 'fakes' / 'fake_holiday_llm.py'}"
TYPHOON_VERDICT = {
    "is_holiday": True,
    "reason": "薔琵颱風，臺北市停止上班，證交所全日休市",
    "evidence_url": "https://www.cna.com.tw/news/afe/202610220388.aspx",
}


def load_fixture(report: str, d: date, key: str | None = None) -> dict:
    return json.loads(gzip.decompress(sources.fixture_path(FIXTURES, report, d, key).read_bytes()))


def make_settings(home: Path) -> Settings:
    return Settings(
        data_uri=str(home / "data"),
        warehouse_uri=str(home / "warehouse"),
        catalog_type="sql",
        catalog_uri=f"sqlite:///{home / 'warehouse' / 'catalog.db'}",
        source="fixture",
        fixture_dir=FIXTURES,
        vendor_dir=REPO_ROOT / "fixtures" / "vendor",
    )


def staged_actions(home: Path, end: date, start: date | None = None) -> pl.DataFrame:
    """Corporate actions over [start, end] exactly as the pipeline stages them, from fixtures."""
    settings = make_settings(home)
    bundle = json.loads(pipeline.extract_corporate_actions(end, settings, start=start))
    return pipeline.stage_corporate_actions(bundle, end, settings)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
def fake_llm(monkeypatch: pytest.MonkeyPatch):
    """Point HOLIDAY_LLM_CMD at the fake; returns a setter for its verdict / exit code."""
    monkeypatch.setenv("HOLIDAY_LLM_CMD", FAKE_LLM)

    def set_verdict(verdict: dict | str | None, exit_code: int = 0) -> None:
        text = verdict if isinstance(verdict, str) else json.dumps(verdict or {}, ensure_ascii=False)
        monkeypatch.setenv("FAKE_LLM_VERDICT", text)
        monkeypatch.setenv("FAKE_LLM_EXIT", str(exit_code))

    return set_verdict
