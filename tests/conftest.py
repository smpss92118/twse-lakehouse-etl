from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from twse_etl import pipeline, sources
from twse_etl.config import REPO_ROOT, Settings

FIXTURES = REPO_ROOT / "fixtures" / "twse"


def load_fixture(report: str, d: date) -> dict:
    return json.loads(gzip.decompress(sources.fixture_path(FIXTURES, report, d).read_bytes()))


def make_settings(home: Path) -> Settings:
    return Settings(
        data_uri=str(home / "data"),
        warehouse_uri=str(home / "warehouse"),
        catalog_type="sql",
        catalog_uri=f"sqlite:///{home / 'warehouse' / 'catalog.db'}",
        source="fixture",
        fixture_dir=FIXTURES,
    )


def staged_actions(home: Path, end: date, start: date | None = None) -> pl.DataFrame:
    """Corporate actions over [start, end] exactly as the pipeline stages them, from fixtures."""
    settings = make_settings(home)
    bundle = json.loads(pipeline.extract_corporate_actions(end, settings, start=start))
    return pipeline.stage_corporate_actions(bundle)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)
