"""Runtime settings, resolved from environment variables only.

Local Docker, CI and AWS differ only in these values; no code path branches on the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    data_uri: str  # Bronze raw JSON, staging parquet, run manifests (local path or s3://)
    warehouse_uri: str  # Iceberg warehouse (local path or s3://)
    catalog_type: str  # "sql" (SQLite/Postgres) locally, "glue" on AWS
    catalog_uri: str  # SQLAlchemy URI, used when catalog_type == "sql"
    source: str  # "twse" = live HTTP, "fixture" = recorded raw JSON
    fixture_dir: Path
    vendor_dir: Path  # third-party corporate actions (FinMind JSON export)
    namespace: str = "twse"

    @classmethod
    def from_env(cls) -> Settings:
        home = Path(os.environ.get("TWSE_ETL_HOME", REPO_ROOT / ".local" / "dev")).resolve()
        sqlite_default = f"sqlite:///{home / 'warehouse' / 'catalog.db'}"
        return cls(
            data_uri=os.environ.get("TWSE_DATA_URI", str(home / "data")),
            warehouse_uri=os.environ.get("ICEBERG_WAREHOUSE", str(home / "warehouse")),
            catalog_type=os.environ.get("ICEBERG_CATALOG_TYPE", "sql"),
            catalog_uri=os.environ.get("ICEBERG_CATALOG_URI", sqlite_default),
            source=os.environ.get("TWSE_SOURCE", "twse"),
            fixture_dir=Path(os.environ.get("TWSE_FIXTURE_DIR", REPO_ROOT / "fixtures" / "twse")),
            vendor_dir=Path(os.environ.get("VENDOR_CORP_ACTIONS_DIR", REPO_ROOT / "fixtures" / "vendor")),
        )

    def with_overrides(self, **changes) -> Settings:
        return replace(self, **changes)
