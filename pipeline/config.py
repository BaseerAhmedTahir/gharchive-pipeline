"""Central configuration for the pipeline.

Every path derives from a single DATA_ROOT so the same code runs on the
Windows host and inside Airflow containers — only the DATA_ROOT env var
changes between the two.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Config:
    data_root: Path

    # Retention: silver must exceed the largest aggregation window (7-day
    # trailing average in the trending mart); bronze only needs to cover
    # plausible reprocessing of recent hours.
    bronze_retention_days: int = 3
    silver_retention_days: int = 10

    gharchive_url_template: str = "https://data.gharchive.org/{date:%Y-%m-%d}-{hour}.json.gz"

    http_timeout_seconds: float = 60.0
    http_max_attempts: int = 4
    http_retry_initial_wait_seconds: float = 1.0
    download_chunk_bytes: int = 1 << 20

    # Explicit DuckDB session limits so a big hour spills to disk (via
    # temp_directory) instead of OOMing the host or the Airflow container.
    duckdb_memory_limit: str = "2GB"
    duckdb_threads: int = 4
    # The Parquet writer buffers one full row group in memory, so this bounds
    # writer memory at roughly row_group_size * row_width. 20k keeps that in
    # the tens of MB even for wide rows while staying large enough for
    # efficient scans at our per-hour volumes.
    parquet_row_group_size: int = 20_000

    @property
    def bronze_dir(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_root / "gold"

    @property
    def silver_events_dir(self) -> Path:
        return self.silver_dir / "events"

    @property
    def duckdb_tmp_dir(self) -> Path:
        return self.data_root / ".duckdb_tmp"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            data_root=Path(os.environ.get("DATA_ROOT", str(_PROJECT_ROOT / "data"))),
            bronze_retention_days=int(os.environ.get("BRONZE_RETENTION_DAYS", "3")),
            silver_retention_days=int(os.environ.get("SILVER_RETENTION_DAYS", "10")),
            duckdb_memory_limit=os.environ.get("DUCKDB_MEMORY_LIMIT", "2GB"),
            duckdb_threads=int(os.environ.get("DUCKDB_THREADS", "4")),
        )


def get_config() -> Config:
    return Config.from_env()


def hour_partition(base: Path, hour_dt: datetime) -> Path:
    """Hive-style partition directory for one hour: date=YYYY-MM-DD/hour=HH."""
    return base / f"date={hour_dt:%Y-%m-%d}" / f"hour={hour_dt:%H}"


def gharchive_url(cfg: Config, hour_dt: datetime) -> str:
    # GH Archive hours are NOT zero-padded in the URL: ...-2026-07-09-5.json.gz
    return cfg.gharchive_url_template.format(date=hour_dt, hour=hour_dt.hour)
