"""Data-quality gate for a silver hour, run between transform and aggregate.

Hard checks fail the run (raise QualityGateError) because letting the data
through would poison the gold layer:
- silver partition exists and has the expected columns
- row count >= configured floor (set from measured baselines, ~160k/hour)
- no NULL event ids
- no duplicate event ids (the transform preserves same-id collisions on
  purpose so they surface HERE, loudly, instead of being silently resolved)

Soft checks only log a warning and are recorded in the report, because they
reflect known upstream quirks, not pipeline defects:
- event created_at outside the file's hour +/- tolerance (GH Archive names
  files by *collection* hour; events routinely spill across the boundary)
- NULL created_at (malformed timestamp TRY_CASTed to NULL in the transform)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from pipeline.config import Config, get_config, hour_partition
from pipeline.transform import SILVER_COLUMNS, SILVER_FILENAME, connect

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class QualityReport:
    hour: datetime
    rows: int
    hard_failures: tuple[str, ...]
    soft_warnings: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.hard_failures


class QualityGateError(Exception):
    def __init__(self, report: QualityReport):
        self.report = report
        super().__init__(
            f"quality gate failed for {report.hour:%Y-%m-%d %H}:00 — "
            + "; ".join(report.hard_failures)
        )


def check_hour(hour_dt: datetime, cfg: Config | None = None) -> QualityReport:
    """Validate one silver hour. Raises QualityGateError on hard failure."""
    cfg = cfg or get_config()
    silver = hour_partition(cfg.silver_events_dir, hour_dt) / SILVER_FILENAME

    hard: list[str] = []
    soft: list[str] = []
    rows = 0

    if not silver.exists():
        hard.append(f"silver partition missing: {silver}")
    else:
        con = connect(cfg)
        try:
            names = {
                r[0]
                for r in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{silver.as_posix()}')"
                ).fetchall()
            }
            missing = set(SILVER_COLUMNS) - names
            if missing:
                hard.append(f"missing columns: {sorted(missing)}")

            tolerance = timedelta(minutes=cfg.quality_ts_tolerance_minutes)
            window_start = hour_dt - tolerance
            window_end = hour_dt + timedelta(hours=1) + tolerance
            rows, null_ids, dup_ids, null_ts, out_of_window = con.execute(
                f"""
                SELECT
                    count(*),
                    count(*) FILTER (id IS NULL),
                    count(*) - count(DISTINCT id),
                    count(*) FILTER (created_at IS NULL),
                    count(*) FILTER (
                        created_at IS NOT NULL
                        AND (created_at < ? OR created_at >= ?)
                    )
                FROM read_parquet('{silver.as_posix()}')
                """,
                [window_start, window_end],
            ).fetchone()
        finally:
            con.close()

        if rows < cfg.quality_min_rows:
            hard.append(f"row count {rows} below floor {cfg.quality_min_rows}")
        if null_ids:
            hard.append(f"{null_ids} NULL event ids")
        if dup_ids:
            hard.append(
                f"{dup_ids} duplicate event ids (same-id collision preserved by "
                "transform — investigate before letting this hour through)"
            )
        if null_ts:
            soft.append(f"{null_ts} NULL created_at (malformed upstream timestamps)")
        if out_of_window:
            soft.append(
                f"{out_of_window} events outside collection hour ±"
                f"{cfg.quality_ts_tolerance_minutes}min (known GH Archive boundary spill)"
            )

    report = QualityReport(
        hour=hour_dt, rows=rows, hard_failures=tuple(hard), soft_warnings=tuple(soft)
    )
    for warning in report.soft_warnings:
        log.warning("quality soft check, %s: %s", hour_dt, warning)
    if not report.passed:
        raise QualityGateError(report)
    log.info("quality gate passed for %s: %d rows, %d warnings", hour_dt, rows, len(soft))
    return report
