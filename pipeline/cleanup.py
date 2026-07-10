"""Retention pruning: delete bronze/silver date partitions older than the
configured windows so the pipeline can run indefinitely on bounded disk.

Retention is deliberately wall-clock-relative (not data-interval-relative):
disk is a wall-clock resource, and pruning during a backfill of old dates
must not delete the very partitions the backfill just wrote relative to
their own (old) logical dates.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from pipeline.config import Config, get_config

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PruneResult:
    removed: tuple[str, ...]


def prune_retention(cfg: Config | None = None, today: date | None = None) -> PruneResult:
    cfg = cfg or get_config()
    today = today or datetime.now(timezone.utc).date()
    removed: list[str] = []

    for base, retention_days in (
        (cfg.bronze_dir, cfg.bronze_retention_days),
        (cfg.silver_events_dir, cfg.silver_retention_days),
    ):
        if not base.exists():
            continue
        cutoff = today - timedelta(days=retention_days)
        for partition in sorted(base.glob("date=*")):
            try:
                partition_date = date.fromisoformat(partition.name.removeprefix("date="))
            except ValueError:
                log.warning("skipping unrecognized partition dir: %s", partition)
                continue
            if partition_date < cutoff:
                shutil.rmtree(partition)
                removed.append(str(partition))

    log.info("retention pruned %d partitions", len(removed))
    return PruneResult(removed=tuple(removed))
