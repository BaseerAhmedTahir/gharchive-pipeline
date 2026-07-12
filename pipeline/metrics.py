"""Per-run pipeline metrics, one small JSON file per (kind, hour).

One file per record keeps the write atomic (tmp + os.replace) and re-runs
idempotent — a task run overwrites its own record instead of appending a
duplicate. DuckDB reads the whole history in one read_json glob. At ~75
records/day this stays trivially small; the dashboard's ops page is the
consumer.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import Config, get_config

log = logging.getLogger(__name__)

METRICS_GLOB = "date=*/*.json"


def record(kind: str, hour_dt: datetime, cfg: Config | None = None, **fields) -> Path:
    """Write one metrics record for a task run. Idempotent per (kind, hour)."""
    cfg = cfg or get_config()
    day_dir = cfg.metrics_dir / f"date={hour_dt:%Y-%m-%d}"
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / f"{kind}_hour={hour_dt:%H}.json"

    payload = {
        "kind": kind,
        "hour": hour_dt.isoformat(),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, path)
    log.info("recorded metrics %s for %s", kind, hour_dt)
    return path
