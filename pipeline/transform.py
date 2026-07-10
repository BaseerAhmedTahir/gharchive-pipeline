"""Transform one bronze hour into a typed, deduplicated silver Parquet partition.

Design notes:
- DuckDB session limits (memory_limit, threads, temp_directory) are set
  explicitly so an oversized hour spills to disk instead of OOMing the host
  or the Airflow container.
- The raw JSON is read with an explicit column spec: fields missing from an
  event become NULL and unknown fields are ignored, so GH Archive schema
  drift degrades gracefully instead of failing the run. Payload fields are
  TRY_CASTed — one malformed event must not kill the hour (the stage-3
  quality gate decides what's tolerable).
- Two-phase execution, discovered empirically under low memory limits:
  DuckDB 1.5's window operator (QUALIFY row_number()) materializes all
  partitions in memory and cannot spill, and blocking operators sharing a
  pipeline with the gzip JSON reader OOM before spilling engages. Streaming
  JSON->Parquet, then DISTINCT from Parquet, keeps every phase spillable
  (and parses the JSON only once).
- Dedup semantics: DISTINCT removes exact duplicates (what GH Archive
  actually emits); a same-id-different-content collision is deliberately
  preserved so the quality gate can fail loudly instead of us silently
  discarding data.
- Overwrite-partition atomicity: COPY into `hour=HH.tmp`, remove any old
  partition, then os.replace the tmp dir onto the final name. A crash leaves
  either the previous partition or none — never a partial one — so
  re-running the hour always heals.
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from pipeline.config import Config, get_config, hour_partition
from pipeline.ingest import BRONZE_FILENAME

log = logging.getLogger(__name__)

SILVER_FILENAME = "events.parquet"

# Single source of truth for the silver schema (quality gate checks against
# this; keep in sync with _PROJECTION below).
SILVER_COLUMNS = (
    "id",
    "type",
    "created_at",
    "public",
    "actor_login",
    "repo_id",
    "repo_name",
    "org_login",
    "payload_action",
    "pr_number",
    "pr_merged_at",
    "push_commits",
)


class BronzeNotFoundError(Exception):
    """The bronze partition for this hour hasn't been ingested."""


@dataclass(frozen=True)
class TransformResult:
    path: Path
    rows_in: int
    rows_out: int
    bytes_out: int
    duration_seconds: float


# Explicit schema: only these fields are parsed; anything else in the JSON is
# ignored, and events missing a field get NULL.
_RAW_COLUMNS = """{
    'id': 'VARCHAR',
    'type': 'VARCHAR',
    'created_at': 'VARCHAR',
    'public': 'BOOLEAN',
    'actor': 'STRUCT(login VARCHAR)',
    'repo': 'STRUCT(id BIGINT, name VARCHAR)',
    'org': 'STRUCT(login VARCHAR)',
    'payload': 'JSON'
}"""

# Typed projection kept deliberately lean: just what the gold marts need
# (repo activity, trending, PR open->merge stats).
_PROJECTION = """
    id,
    type,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    public,
    actor.login AS actor_login,
    repo.id AS repo_id,
    repo.name AS repo_name,
    org.login AS org_login,
    json_extract_string(payload, '$.action') AS payload_action,
    TRY_CAST(json_extract_string(payload, '$.pull_request.number') AS BIGINT) AS pr_number,
    -- NULL in current data: 2026 GH Archive slimmed PR payloads to
    -- {url,id,number,head,base}. Kept for schema stability and because
    -- older archive eras do carry it.
    TRY_CAST(json_extract_string(payload, '$.pull_request.merged_at') AS TIMESTAMP) AS pr_merged_at,
    TRY_CAST(json_extract_string(payload, '$.size') AS INTEGER) AS push_commits
"""


def connect(cfg: Config) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB session with explicit resource limits."""
    cfg.duckdb_tmp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{cfg.duckdb_memory_limit}'")
    con.execute(f"SET threads = {cfg.duckdb_threads}")
    con.execute(f"SET temp_directory = '{cfg.duckdb_tmp_dir.as_posix()}'")
    # Insertion-order preservation pins intermediates in memory and defeats
    # spilling; row order is meaningless for our partitions.
    con.execute("SET preserve_insertion_order = false")
    return con


def transform_hour(hour_dt: datetime, cfg: Config | None = None) -> TransformResult:
    """Bronze hour -> silver Parquet partition. Idempotent (overwrite-partition)."""
    cfg = cfg or get_config()
    bronze = hour_partition(cfg.bronze_dir, hour_dt) / BRONZE_FILENAME
    if not bronze.exists():
        raise BronzeNotFoundError(f"bronze partition missing, ingest first: {bronze}")

    partition = hour_partition(cfg.silver_events_dir, hour_dt)
    tmp_dir = partition.with_name(partition.name + ".tmp")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)
    out_file = tmp_dir / SILVER_FILENAME

    read_expr = f"""read_json(
        '{bronze.as_posix()}',
        format = 'newline_delimited',
        compression = 'gzip',
        columns = {_RAW_COLUMNS}
    )"""
    staging = tmp_dir / "staging.parquet"
    parquet_opts = (
        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {cfg.parquet_row_group_size})"
    )

    start = time.monotonic()
    con = connect(cfg)
    try:
        # Phase 1: stream raw JSON into a typed staging Parquet file. No
        # blocking operator shares the pipeline with the (unspillable) JSON
        # reader, so this holds only small buffers regardless of hour size.
        rows_in = con.execute(
            f"COPY (SELECT {_PROJECTION} FROM {read_expr}) "
            f"TO '{staging.as_posix()}' {parquet_opts}"
        ).fetchone()[0]
        # Phase 2: dedup from Parquet. DISTINCT is a hash aggregate, which
        # spills to temp_directory when the hour exceeds memory_limit.
        rows_out = con.execute(
            f"COPY (SELECT DISTINCT * FROM read_parquet('{staging.as_posix()}')) "
            f"TO '{out_file.as_posix()}' {parquet_opts}"
        ).fetchone()[0]
    finally:
        con.close()
    staging.unlink()

    bytes_out = out_file.stat().st_size
    if partition.exists():
        shutil.rmtree(partition)
    os.replace(tmp_dir, partition)
    duration = time.monotonic() - start

    log.info(
        "transformed %s: %d -> %d rows (%d duplicates), %.1f MB parquet in %.1fs",
        partition, rows_in, rows_out, rows_in - rows_out, bytes_out / 1e6, duration,
    )
    return TransformResult(
        path=partition / SILVER_FILENAME,
        rows_in=rows_in,
        rows_out=rows_out,
        bytes_out=bytes_out,
        duration_seconds=duration,
    )
