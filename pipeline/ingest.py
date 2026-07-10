"""Ingest one GH Archive hour into the bronze layer.

Fault-tolerance contract:
- Transient failures (connection errors, timeouts, 5xx, corrupt/truncated
  downloads) are retried here with exponential backoff + jitter.
- A 404 means GH Archive hasn't published the hour yet. That is raised as
  HourNotPublishedError without local retries so the orchestrator can retry
  the whole task on its own (longer) schedule.
- Writes are atomic: stream to a .tmp file, gzip-verify, then os.replace.
  A target file, if present, is therefore always complete — which is what
  makes skip-if-exists a safe idempotency check.
"""

from __future__ import annotations

import gzip
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from pipeline.config import Config, get_config, gharchive_url, hour_partition

log = logging.getLogger(__name__)

BRONZE_FILENAME = "events.json.gz"


class HourNotPublishedError(Exception):
    """GH Archive returned 404 — the hour isn't available (yet)."""


class TransientDownloadError(Exception):
    """Retryable failure: connection problem, timeout, or 5xx response."""


class CorruptDownloadError(TransientDownloadError):
    """Downloaded file failed gzip verification (often a truncated stream,
    so it is treated as retryable)."""


@dataclass(frozen=True)
class IngestResult:
    path: Path
    bytes_downloaded: int
    duration_seconds: float
    skipped: bool


def ingest_hour(hour_dt: datetime, cfg: Config | None = None, force: bool = False) -> IngestResult:
    """Download one GH Archive hour into its bronze partition. Idempotent."""
    cfg = cfg or get_config()
    target = hour_partition(cfg.bronze_dir, hour_dt) / BRONZE_FILENAME

    if target.exists() and not force:
        log.info("bronze partition already ingested, skipping: %s", target)
        return IngestResult(path=target, bytes_downloaded=0, duration_seconds=0.0, skipped=True)

    target.parent.mkdir(parents=True, exist_ok=True)
    url = gharchive_url(cfg, hour_dt)
    tmp = target.with_name(target.name + ".tmp")

    start = time.monotonic()
    retryer = Retrying(
        retry=retry_if_exception_type(TransientDownloadError),
        stop=stop_after_attempt(cfg.http_max_attempts),
        wait=wait_exponential_jitter(
            initial=cfg.http_retry_initial_wait_seconds,
            jitter=cfg.http_retry_initial_wait_seconds,
        ),
        reraise=True,
    )
    bytes_downloaded = retryer(_download_and_verify, url, tmp, cfg)
    os.replace(tmp, target)
    duration = time.monotonic() - start

    log.info(
        "ingested %s: %.1f MB in %.1fs", target, bytes_downloaded / 1e6, duration
    )
    return IngestResult(
        path=target, bytes_downloaded=bytes_downloaded, duration_seconds=duration, skipped=False
    )


def _download_and_verify(url: str, tmp: Path, cfg: Config) -> int:
    """One download attempt: stream to tmp, then gzip-verify. Cleans tmp on failure."""
    try:
        with requests.get(url, stream=True, timeout=cfg.http_timeout_seconds) as resp:
            if resp.status_code == 404:
                raise HourNotPublishedError(url)
            if resp.status_code >= 500:
                raise TransientDownloadError(f"HTTP {resp.status_code} from {url}")
            resp.raise_for_status()
            written = 0
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=cfg.download_chunk_bytes):
                    f.write(chunk)
                    written += len(chunk)
    except (requests.ConnectionError, requests.Timeout) as exc:
        tmp.unlink(missing_ok=True)
        raise TransientDownloadError(str(exc)) from exc
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    if written == 0:
        tmp.unlink(missing_ok=True)
        raise CorruptDownloadError(f"empty response body from {url}")
    _verify_gzip(tmp)
    return written


def _verify_gzip(path: Path) -> None:
    """Stream through the whole gzip file to prove it decompresses cleanly."""
    try:
        with gzip.open(path, "rb") as f:
            while f.read(1 << 20):
                pass
    except (OSError, EOFError) as exc:  # BadGzipFile subclasses OSError
        path.unlink(missing_ok=True)
        raise CorruptDownloadError(f"gzip verification failed for {path}: {exc}") from exc
