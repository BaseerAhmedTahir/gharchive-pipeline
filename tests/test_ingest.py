import gzip
from datetime import datetime
from pathlib import Path

import pytest
import requests
import responses

from pipeline.config import Config
from pipeline.ingest import (
    BRONZE_FILENAME,
    CorruptDownloadError,
    HourNotPublishedError,
    TransientDownloadError,
    ingest_hour,
)

HOUR = datetime(2026, 7, 9, 15)
URL = "https://data.gharchive.org/2026-07-09-15.json.gz"
EVENTS = b'{"id":"1","type":"PushEvent"}\n{"id":"2","type":"IssuesEvent"}\n'


@pytest.fixture
def cfg(tmp_path):
    # zero retry wait so failure tests run instantly
    return Config(data_root=tmp_path, http_max_attempts=3, http_retry_initial_wait_seconds=0.0)


def target_path(cfg):
    return cfg.bronze_dir / "date=2026-07-09" / "hour=15" / BRONZE_FILENAME


def gz_body():
    return gzip.compress(EVENTS)


@responses.activate
def test_successful_ingest_writes_verified_gzip(cfg):
    responses.add(responses.GET, URL, body=gz_body(), status=200)

    result = ingest_hour(HOUR, cfg=cfg)

    assert result.path == target_path(cfg)
    assert not result.skipped
    assert result.bytes_downloaded == len(gz_body())
    with gzip.open(result.path, "rb") as f:
        assert f.read() == EVENTS


@responses.activate
def test_skips_when_partition_already_exists(cfg):
    # no responses registered: any HTTP call would raise ConnectionError
    target = target_path(cfg)
    target.parent.mkdir(parents=True)
    target.write_bytes(gz_body())

    result = ingest_hour(HOUR, cfg=cfg)

    assert result.skipped
    assert len(responses.calls) == 0


@responses.activate
def test_force_redownloads_existing_partition(cfg):
    target = target_path(cfg)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"stale")
    responses.add(responses.GET, URL, body=gz_body(), status=200)

    result = ingest_hour(HOUR, cfg=cfg, force=True)

    assert not result.skipped
    assert target.read_bytes() == gz_body()


@responses.activate
def test_404_raises_hour_not_published_without_retry(cfg):
    responses.add(responses.GET, URL, status=404)

    with pytest.raises(HourNotPublishedError):
        ingest_hour(HOUR, cfg=cfg)

    assert len(responses.calls) == 1
    assert not target_path(cfg).exists()


@responses.activate
def test_transient_500_is_retried_until_success(cfg):
    responses.add(responses.GET, URL, status=500)
    responses.add(responses.GET, URL, status=503)
    responses.add(responses.GET, URL, body=gz_body(), status=200)

    result = ingest_hour(HOUR, cfg=cfg)

    assert not result.skipped
    assert len(responses.calls) == 3
    assert target_path(cfg).exists()


@responses.activate
def test_persistent_500_fails_after_max_attempts(cfg):
    responses.add(responses.GET, URL, status=500)

    with pytest.raises(TransientDownloadError):
        ingest_hour(HOUR, cfg=cfg)

    assert len(responses.calls) == cfg.http_max_attempts
    _assert_no_files_left(cfg)


@responses.activate
def test_connection_error_is_retried_until_success(cfg):
    responses.add(responses.GET, URL, body=requests.ConnectionError("reset"))
    responses.add(responses.GET, URL, body=gz_body(), status=200)

    result = ingest_hour(HOUR, cfg=cfg)

    assert not result.skipped
    assert len(responses.calls) == 2


@responses.activate
def test_corrupt_gzip_fails_and_leaves_no_partial_files(cfg):
    responses.add(responses.GET, URL, body=b"this is not gzip", status=200)

    with pytest.raises(CorruptDownloadError):
        ingest_hour(HOUR, cfg=cfg)

    # corrupt download is treated as transient (truncation), so it retried
    assert len(responses.calls) == cfg.http_max_attempts
    _assert_no_files_left(cfg)


@responses.activate
def test_empty_body_fails_and_leaves_no_partial_files(cfg):
    responses.add(responses.GET, URL, body=b"", status=200)

    with pytest.raises(CorruptDownloadError):
        ingest_hour(HOUR, cfg=cfg)

    _assert_no_files_left(cfg)


def _assert_no_files_left(cfg):
    """The atomicity guarantee: after a failed ingest, neither the target
    file nor any .tmp leftovers exist."""
    partition = target_path(cfg).parent
    leftovers = list(partition.glob("*")) if partition.exists() else []
    assert leftovers == []
