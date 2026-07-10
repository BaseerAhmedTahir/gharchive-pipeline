from datetime import datetime
from pathlib import Path

from pipeline.config import Config, get_config, gharchive_url, hour_partition

HOUR = datetime(2026, 7, 9, 5)


def test_data_root_env_override(monkeypatch):
    monkeypatch.setenv("DATA_ROOT", r"C:\somewhere\lake")
    assert get_config().data_root == Path(r"C:\somewhere\lake")


def test_data_root_defaults_to_repo_data_dir(monkeypatch):
    monkeypatch.delenv("DATA_ROOT", raising=False)
    cfg = get_config()
    assert cfg.data_root.name == "data"
    assert cfg.bronze_dir == cfg.data_root / "bronze"


def test_retention_env_overrides(monkeypatch):
    monkeypatch.setenv("BRONZE_RETENTION_DAYS", "5")
    monkeypatch.setenv("SILVER_RETENTION_DAYS", "14")
    cfg = get_config()
    assert cfg.bronze_retention_days == 5
    assert cfg.silver_retention_days == 14


def test_hour_partition_is_zero_padded_hive_style(tmp_path):
    assert hour_partition(tmp_path, HOUR) == tmp_path / "date=2026-07-09" / "hour=05"


def test_gharchive_url_hour_is_not_zero_padded():
    cfg = Config(data_root=Path("unused"))
    assert gharchive_url(cfg, HOUR) == "https://data.gharchive.org/2026-07-09-5.json.gz"
