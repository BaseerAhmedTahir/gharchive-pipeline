import logging
from types import SimpleNamespace

import requests
import responses

from pipeline.alerts import notify_failure

WEBHOOK = "https://hooks.example.com/alert"


def fake_context():
    return {
        "task_instance": SimpleNamespace(
            dag_id="gharchive_hourly", task_id="ingest", try_number=3
        ),
        "logical_date": "2026-07-09T15:00:00+00:00",
        "exception": RuntimeError("boom"),
    }


@responses.activate
def test_posts_to_webhook_when_configured(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", WEBHOOK)
    responses.add(responses.POST, WEBHOOK, status=200)

    notify_failure(fake_context())

    assert len(responses.calls) == 1
    body = responses.calls[0].request.body.decode()
    assert "gharchive_hourly" in body
    assert "ingest" in body
    assert "boom" in body


@responses.activate
def test_logs_instead_of_posting_when_unconfigured(monkeypatch, caplog):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)

    with caplog.at_level(logging.ERROR):
        notify_failure(fake_context())

    assert len(responses.calls) == 0
    assert any("ALERT" in r.message for r in caplog.records)


@responses.activate
def test_webhook_failure_never_raises(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", WEBHOOK)
    responses.add(responses.POST, WEBHOOK, body=requests.ConnectionError("down"))

    notify_failure(fake_context())  # must not raise


@responses.activate
def test_webhook_5xx_never_raises(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", WEBHOOK)
    responses.add(responses.POST, WEBHOOK, status=500)

    notify_failure(fake_context())  # must not raise
