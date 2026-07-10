"""Failure alerting for Airflow task callbacks.

Posts a Slack-compatible JSON message to ALERT_WEBHOOK_URL; if the webhook
isn't configured, the alert goes to the task log at ERROR level instead.
A callback must never raise — a broken alert channel must not turn a task
failure into a callback failure loop — so delivery errors are swallowed
(and logged).
"""

from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)


def notify_failure(context: dict) -> None:
    """Airflow on_failure_callback."""
    ti = context.get("task_instance")
    text = (
        f"Pipeline failure: dag={getattr(ti, 'dag_id', '?')} "
        f"task={getattr(ti, 'task_id', '?')} "
        f"run={context.get('logical_date') or context.get('run_id', '?')} "
        f"try={getattr(ti, 'try_number', '?')} "
        f"error={context.get('exception')!r}"
    )

    url = (os.environ.get("ALERT_WEBHOOK_URL") or "").strip()
    if not url:
        log.error("ALERT (no webhook configured): %s", text)
        return
    try:
        response = requests.post(url, json={"text": text}, timeout=10)
        response.raise_for_status()
        log.info("failure alert delivered to webhook")
    except requests.RequestException:
        log.exception("failed to deliver failure alert (alert text: %s)", text)
