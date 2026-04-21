"""AWS Lambda entry point for the daily incremental ingest pipeline.

The Step Functions state machine invokes this handler multiple times within
a single execution, with different ``task`` values:

    1. task="fetch_fixtures_window", domain="club"     (result: new_fixture_ids)
    2. task="fetch_fixture_details",  domain="club"     (input: fixture_ids)
    3. task="fetch_fixtures_window", domain="national"
    4. task="fetch_fixture_details", domain="national"
    5. task="commit_manifest", domain=...               (updates manifest)

The API-Football key comes from Secrets Manager — the secret ARN is passed via
the ``API_FOOTBALL_KEY_SECRET_ARN`` env var. Raw JSON lands in the bucket named
by ``DATA_BUCKET``.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3

from src.data.api_client import APIFootballClient
from src.data.incremental import (
    fetch_fixture_details,
    fetch_fixtures_window,
    update_manifest,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_secret_cache: str | None = None


def _get_api_key() -> str:
    """Resolve the API-Football key from Secrets Manager (once per cold start)."""
    global _secret_cache
    if _secret_cache:
        return _secret_cache

    secret_arn = os.environ.get("API_FOOTBALL_KEY_SECRET_ARN")
    if not secret_arn:
        # Fallback for local testing
        key = os.environ.get("API_FOOTBALL_KEY")
        if not key:
            raise RuntimeError(
                "No API key available — set API_FOOTBALL_KEY_SECRET_ARN (Lambda) "
                "or API_FOOTBALL_KEY (local)"
            )
        _secret_cache = key
        return key

    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_arn)
    # Secret may be plain string or JSON {"api_key": "..."}
    raw = resp.get("SecretString", "")
    try:
        parsed = json.loads(raw)
        _secret_cache = parsed.get("api_key") or parsed.get("API_FOOTBALL_KEY") or raw
    except json.JSONDecodeError:
        _secret_cache = raw
    return _secret_cache


def _build_client() -> APIFootballClient:
    """Instantiate the client with Lambda-friendly caching (/tmp)."""
    return APIFootballClient(
        api_key=_get_api_key(),
        cache_dir="/tmp/api_cache",
        plan=os.environ.get("API_FOOTBALL_PLAN", "pro"),
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    """Route based on the ``task`` field in the event payload."""
    task = event.get("task")
    domain = event.get("domain", "club")
    params = event.get("params", {})

    bucket = os.environ["DATA_BUCKET"]
    s3 = boto3.client("s3")
    client = _build_client()

    if task == "fetch_fixtures_window":
        fixture_ids = fetch_fixtures_window(client, s3, bucket, domain)
        return {
            "task": task,
            "domain": domain,
            "new_fixture_ids": fixture_ids,
            "count": len(fixture_ids),
        }

    if task == "fetch_fixture_details":
        fixture_ids = params.get("fixture_ids", [])
        if not fixture_ids:
            return {"task": task, "domain": domain, "status": "skipped", "reason": "no fixtures"}
        summary = fetch_fixture_details(client, s3, bucket, domain, fixture_ids)
        # Mark these as seen in the manifest regardless of per-endpoint errors —
        # retrying the whole fixture next day just re-pulls the same data.
        total = update_manifest(s3, bucket, domain, fixture_ids)
        return {
            "task": task,
            "domain": domain,
            "manifest_total": total,
            **summary,
        }

    raise ValueError(f"Unknown task: {task!r}")
