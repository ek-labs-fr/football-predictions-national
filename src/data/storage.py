"""S3 storage helpers for raw JSON and Parquet artefacts.

Used by the Step Functions data pipeline (``src/data/lambda_handlers.py`` —
not yet created) and ad-hoc backfill scripts when the target is S3. Local
development writes directly to ``data/raw/{club,national}/`` through
``APIFootballClient`` — this module is only needed once data starts landing
in S3.

See ``Documents/lean-implementation-guide-combined/03-data-ingestion.md`` §3.1.3.
"""

from __future__ import annotations

import io
import json
from typing import Any

import boto3


def save_to_s3(data: Any, bucket: str, key: str, format: str = "json") -> None:
    """Save data to S3 as JSON or Parquet.

    Parameters
    ----------
    data:
        JSON-serialisable object for ``format="json"``, or a pandas ``DataFrame``
        for ``format="parquet"``.
    bucket:
        Destination S3 bucket name (e.g. ``football-predictions-data``).
    key:
        S3 object key. The combined guide convention fans out by domain —
        e.g. ``club/raw/fixtures/39/2025/fixtures.json`` or
        ``national/features/inference/2026-04-14/features.parquet``.
    format:
        ``"json"`` or ``"parquet"``.
    """
    s3 = boto3.client("s3")

    if format == "json":
        body = json.dumps(data, default=str).encode("utf-8")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return

    if format == "parquet":
        buffer = io.BytesIO()
        data.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        return

    raise ValueError(f"Unsupported format {format!r} — use 'json' or 'parquet'")
