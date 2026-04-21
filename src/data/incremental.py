"""Incremental (forward-sync) ingestion helpers for the AWS Lambda pipeline.

Design
------

Runs daily at 06:00 UTC. Each run:

1. Calls ``/fixtures?from=<today-3d>&to=<today+7d>&league=<id>&season=<yr>``
   for every configured (league, season). Writes the response JSON to S3
   under ``{domain}/fixtures/{date}/league={league}-season={season}.json``
   (date = run date).
2. Extracts fixture IDs whose status code is in COMPLETED_STATUSES AND
   whose ID is not yet in the manifest. For those "newly completed"
   fixtures, pulls /fixtures/events, /fixtures/statistics, /fixtures/lineups,
   /fixtures/headtohead, /odds, /injuries and writes each to S3.
3. Appends the newly-processed fixture IDs to the manifest at
   ``manifests/fixtures_seen.json``.

Paths match the existing local layout so data can be unified later.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from src.data.api_client import APIFootballClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration — edit this tuple to change the daily sync scope.
# ---------------------------------------------------------------------------

# (league_id, season) pairs to sync every day
CLUB_LEAGUE_SEASONS: list[tuple[int, int]] = [
    (39, 2025),   # Premier League
    (61, 2025),   # Ligue 1
    (140, 2025),  # La Liga
]

# National competitions — same 8 leagues used for model training. Season
# values cover the ongoing 2025-2026 tournament calendar.
NATIONAL_LEAGUE_SEASONS: list[tuple[int, int]] = [
    (1, 2026),   # World Cup 2026
    (4, 2024),   # EURO 2024 (tail end of post-tournament reporting)
    (5, 2024),   # Nations League 2024-25
    (9, 2024),   # Copa America 2024
    (10, 2023),  # Asian Cup 2023
    (11, 2026),  # Friendlies 2026
]

LEAGUE_SEASONS_BY_DOMAIN = {
    "club": CLUB_LEAGUE_SEASONS,
    "national": NATIONAL_LEAGUE_SEASONS,
}

# Day window around today: [today-3, today+7]
WINDOW_BACK_DAYS = 3
WINDOW_FORWARD_DAYS = 7

COMPLETED_STATUSES = {"FT", "AET", "PEN"}


# ---------------------------------------------------------------------------
# S3 manifest — tracks which fixture IDs have had their details pulled
# ---------------------------------------------------------------------------


def _manifest_key(domain: str) -> str:
    return f"{domain}/manifests/fixtures_seen.json"


def load_manifest(s3, bucket: str, domain: str) -> set[int]:  # noqa: ANN001
    """Load the set of fixture IDs already processed for a domain."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=_manifest_key(domain))
        data = json.loads(resp["Body"].read().decode("utf-8"))
        return set(int(x) for x in data.get("fixture_ids", []))
    except s3.exceptions.NoSuchKey:
        return set()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Manifest load failed for %s: %s — starting empty", domain, exc)
        return set()


def save_manifest(s3, bucket: str, domain: str, fixture_ids: set[int]) -> None:  # noqa: ANN001
    """Persist the manifest back to S3."""
    body = json.dumps(
        {
            "domain": domain,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(fixture_ids),
            "fixture_ids": sorted(fixture_ids),
        },
        indent=2,
    ).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=_manifest_key(domain),
        Body=body,
        ContentType="application/json",
    )


# ---------------------------------------------------------------------------
# S3 put helpers
# ---------------------------------------------------------------------------


def _put_json(s3, bucket: str, key: str, payload: Any) -> None:  # noqa: ANN001
    body = json.dumps(payload, default=str).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


# ---------------------------------------------------------------------------
# Fixture-window pull
# ---------------------------------------------------------------------------


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def fetch_fixtures_window(
    client: APIFootballClient,
    s3,  # noqa: ANN001
    bucket: str,
    domain: str,
    league_seasons: list[tuple[int, int]] | None = None,
    run_date: date | None = None,
) -> list[int]:
    """Fetch fixtures in the [run_date-3d, run_date+7d] window for each (league, season).

    Returns the list of newly-completed fixture IDs whose details should be pulled.
    """
    league_seasons = league_seasons or LEAGUE_SEASONS_BY_DOMAIN[domain]
    run_date = run_date or _today_utc()
    date_from = (run_date - timedelta(days=WINDOW_BACK_DAYS)).isoformat()
    date_to = (run_date + timedelta(days=WINDOW_FORWARD_DAYS)).isoformat()

    manifest = load_manifest(s3, bucket, domain)
    newly_completed: set[int] = set()

    for league_id, season in league_seasons:
        params = {
            "league": league_id,
            "season": season,
            "from": date_from,
            "to": date_to,
        }
        logger.info("Fetching fixtures %s season=%d from=%s to=%s", league_id, season, date_from, date_to)
        payload = client.get("/fixtures", params)
        key = (
            f"{domain}/fixtures/{run_date.isoformat()}/"
            f"league={league_id}-season={season}.json"
        )
        _put_json(s3, bucket, key, payload)

        for item in payload.get("response", []):
            try:
                fid = int(item["fixture"]["id"])
                status = item["fixture"]["status"]["short"]
            except (KeyError, TypeError):
                continue
            if status in COMPLETED_STATUSES and fid not in manifest:
                newly_completed.add(fid)

    logger.info("Domain=%s — %d newly completed fixtures", domain, len(newly_completed))
    return sorted(newly_completed)


# ---------------------------------------------------------------------------
# Per-fixture detail pulls
# ---------------------------------------------------------------------------


_DETAIL_ENDPOINTS: list[tuple[str, str]] = [
    # (endpoint_path, s3_subdir)
    ("/fixtures/events", "fixtures_events"),
    ("/fixtures/statistics", "fixtures_statistics"),
    ("/fixtures/lineups", "fixtures_lineups"),
    ("/odds", "odds"),
    ("/injuries", "injuries"),
]


def fetch_fixture_details(
    client: APIFootballClient,
    s3,  # noqa: ANN001
    bucket: str,
    domain: str,
    fixture_ids: list[int],
) -> dict[str, int]:
    """Pull per-fixture detail endpoints and write each to S3.

    Returns a summary dict of counts per endpoint.
    """
    summary: dict[str, int] = {subdir: 0 for _, subdir in _DETAIL_ENDPOINTS}
    errors: dict[str, int] = {subdir: 0 for _, subdir in _DETAIL_ENDPOINTS}

    for fid in fixture_ids:
        for endpoint, subdir in _DETAIL_ENDPOINTS:
            try:
                payload = client.get(endpoint, {"fixture": fid})
                key = f"{domain}/{subdir}/{fid}.json"
                _put_json(s3, bucket, key, payload)
                summary[subdir] += 1
            except Exception as exc:  # noqa: BLE001
                errors[subdir] += 1
                logger.warning("Pull failed endpoint=%s fixture=%d: %s", endpoint, fid, exc)

    return {"summary": summary, "errors": errors, "fixtures_processed": len(fixture_ids)}


def update_manifest(
    s3,  # noqa: ANN001
    bucket: str,
    domain: str,
    new_fixture_ids: list[int],
) -> int:
    """Merge new fixture IDs into the manifest; return the new total."""
    existing = load_manifest(s3, bucket, domain)
    existing.update(new_fixture_ids)
    save_manifest(s3, bucket, domain, existing)
    return len(existing)
