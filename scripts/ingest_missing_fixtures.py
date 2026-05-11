"""One-shot ingestion of missing national tournament fixtures.

Targets the 10 (league_id, season) pairs identified by the
``probe_missing_leagues.py`` script: WC 2026 qualifying for every
confederation, AFCON 2025, CONCACAF Gold Cup 2025, Olympics Men 2024.

For each pair, calls ``/fixtures``, then writes the raw response to
``data/raw/national/fixtures/<today>/league=X-season=Y.json`` — the
structured path ``src.features.rebuild.rebuild_fixtures_csv`` reads.

After this finishes, run ``rebuild_fixtures_csv("national")`` to merge
into ``data/processed/all_fixtures.csv``.

Quota cost: ~10 API requests.
"""

from __future__ import annotations

import logging
import sys
from datetime import date

from src.data.api_client import APIFootballClient
from src.features import io

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


# (league_id, season) pairs — must match the discovery in
# ``scripts/probe_missing_leagues.py``.
PAIRS: list[tuple[int, int]] = [
    (6, 2025),  # AFCON 2025
    (22, 2025),  # CONCACAF Gold Cup 2025
    (29, 2023),  # WC Qualification — Africa
    (30, 2026),  # WC Qualification — Asia
    (31, 2026),  # WC Qualification — CONCACAF
    (32, 2024),  # WC Qualification — Europe
    (33, 2026),  # WC Qualification — Oceania
    (34, 2026),  # WC Qualification — South America
    (37, 2026),  # WC Qualification — Intercontinental Play-offs
    (480, 2024),  # Olympics Men 2024
]


def main() -> None:
    client = APIFootballClient(plan="pro")
    today = date.today().isoformat()
    summary: list[tuple[int, int, int, str]] = []

    for league_id, season in PAIRS:
        try:
            resp = client.get("/fixtures", {"league": league_id, "season": season})
        except Exception as exc:  # noqa: BLE001
            logger.warning("league=%d season=%d FAILED: %s", league_id, season, exc)
            summary.append((league_id, season, -1, "error"))
            continue
        n = len(resp.get("response", []))
        out_key = f"data/raw/national/fixtures/{today}/league={league_id}-season={season}.json"
        io.write_json(out_key, resp)
        logger.info("league=%d season=%d  N=%d  → %s", league_id, season, n, out_key)
        summary.append((league_id, season, n, out_key))

    sys.stdout.buffer.write(b"\n=== SUMMARY ===\n")
    total = 0
    for league_id, season, n, key in summary:
        line = f"  league={league_id:>4d}  season={season}  N={n:>4d}  {key}\n"
        sys.stdout.buffer.write(line.encode("utf-8"))
        if n > 0:
            total += n
    sys.stdout.buffer.write(f"\nTotal new fixtures: {total}\n".encode())
    sys.stdout.buffer.write(
        f"API quota remaining: {client.last_quota_remaining}/{client.last_quota_limit}\n".encode()
    )


if __name__ == "__main__":
    main()
