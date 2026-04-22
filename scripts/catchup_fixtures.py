"""One-off catch-up: fetch upcoming fixtures for the rest of the season.

The daily ingest Lambda pulls a [today-3d, today+7d] window, which is
fine once it's running daily but leaves a gap when it hasn't run in a
while (or hasn't been deployed yet). This script pulls a wide window
(default: today → 2026-07-31) in a single /fixtures request per
(league, season), writing results through the normal APIFootballClient
disk cache so the output layout matches the bootstrap data.

Usage:
    uv run python scripts/catchup_fixtures.py
    uv run python scripts/catchup_fixtures.py --club-only
    uv run python scripts/catchup_fixtures.py --from 2026-04-22 --to 2026-06-01
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime

from src.data.api_client import APIFootballClient
from src.data.incremental import CLUB_LEAGUE_SEASONS, NATIONAL_LEAGUE_SEASONS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _pull_domain(
    domain: str,
    league_seasons: list[tuple[int, int]],
    date_from: date,
    date_to: date,
) -> dict[tuple[int, int], int]:
    """Pull the /fixtures window for each (league, season); return counts."""
    client = APIFootballClient(cache_dir=f"data/raw/{domain}")
    counts: dict[tuple[int, int], int] = {}

    for league_id, season in league_seasons:
        payload = client.get(
            "/fixtures",
            {
                "league": league_id,
                "season": season,
                "from": date_from.isoformat(),
                "to": date_to.isoformat(),
            },
        )
        fixtures = payload.get("response", [])
        counts[(league_id, season)] = len(fixtures)
        logger.info(
            "%s league=%d season=%d: %d fixtures in [%s, %s]",
            domain, league_id, season, len(fixtures), date_from, date_to,
        )

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from", dest="date_from", type=_parse_date, default=date.today(),
        help="Window start (inclusive). Default: today.",
    )
    parser.add_argument(
        "--to", dest="date_to", type=_parse_date, default=_parse_date("2026-07-31"),
        help="Window end (inclusive). Default: 2026-07-31 (covers WC 2026 final).",
    )
    parser.add_argument("--club-only", action="store_true")
    parser.add_argument("--national-only", action="store_true")
    args = parser.parse_args()

    if args.date_to < args.date_from:
        parser.error("--to must be >= --from")

    run_club = not args.national_only
    run_national = not args.club_only

    total = 0
    if run_club:
        logger.info("=== Club fixtures: %d leagues ===", len(CLUB_LEAGUE_SEASONS))
        club_counts = _pull_domain("club", CLUB_LEAGUE_SEASONS, args.date_from, args.date_to)
        total += sum(club_counts.values())

    if run_national:
        logger.info("=== National fixtures: %d leagues ===", len(NATIONAL_LEAGUE_SEASONS))
        nat_counts = _pull_domain("national", NATIONAL_LEAGUE_SEASONS, args.date_from, args.date_to)
        total += sum(nat_counts.values())

    logger.info("=== Catch-up complete — %d fixtures fetched ===", total)


if __name__ == "__main__":
    main()
