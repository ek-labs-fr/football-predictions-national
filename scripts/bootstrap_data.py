"""Bootstrap script — pulls all required data from API-Football.

Usage:
    uv run python scripts/bootstrap_data.py [--skip-events] [--skip-players]
    uv run python scripts/bootstrap_data.py --only-remaining

This script executes the full data ingestion pipeline (Steps 1.4–1.12):
  1. Fetch international leagues and national teams (reference data)
  2. Pull all historical fixtures across configured competitions/seasons
  3. Pull team statistics for every (team, league, season) combo
  4. Pull player/squad data for post-2006 fixtures
  5. Pull H2H records for all unique team pairs
  6. Pull match events for post-2006 fixtures
  7. Pull betting odds for all fixtures
  8. Pull match statistics (shots, xG, possession) for post-2006 fixtures
  9. Pull injuries/suspensions for post-2010 fixtures

All raw API responses are cached to data/raw/ — re-running is safe and
will skip already-cached requests.
"""

from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from src.data.api_client import APIFootballClient
from src.data.ingest import (
    PROCESSED_DIR,
    build_team_lookup,
    fetch_international_leagues,
    fetch_national_teams,
    merge_all_fixtures,
    pull_events,
    pull_head_to_head,
    pull_injuries,
    pull_match_statistics,
    pull_odds,
    pull_players,
    pull_team_statistics,
    save_leagues,
    save_teams,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _load_fixtures() -> pd.DataFrame:
    """Load existing fixtures CSV if available."""
    path = PROCESSED_DIR / "all_fixtures.csv"
    if path.exists():
        df = pd.read_csv(path)
        logger.info("Loaded %d existing fixtures from %s", len(df), path)
        return df
    return pd.DataFrame()


def main(
    skip_players: bool = False,
    skip_events: bool = False,
    only_remaining: bool = False,
) -> None:
    client = APIFootballClient()

    if only_remaining:
        # Skip steps 1.4–1.8 (already complete) and only pull new data types
        fixtures_df = _load_fixtures()
        if fixtures_df.empty:
            logger.error("No fixtures found — run full bootstrap first")
            sys.exit(1)
        logger.info("=== Pulling remaining data (events, odds, stats, injuries) ===")
    else:
        # Step 1.4 — Reference data
        logger.info("=== Step 1.4: Reference data (leagues & teams) ===")
        leagues = fetch_international_leagues(client)
        save_leagues(leagues)

        teams = fetch_national_teams(client)
        save_teams(teams)
        build_team_lookup(teams)

        # Step 1.5 — Historical fixtures
        logger.info("=== Step 1.5: Historical fixtures ===")
        fixtures_df = merge_all_fixtures(client)
        logger.info("Total fixtures: %d", len(fixtures_df))

        # Step 1.6 — Team statistics
        logger.info("=== Step 1.6: Team statistics ===")
        stats_df = pull_team_statistics(client, fixtures_df)
        logger.info("Total team-season stats: %d", len(stats_df))

        # Step 1.7 — Player data
        if skip_players:
            logger.info("=== Step 1.7: Skipped (--skip-players) ===")
        else:
            logger.info("=== Step 1.7: Player & squad data ===")
            players_df = pull_players(client, fixtures_df)
            logger.info("Total player rows: %d", len(players_df))

        # Step 1.8 — Head-to-head
        logger.info("=== Step 1.8: Head-to-head data ===")
        h2h_df = pull_head_to_head(client, fixtures_df)
        logger.info("Total H2H fixture rows: %d", len(h2h_df))

    # Step 1.9 — Match events
    if skip_events and not only_remaining:
        logger.info("=== Step 1.9: Skipped (--skip-events) ===")
    else:
        logger.info("=== Step 1.9: Match events ===")
        events_df = pull_events(client, fixtures_df)
        logger.info("Total event rows: %d", len(events_df))

    # Step 1.10 — Betting odds
    logger.info("=== Step 1.10: Betting odds ===")
    odds_df = pull_odds(client, fixtures_df)
    logger.info("Total odds rows: %d", len(odds_df))

    # Step 1.11 — Match statistics
    logger.info("=== Step 1.11: Match statistics (shots, xG, possession) ===")
    match_stats_df = pull_match_statistics(client, fixtures_df)
    logger.info("Total match statistics rows: %d", len(match_stats_df))

    # Step 1.12 — Injuries / suspensions
    logger.info("=== Step 1.12: Injuries / suspensions ===")
    injuries_df = pull_injuries(client, fixtures_df)
    logger.info("Total injury rows: %d", len(injuries_df))

    logger.info("=== Bootstrap complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bootstrap data from API-Football")
    parser.add_argument("--skip-players", action="store_true", help="Skip player data pull")
    parser.add_argument("--skip-events", action="store_true", help="Skip match events pull")
    parser.add_argument(
        "--only-remaining",
        action="store_true",
        help="Only pull remaining data (odds, match stats, injuries) — skips steps 1.4–1.8",
    )
    args = parser.parse_args()
    try:
        main(
            skip_players=args.skip_players,
            skip_events=args.skip_events,
            only_remaining=args.only_remaining,
        )
    except KeyboardInterrupt:
        logger.info("Interrupted — cached data is safe to resume from")
        sys.exit(1)
