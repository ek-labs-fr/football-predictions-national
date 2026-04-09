"""Bootstrap script — pulls all required data from API-Football.

Usage:
    uv run python scripts/bootstrap_data.py [--skip-events] [--skip-players]

This script executes the full data ingestion pipeline (Steps 1.4–1.9):
  1. Fetch international leagues and national teams (reference data)
  2. Pull all historical fixtures across configured competitions/seasons
  3. Pull team statistics for every (team, league, season) combo
  4. Pull player/squad data for post-2006 fixtures
  5. Pull H2H records for all unique team pairs
  6. Pull match events for post-2006 fixtures

All raw API responses are cached to data/raw/ — re-running is safe and
will skip already-cached requests.
"""

from __future__ import annotations

import argparse
import logging
import sys

from src.data.api_client import APIFootballClient
from src.data.ingest import (
    build_team_lookup,
    fetch_international_leagues,
    fetch_national_teams,
    merge_all_fixtures,
    pull_events,
    pull_head_to_head,
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


def main(skip_players: bool = False, skip_events: bool = False) -> None:
    client = APIFootballClient()

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
    if skip_events:
        logger.info("=== Step 1.9: Skipped (--skip-events) ===")
    else:
        logger.info("=== Step 1.9: Match events ===")
        events_df = pull_events(client, fixtures_df)
        logger.info("Total event rows: %d", len(events_df))

    logger.info("=== Bootstrap complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bootstrap data from API-Football")
    parser.add_argument("--skip-players", action="store_true", help="Skip player data pull")
    parser.add_argument("--skip-events", action="store_true", help="Skip match events pull")
    args = parser.parse_args()
    try:
        main(skip_players=args.skip_players, skip_events=args.skip_events)
    except KeyboardInterrupt:
        logger.info("Interrupted — cached data is safe to resume from")
        sys.exit(1)
