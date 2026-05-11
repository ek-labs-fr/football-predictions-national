"""Incrementally fetch /players for (team, season) pairs from the newly
ingested tournaments that aren't yet in ``data/processed/players.csv``.

Squad features (avg age, avg rating, top-5-league ratio, star-player flag,
club-season goals) need at least one row per (team, season). Without
them, fixtures involving new teams in WC 2026 qualifying / AFCON 2025 /
Gold Cup 2025 / Olympics 2024 fall back to medians.

This script:
  1. Reads existing ``players.csv``
  2. Finds (team_id, season) combos appearing in fixtures that are NOT
     covered yet
  3. Calls /players for each missing combo
  4. Appends to players.csv
  5. Does NOT touch existing rows (so cached squad data stays).

Quota cost: ~1 request per missing combo (more if a team has > 30
players triggering pagination — rare for national squads).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.data.api_client import APIFootballClient
from src.data.ingest import _extract_player_row, fetch_players

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

_FIXTURES_CSV = Path("data/processed/all_fixtures.csv")
_PLAYERS_CSV = Path("data/processed/players.csv")

# Tournament filters — the newly-ingested leagues whose squads are most
# likely missing. Restrict to keep the API spend bounded.
_NEW_LEAGUES = {22, 29, 30, 31, 32, 33, 34, 37, 480}
_AFCON_2025 = (6, 2025)


def main() -> None:
    if not _FIXTURES_CSV.exists():
        raise SystemExit(f"missing fixtures CSV: {_FIXTURES_CSV}")
    if not _PLAYERS_CSV.exists():
        raise SystemExit(f"missing players CSV: {_PLAYERS_CSV}")

    fixtures = pd.read_csv(_FIXTURES_CSV)
    players = pd.read_csv(_PLAYERS_CSV)

    new_mask = fixtures["league_id"].isin(_NEW_LEAGUES) | (
        (fixtures["league_id"] == _AFCON_2025[0]) & (fixtures["season"] == _AFCON_2025[1])
    )
    new_fixtures = fixtures[new_mask]

    fix_combos: set[tuple[int, int]] = set()
    for _, row in new_fixtures.iterrows():
        season = int(row["season"])
        fix_combos.add((int(row["home_team_id"]), season))
        fix_combos.add((int(row["away_team_id"]), season))

    existing_combos = set(
        zip(players["team_id"].astype(int), players["season"].astype(int), strict=True)
    )
    missing = sorted(fix_combos - existing_combos)

    logger.info("New fixtures: %d", len(new_fixtures))
    logger.info("Combos in new fixtures: %d", len(fix_combos))
    logger.info("Already in players.csv: %d", len(fix_combos & existing_combos))
    logger.info("Missing — to fetch: %d", len(missing))

    if not missing:
        sys.stdout.buffer.write(b"Nothing to do.\n")
        return

    client = APIFootballClient(plan="pro")
    new_rows: list[dict] = []
    errors = 0
    for i, (team_id, season) in enumerate(missing, 1):
        try:
            pls = fetch_players(client, team_id, season)
        except Exception as exc:  # noqa: BLE001
            logger.warning("  team=%d season=%d FAILED: %s", team_id, season, exc)
            errors += 1
            continue
        for p in pls:
            new_rows.append(_extract_player_row(p, team_id, season))
        if i % 25 == 0:
            sys.stdout.buffer.write(
                f"  fetched {i}/{len(missing)} "
                f"(quota remaining: {client.last_quota_remaining})\n".encode()
            )

    if not new_rows:
        sys.stdout.buffer.write(b"No new player rows.\n")
        return

    new_df = pd.DataFrame(new_rows)
    combined = pd.concat([players, new_df], ignore_index=True)
    # Dedup just in case (player_id + team_id + season is the natural key)
    combined = combined.drop_duplicates(subset=["player_id", "team_id", "season"], keep="last")
    combined.to_csv(_PLAYERS_CSV, index=False)

    sys.stdout.buffer.write(b"\n=== SUMMARY ===\n")
    sys.stdout.buffer.write(f"  Combos requested: {len(missing)}\n".encode())
    sys.stdout.buffer.write(f"  Combos failed:    {errors}\n".encode())
    sys.stdout.buffer.write(f"  New player rows:  {len(new_rows)}\n".encode())
    sys.stdout.buffer.write(f"  players.csv now:  {len(combined)} rows\n".encode())
    sys.stdout.buffer.write(
        f"  Quota remaining:  {client.last_quota_remaining}/{client.last_quota_limit}\n".encode()
    )


if __name__ == "__main__":
    main()
