"""Rebuild ``data/processed/all_fixtures.csv`` and ``all_fixtures_club.csv``
from the cached raw fixture JSONs under ``data/raw/{national,club}/fixtures/``.

Unlike ``ingest.merge_all_fixtures`` (which drops rows with null goals), this
script *keeps* upcoming NS fixtures so that the feature pipeline's inference
table has rows to populate. Past fixtures (FT/AET/PEN) keep their labels;
NS fixtures carry nulls for ``home_goals``, ``away_goals``, and ``outcome``.

Usage:
    uv run python scripts/rebuild_fixtures_csv.py
    uv run python scripts/rebuild_fixtures_csv.py --national-only
    uv run python scripts/rebuild_fixtures_csv.py --club-only
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import pandas as pd

from src.data.ingest import _derive_outcome, _parse_stage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

RAW_ROOT = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

# Leagues that exclusively contain national teams (API-Football IDs).
# See src/data/ingest.py::COMPETITION_SEASONS.
NATIONAL_LEAGUE_IDS = {1, 4, 5, 6, 7, 9, 10, 11}


def _row_from_fixture_item(item: dict) -> dict:
    fixture = item.get("fixture", {})
    league = item.get("league", {})
    teams = item.get("teams", {})
    goals = item.get("goals", {}) or {}
    score = item.get("score", {}) or {}
    ht = score.get("halftime") or {}
    status = (fixture.get("status") or {}).get("short")
    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "league_id": league.get("id"),
        "season": league.get("season"),
        "round": league.get("round"),
        "stage": _parse_stage(league.get("round")),
        "home_team_id": (teams.get("home") or {}).get("id"),
        "home_team_name": (teams.get("home") or {}).get("name"),
        "away_team_id": (teams.get("away") or {}).get("id"),
        "away_team_name": (teams.get("away") or {}).get("name"),
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away"),
        "home_goals_ht": ht.get("home"),
        "away_goals_ht": ht.get("away"),
        "outcome": _derive_outcome(goals.get("home"), goals.get("away")),
        "status": status,
    }


def _load_domain(domain: str) -> pd.DataFrame:
    files = sorted((RAW_ROOT / domain / "fixtures").glob("*.json"))
    logger.info("[%s] reading %d cached fixture JSONs", domain, len(files))

    rows: list[dict] = []
    for f in files:
        try:
            payload = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("  skipping unreadable %s: %s", f.name, exc)
            continue
        for item in payload.get("response", []) or []:
            rows.append(_row_from_fixture_item(item))

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    before = len(df)
    df = df.dropna(subset=["fixture_id"]).drop_duplicates(subset=["fixture_id"])
    df["fixture_id"] = df["fixture_id"].astype(int)
    df = df.sort_values("date").reset_index(drop=True)
    logger.info("[%s] %d raw rows → %d unique fixtures", domain, before, len(df))

    status_counts = df["status"].value_counts(dropna=False).to_dict()
    logger.info("[%s] status counts: %s", domain, status_counts)
    return df


def _apply_national_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only fixtures from known national-team competitions.

    Trusts the league ID rather than cross-checking team IDs against a
    possibly-incomplete lookup — ``NATIONAL_LEAGUE_IDS`` are national-only
    competitions by construction, so any team playing in them is a national
    side even if the API didn't list it in a ``/teams?season=2022`` roster.
    """
    before = len(df)
    df = df[df["league_id"].isin(NATIONAL_LEAGUE_IDS)].reset_index(drop=True)
    logger.info("National-league filter: %d → %d fixtures", before, len(df))
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--national-only", action="store_true")
    ap.add_argument("--club-only", action="store_true")
    args = ap.parse_args()

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    if not args.club_only:
        national = _load_domain("national")
        national = _apply_national_filter(national)
        upcoming = int(national["outcome"].isna().sum())
        national.to_csv(PROCESSED_DIR / "all_fixtures.csv", index=False)
        logger.info(
            "Wrote all_fixtures.csv — %d fixtures (%d upcoming)", len(national), upcoming
        )

    if not args.national_only:
        club = _load_domain("club")
        upcoming = int(club["outcome"].isna().sum())
        club.to_csv(PROCESSED_DIR / "all_fixtures_club.csv", index=False)
        logger.info(
            "Wrote all_fixtures_club.csv — %d fixtures (%d upcoming)", len(club), upcoming
        )


if __name__ == "__main__":
    main()
