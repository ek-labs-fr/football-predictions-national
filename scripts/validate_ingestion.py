"""Validate completeness and integrity of all ingested data (Step 1.10).

Usage:
    uv run python scripts/validate_ingestion.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROCESSED = Path("data/processed")
RAW = Path("data/raw")


def _check(label: str, passed: bool, detail: str = "") -> bool:
    status = "PASS" if passed else "FAIL"
    msg = f"  [{status}] {label}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    return passed


def validate() -> bool:
    all_ok = True

    print("\n=== Data Ingestion Validation ===\n")

    # ---- all_fixtures.csv ----
    fixtures_path = PROCESSED / "all_fixtures.csv"
    if not fixtures_path.exists():
        print(f"  [FAIL] {fixtures_path} not found — run bootstrap_data.py first")
        return False

    df = pd.read_csv(fixtures_path)
    df["date"] = pd.to_datetime(df["date"])
    print(f"  Fixtures: {len(df)} rows, date range {df['date'].min()} – {df['date'].max()}")

    all_ok &= _check(
        "No duplicate fixture_ids",
        df["fixture_id"].is_unique,
        f"{df['fixture_id'].duplicated().sum()} dupes" if not df["fixture_id"].is_unique else "",
    )

    all_ok &= _check(
        "All fixtures have an outcome",
        df["outcome"].notna().all(),
        f"{df['outcome'].isna().sum()} missing" if not df["outcome"].notna().all() else "",
    )

    # ---- team_lookup.json ----
    lookup_path = PROCESSED / "team_lookup.json"
    if lookup_path.exists():
        lookup = json.loads(lookup_path.read_text(encoding="utf-8"))
        known_ids = set(lookup.values())
        home_ids = set(df["home_team_id"].unique())
        away_ids = set(df["away_team_id"].unique())
        all_ids = home_ids | away_ids
        missing = all_ids - known_ids
        all_ok &= _check(
            "All fixture team IDs in team_lookup",
            len(missing) == 0,
            f"{len(missing)} unknown IDs: {sorted(missing)[:10]}" if missing else "",
        )
    else:
        _check("team_lookup.json exists", False)
        all_ok = False

    # ---- team_statistics.csv ----
    stats_path = PROCESSED / "team_statistics.csv"
    if stats_path.exists():
        stats_df = pd.read_csv(stats_path)
        expected = set()
        for _, row in df.iterrows():
            lid, season = int(row["league_id"]), int(row["season"])
            expected.add((int(row["home_team_id"]), lid, season))
            expected.add((int(row["away_team_id"]), lid, season))
        actual = set(
            zip(stats_df["team_id"], stats_df["league_id"], stats_df["season"], strict=False)
        )
        coverage = len(actual & expected) / len(expected) * 100 if expected else 0
        all_ok &= _check(
            "Team statistics coverage ≥ 80%",
            coverage >= 80,
            f"{coverage:.1f}% ({len(actual)}/{len(expected)})",
        )
    else:
        _check("team_statistics.csv exists", False, "non-critical")

    # ---- players.csv ----
    players_path = PROCESSED / "players.csv"
    if players_path.exists():
        players_df = pd.read_csv(players_path)
        post2006_teams = set(df[df["date"].dt.year >= 2006]["home_team_id"].unique()) | set(
            df[df["date"].dt.year >= 2006]["away_team_id"].unique()
        )
        player_teams = set(players_df["team_id"].unique())
        coverage = len(player_teams & post2006_teams) / len(post2006_teams) * 100
        _check(
            "Player data covers post-2006 teams",
            coverage >= 70,
            f"{coverage:.1f}% ({len(player_teams & post2006_teams)}/{len(post2006_teams)})",
        )
    else:
        _check("players.csv exists", False, "non-critical")

    # ---- h2h_raw.csv ----
    h2h_path = PROCESSED / "h2h_raw.csv"
    if h2h_path.exists():
        h2h_df = pd.read_csv(h2h_path)
        expected_pairs = set()
        for _, row in df.iterrows():
            a, b = int(row["home_team_id"]), int(row["away_team_id"])
            expected_pairs.add((min(a, b), max(a, b)))
        _check("H2H data exists", len(h2h_df) > 0, f"{len(h2h_df)} rows")
    else:
        _check("h2h_raw.csv exists", False, "non-critical")

    # ---- events.csv ----
    events_path = PROCESSED / "events.csv"
    if events_path.exists():
        events_df = pd.read_csv(events_path)
        _check("Events data exists", len(events_df) > 0, f"{len(events_df)} rows")
    else:
        _check("events.csv exists", False, "non-critical — older tournaments may lack events")

    # ---- Summary ----
    print("\n  Fixtures per competition:")
    for lid, group in df.groupby("league_id"):
        comp_name = next(
            (
                k
                for k, v in {
                    "World Cup": 1,
                    "EURO": 4,
                    "Nations League": 5,
                    "AFCON": 6,
                    "Gold Cup": 7,
                    "Copa America": 9,
                    "Asian Cup": 10,
                    "Friendlies": 11,
                }.items()
                if v == lid
            ),
            f"league_{lid}",
        )
        print(f"    {comp_name}: {len(group)} matches")

    outcome_counts = df["outcome"].value_counts()
    print("\n  Outcome distribution:")
    for outcome, count in outcome_counts.items():
        print(f"    {outcome}: {count} ({count / len(df) * 100:.1f}%)")

    print(f"\n  Overall: {'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
    return all_ok


if __name__ == "__main__":
    ok = validate()
    sys.exit(0 if ok else 1)
