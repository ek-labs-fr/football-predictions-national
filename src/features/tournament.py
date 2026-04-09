"""In-tournament running features tracking team state within a competition."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

# Statuses indicating extra time / penalties
_AET_INDICATORS = {"AET", "aet", "Extra Time"}
_PEN_INDICATORS = {"PEN", "pen", "Penalty"}


def compute_tournament_features(
    fixtures_path: str | Path = PROCESSED_DIR / "all_fixtures.csv",
    events_path: str | Path = PROCESSED_DIR / "events.csv",
    output_path: str | Path = PROCESSED_DIR / "features_tournament.csv",
) -> pd.DataFrame:
    """Compute in-tournament running features for each (fixture_id, team_id).

    For each fixture within a (league_id, season), accumulates stats for each
    team from prior matches in the same tournament, *not including* the current
    match.

    Parameters
    ----------
    fixtures_path:
        Path to all_fixtures.csv.
    events_path:
        Path to events.csv (for yellow/red card counts).
    output_path:
        Where to write the output CSV.

    Returns
    -------
    pd.DataFrame
        Tournament features keyed by (fixture_id, team_id).
    """
    fixtures = pd.read_csv(fixtures_path)
    fixtures["date"] = pd.to_datetime(fixtures["date"], utc=True)
    fixtures = fixtures.sort_values("date").reset_index(drop=True)

    # Load events if available
    events_df: pd.DataFrame | None = None
    events_path = Path(events_path)
    if events_path.exists():
        events_df = pd.read_csv(events_path)

    rows: list[dict[str, Any]] = []

    # Process each tournament (league_id, season) independently
    for (_lid, _season), tournament in fixtures.groupby(["league_id", "season"]):
        tournament = tournament.sort_values("date").reset_index(drop=True)

        # Accumulator per team within this tournament
        team_state: dict[int, dict[str, Any]] = {}

        for _, match in tournament.iterrows():
            fid = match["fixture_id"]
            home_id = int(match["home_team_id"])
            away_id = int(match["away_team_id"])
            match_date = match["date"]

            for team_id in (home_id, away_id):
                state = team_state.get(team_id)
                if state is None:
                    # First match in tournament — all zeros
                    rows.append(
                        {
                            "fixture_id": fid,
                            "team_id": team_id,
                            "matches_played_in_tournament": 0,
                            "tournament_goals_scored_so_far": 0,
                            "tournament_goals_conceded_so_far": 0,
                            "tournament_yellows_so_far": 0,
                            "tournament_reds_so_far": 0,
                            "days_since_last_match": None,
                            "came_from_extra_time": False,
                            "came_from_shootout": False,
                        }
                    )
                else:
                    days_since = (match_date - state["last_date"]).days
                    rows.append(
                        {
                            "fixture_id": fid,
                            "team_id": team_id,
                            "matches_played_in_tournament": state["played"],
                            "tournament_goals_scored_so_far": state["goals_scored"],
                            "tournament_goals_conceded_so_far": state["goals_conceded"],
                            "tournament_yellows_so_far": state["yellows"],
                            "tournament_reds_so_far": state["reds"],
                            "days_since_last_match": days_since,
                            "came_from_extra_time": state["last_extra_time"],
                            "came_from_shootout": state["last_shootout"],
                        }
                    )

            # After emitting features, update accumulators for both teams
            home_goals = int(match["home_goals"])
            away_goals = int(match["away_goals"])

            # Detect extra time / shootout from score columns
            went_to_et = False
            went_to_pen = False
            status = str(match.get("status", ""))
            if status in _AET_INDICATORS:
                went_to_et = True
            if status in _PEN_INDICATORS:
                went_to_pen = True

            # Look up event cards for this fixture
            home_yellows = 0
            home_reds = 0
            away_yellows = 0
            away_reds = 0
            if events_df is not None:
                fixture_events = events_df[events_df["fixture_id"] == fid]
                for _, ev in fixture_events.iterrows():
                    if int(ev["team_id"]) == home_id:
                        home_yellows += int(ev.get("yellow_cards", 0) or 0)
                        home_reds += int(ev.get("red_cards", 0) or 0)
                    elif int(ev["team_id"]) == away_id:
                        away_yellows += int(ev.get("yellow_cards", 0) or 0)
                        away_reds += int(ev.get("red_cards", 0) or 0)

            for team_id, gs, gc, yc, rc in [
                (home_id, home_goals, away_goals, home_yellows, home_reds),
                (away_id, away_goals, home_goals, away_yellows, away_reds),
            ]:
                prev = team_state.get(team_id)
                if prev is None:
                    team_state[team_id] = {
                        "played": 1,
                        "goals_scored": gs,
                        "goals_conceded": gc,
                        "yellows": yc,
                        "reds": rc,
                        "last_date": match_date,
                        "last_extra_time": went_to_et,
                        "last_shootout": went_to_pen,
                    }
                else:
                    prev["played"] += 1
                    prev["goals_scored"] += gs
                    prev["goals_conceded"] += gc
                    prev["yellows"] += yc
                    prev["reds"] += rc
                    prev["last_date"] = match_date
                    prev["last_extra_time"] = went_to_et
                    prev["last_shootout"] = went_to_pen

    result = pd.DataFrame(rows)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    logger.info("Saved %d tournament feature rows to %s", len(result), output_path)
    return result
