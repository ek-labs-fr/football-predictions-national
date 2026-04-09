"""Head-to-head features computed from historical matchup records."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

# Minimum prior H2H meetings before populating features
_MIN_H2H_MATCHES = 3

# Neutral defaults when teams have fewer than _MIN_H2H_MATCHES meetings
_NEUTRAL: dict[str, float | int | str | None] = {
    "h2h_home_wins": 0,
    "h2h_away_wins": 0,
    "h2h_draws": 0,
    "h2h_home_goals_avg": None,
    "h2h_away_goals_avg": None,
    "h2h_matches_total": 0,
    "h2h_last_winner": None,
    "h2h_home_win_rate": 0.33,
}


def compute_h2h_features(
    fixtures_path: str | Path = PROCESSED_DIR / "all_fixtures.csv",
    h2h_path: str | Path = PROCESSED_DIR / "h2h_raw.csv",
    output_path: str | Path = PROCESSED_DIR / "features_h2h.csv",
) -> pd.DataFrame:
    """Compute head-to-head features for every fixture in the fixture set.

    For each match, looks up all prior H2H fixtures between the two teams
    (date < match_date) and computes aggregate statistics.

    Parameters
    ----------
    fixtures_path:
        Path to all_fixtures.csv.
    h2h_path:
        Path to h2h_raw.csv.
    output_path:
        Where to write the output CSV.

    Returns
    -------
    pd.DataFrame
        H2H features keyed by fixture_id.
    """
    fixtures = pd.read_csv(fixtures_path)
    fixtures["date"] = pd.to_datetime(fixtures["date"], utc=True)

    h2h_raw = pd.read_csv(h2h_path)
    h2h_raw["date"] = pd.to_datetime(h2h_raw["date"], utc=True)

    rows: list[dict] = []  # type: ignore[type-arg]
    for _, match in fixtures.iterrows():
        fid = match["fixture_id"]
        home_id = match["home_team_id"]
        away_id = match["away_team_id"]
        match_date = match["date"]

        # Find all prior H2H between these two teams
        mask = (
            (h2h_raw["home_team_id"] == home_id) & (h2h_raw["away_team_id"] == away_id)
            | (h2h_raw["home_team_id"] == away_id) & (h2h_raw["away_team_id"] == home_id)
        ) & (h2h_raw["date"] < match_date)
        prior = h2h_raw.loc[mask].sort_values("date")

        if len(prior) < _MIN_H2H_MATCHES:
            rows.append({"fixture_id": fid, **_NEUTRAL})
            continue

        # Compute H2H stats from the perspective of home_id vs away_id
        h2h_home_wins = 0
        h2h_away_wins = 0
        h2h_draws = 0
        h2h_home_goals: list[int] = []
        h2h_away_goals: list[int] = []
        last_winner = None

        # Filter out H2H matches with missing goals
        prior = prior.dropna(subset=["home_goals", "away_goals"])
        if len(prior) < _MIN_H2H_MATCHES:
            rows.append({"fixture_id": fid, **_NEUTRAL})
            continue

        for _, h in prior.iterrows():
            hg = int(h["home_goals"])
            ag = int(h["away_goals"])

            if int(h["home_team_id"]) == home_id:
                # Match played in the same home/away orientation
                h2h_home_goals.append(hg)
                h2h_away_goals.append(ag)
                if hg > ag:
                    h2h_home_wins += 1
                    last_winner = "home"
                elif ag > hg:
                    h2h_away_wins += 1
                    last_winner = "away"
                else:
                    h2h_draws += 1
                    last_winner = "draw"
            else:
                # Reversed orientation — swap perspective
                h2h_home_goals.append(ag)
                h2h_away_goals.append(hg)
                if ag > hg:
                    h2h_home_wins += 1
                    last_winner = "home"
                elif hg > ag:
                    h2h_away_wins += 1
                    last_winner = "away"
                else:
                    h2h_draws += 1
                    last_winner = "draw"

        total = h2h_home_wins + h2h_away_wins + h2h_draws
        rows.append(
            {
                "fixture_id": fid,
                "h2h_home_wins": h2h_home_wins,
                "h2h_away_wins": h2h_away_wins,
                "h2h_draws": h2h_draws,
                "h2h_home_goals_avg": sum(h2h_home_goals) / total if total else None,
                "h2h_away_goals_avg": sum(h2h_away_goals) / total if total else None,
                "h2h_matches_total": total,
                "h2h_last_winner": last_winner,
                "h2h_home_win_rate": h2h_home_wins / total if total else 0.33,
            }
        )

    result = pd.DataFrame(rows)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    logger.info("Saved %d H2H feature rows to %s", len(result), output_path)
    return result
