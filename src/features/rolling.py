"""Rolling team performance features computed from historical fixtures.

For each match on date D, for each team, compute features from all that
team's prior fixtures where date < D.  This guarantees no data leakage.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

# Number of recent matches for rolling windows
_WINDOW_LONG = 10
_WINDOW_SHORT = 5


def _team_match_history(fixtures: pd.DataFrame) -> pd.DataFrame:
    """Reshape fixtures into one row per (team, match), with team-centric columns."""
    home = fixtures.assign(
        team_id=fixtures["home_team_id"],
        opponent_id=fixtures["away_team_id"],
        goals_scored=fixtures["home_goals"],
        goals_conceded=fixtures["away_goals"],
        is_home=True,
    )
    away = fixtures.assign(
        team_id=fixtures["away_team_id"],
        opponent_id=fixtures["home_team_id"],
        goals_scored=fixtures["away_goals"],
        goals_conceded=fixtures["home_goals"],
        is_home=False,
    )
    cols = [
        "fixture_id",
        "date",
        "team_id",
        "opponent_id",
        "goals_scored",
        "goals_conceded",
        "is_home",
    ]
    history = pd.concat([home[cols], away[cols]], ignore_index=True)
    history["date"] = pd.to_datetime(history["date"], utc=True)
    history = history.sort_values(["team_id", "date"]).reset_index(drop=True)

    # Derive per-match result
    history["win"] = (history["goals_scored"] > history["goals_conceded"]).astype(int)
    history["draw"] = (history["goals_scored"] == history["goals_conceded"]).astype(int)
    history["loss"] = (history["goals_scored"] < history["goals_conceded"]).astype(int)
    history["points"] = history["win"] * 3 + history["draw"]
    history["clean_sheet"] = (history["goals_conceded"] == 0).astype(int)
    return history


def _rolling_features_for_team(team_history: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling features for a single team's sorted match history.

    Uses expanding windows capped at _WINDOW_LONG / _WINDOW_SHORT so that
    teams with fewer prior matches still get values.
    """
    rows: list[dict] = []  # type: ignore[type-arg]
    for i in range(len(team_history)):
        current = team_history.iloc[i]
        # All prior matches (strictly before current date)
        prior = team_history.iloc[:i]
        prior_l10 = prior.tail(_WINDOW_LONG)
        prior_l5 = prior.tail(_WINDOW_SHORT)

        n_available = len(prior_l10)

        if n_available == 0:
            rows.append(
                {
                    "fixture_id": current["fixture_id"],
                    "team_id": current["team_id"],
                    "win_rate_l10": None,
                    "goals_scored_avg_l10": None,
                    "goals_conceded_avg_l10": None,
                    "points_per_game_l10": None,
                    "clean_sheet_rate_l10": None,
                    "form_last5": None,
                    "matches_available": 0,
                }
            )
            continue

        form_chars = (
            prior_l5["win"]
            .replace({1: "W"})
            .where(prior_l5["win"] == 1)
            .fillna(prior_l5["draw"].replace({1: "D"}).where(prior_l5["draw"] == 1))
            .fillna("L")
        )
        form_str = "".join(form_chars.tolist())

        rows.append(
            {
                "fixture_id": current["fixture_id"],
                "team_id": current["team_id"],
                "win_rate_l10": prior_l10["win"].mean(),
                "goals_scored_avg_l10": prior_l10["goals_scored"].mean(),
                "goals_conceded_avg_l10": prior_l10["goals_conceded"].mean(),
                "points_per_game_l10": prior_l10["points"].mean(),
                "clean_sheet_rate_l10": prior_l10["clean_sheet"].mean(),
                "form_last5": form_str,
                "matches_available": n_available,
            }
        )

    return pd.DataFrame(rows)


def compute_rolling_features(
    fixtures_path: str | Path = PROCESSED_DIR / "all_fixtures.csv",
    output_path: str | Path = PROCESSED_DIR / "features_rolling.csv",
) -> pd.DataFrame:
    """Compute rolling features for all teams across all fixtures.

    Parameters
    ----------
    fixtures_path:
        Path to the all_fixtures.csv file.
    output_path:
        Where to write the output CSV.

    Returns
    -------
    pd.DataFrame
        Rolling features keyed by (fixture_id, team_id).
    """
    fixtures = pd.read_csv(fixtures_path)
    fixtures["date"] = pd.to_datetime(fixtures["date"], utc=True)
    fixtures = fixtures.sort_values("date").reset_index(drop=True)

    history = _team_match_history(fixtures)

    all_features: list[pd.DataFrame] = []
    for _team_id, team_df in history.groupby("team_id"):
        team_df = team_df.sort_values("date").reset_index(drop=True)
        feats = _rolling_features_for_team(team_df)
        all_features.append(feats)

    result = pd.concat(all_features, ignore_index=True)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    logger.info(
        "Saved %d rolling feature rows to %s",
        len(result),
        output_path,
    )
    return result
