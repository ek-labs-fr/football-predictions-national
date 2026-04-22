"""Squad quality features aggregated from player-level data."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.features import io

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

# Top-5 European leagues for the top5_league_ratio feature.
# Matched case-insensitively against the club_league field.
TOP5_LEAGUES = {
    "premier league",
    "la liga",
    "bundesliga",
    "serie a",
    "ligue 1",
}

_STAR_RATING_THRESHOLD = 8.0


def _is_top5(league_name: object) -> bool:
    if not isinstance(league_name, str):
        return False
    return league_name.strip().lower() in TOP5_LEAGUES


def compute_squad_features(
    players_path: str | Path = PROCESSED_DIR / "players.csv",
    output_path: str | Path = PROCESSED_DIR / "features_squad.csv",
) -> pd.DataFrame:
    """Compute per-(team_id, season) squad quality features.

    Parameters
    ----------
    players_path:
        Path to the players.csv file.
    output_path:
        Where to write the output CSV.

    Returns
    -------
    pd.DataFrame
        Squad features keyed by (team_id, season).
    """
    players = io.read_csv(players_path)
    players["rating_num"] = pd.to_numeric(players["rating"], errors="coerce")
    players["in_top5"] = players["club_league"].apply(_is_top5)

    rows: list[dict] = []  # type: ignore[type-arg]
    for (team_id, season), group in players.groupby(["team_id", "season"]):
        rated = group.dropna(subset=["rating_num"])
        has_enough_ratings = len(rated) >= len(group) * 0.5

        rows.append(
            {
                "team_id": team_id,
                "season": season,
                "squad_avg_age": group["age"].mean() if group["age"].notna().any() else None,
                "squad_avg_rating": rated["rating_num"].mean() if has_enough_ratings else None,
                "top5_league_ratio": group["in_top5"].mean(),
                "squad_goals_club_season": (
                    group["goals"].sum() if group["goals"].notna().any() else None
                ),
                "star_player_present": bool((rated["rating_num"] >= _STAR_RATING_THRESHOLD).any())
                if len(rated) > 0
                else False,
            }
        )

    result = pd.DataFrame(rows)

    io.write_csv(output_path, result)
    logger.info("Saved %d squad feature rows to %s", len(result), output_path)
    return result
