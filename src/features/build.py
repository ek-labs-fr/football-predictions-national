"""Assemble flat training table from all feature sources.

Covers Steps 2.5 (match context), 2.6 (FIFA rankings), and 2.7 (join).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
EXTERNAL_DIR = Path("data/external")

# Match weight by competition importance
_WEIGHT_MAP: dict[str, float] = {
    "final": 1.0,
    "semifinal": 0.95,
    "third_place": 0.85,
    "quarterfinal": 0.9,
    "round_of_16": 0.85,
    "group": 0.8,
    "qualifying": 0.4,
    "unknown": 0.5,
}

# Competition-level weight multiplier
_COMP_WEIGHT: dict[int, float] = {
    1: 1.0,  # World Cup
    4: 0.85,  # EURO
    9: 0.8,  # Copa America
    6: 0.75,  # AFCON
    7: 0.7,  # Gold Cup
    10: 0.7,  # Asian Cup
    5: 0.6,  # Nations League
    11: 0.2,  # Friendlies
}

_DEFAULT_RANK = 150  # for teams without a FIFA ranking


# ------------------------------------------------------------------
# Step 2.5 — Match Context Features
# ------------------------------------------------------------------


def add_match_context(fixtures: pd.DataFrame) -> pd.DataFrame:
    """Add match context columns to the fixture DataFrame."""
    df = fixtures.copy()
    df["is_knockout"] = df["stage"].isin(
        {"round_of_16", "quarterfinal", "semifinal", "final", "third_place"}
    )
    df["match_weight"] = df.apply(
        lambda r: _WEIGHT_MAP.get(r["stage"], 0.5) * _COMP_WEIGHT.get(int(r["league_id"]), 0.5),
        axis=1,
    )
    # All tournament matches treated as neutral venue
    df["neutral_venue"] = df["league_id"].isin({1, 4, 5, 6, 7, 9, 10})
    return df


# ------------------------------------------------------------------
# Step 2.6 — FIFA Rankings
# ------------------------------------------------------------------


def _load_fifa_rankings(path: str | Path = EXTERNAL_DIR / "fifa_rankings.csv") -> pd.DataFrame:
    """Load FIFA rankings and prepare for lookup."""
    path = Path(path)
    if not path.exists():
        logger.warning("FIFA rankings file not found at %s — ranking features will be null", path)
        return pd.DataFrame(columns=["team_id", "rank", "rank_date"])
    df = pd.read_csv(path)
    df["rank_date"] = pd.to_datetime(df["rank_date"], utc=True)
    return df.sort_values("rank_date")


def _lookup_rank(team_id: int, match_date: pd.Timestamp, rankings: pd.DataFrame) -> int:
    """Find the most recent rank for a team before the match date."""
    mask = (rankings["team_id"] == team_id) & (rankings["rank_date"] <= match_date)
    matches = rankings.loc[mask]
    if matches.empty:
        return _DEFAULT_RANK
    return int(matches.iloc[-1]["rank"])


def add_fifa_rankings(
    fixtures: pd.DataFrame,
    rankings_path: str | Path = EXTERNAL_DIR / "fifa_rankings.csv",
) -> pd.DataFrame:
    """Add FIFA ranking columns to fixtures."""
    rankings = _load_fifa_rankings(rankings_path)
    df = fixtures.copy()
    df["date"] = pd.to_datetime(df["date"], utc=True)

    if rankings.empty:
        df["home_fifa_rank"] = _DEFAULT_RANK
        df["away_fifa_rank"] = _DEFAULT_RANK
    else:
        df["home_fifa_rank"] = df.apply(
            lambda r: _lookup_rank(int(r["home_team_id"]), r["date"], rankings),
            axis=1,
        )
        df["away_fifa_rank"] = df.apply(
            lambda r: _lookup_rank(int(r["away_team_id"]), r["date"], rankings),
            axis=1,
        )
    return df


# ------------------------------------------------------------------
# Step 2.7 — Assemble Flat Training Table
# ------------------------------------------------------------------


def build_training_table(
    fixtures_path: str | Path = PROCESSED_DIR / "all_fixtures.csv",
    rolling_path: str | Path = PROCESSED_DIR / "features_rolling.csv",
    squad_path: str | Path = PROCESSED_DIR / "features_squad.csv",
    h2h_path: str | Path = PROCESSED_DIR / "features_h2h.csv",
    tournament_path: str | Path = PROCESSED_DIR / "features_tournament.csv",
    rankings_path: str | Path = EXTERNAL_DIR / "fifa_rankings.csv",
    output_path: str | Path = PROCESSED_DIR / "training_table.csv",
) -> pd.DataFrame:
    """Join all feature sources into a single row-per-match training table.

    Returns
    -------
    pd.DataFrame
        The flat training table with all features and labels.
    """
    # Load backbone
    df = pd.read_csv(fixtures_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)
    logger.info("Backbone: %d fixtures", len(df))

    # Match context (Step 2.5)
    df = add_match_context(df)

    # FIFA rankings (Step 2.6)
    df = add_fifa_rankings(df, rankings_path)

    # Rolling features — join for home and away teams
    rolling_path = Path(rolling_path)
    if rolling_path.exists():
        rolling = pd.read_csv(rolling_path)
        home_rolling = rolling.rename(
            columns={c: f"home_{c}" for c in rolling.columns if c not in ("fixture_id", "team_id")}
        )
        away_rolling = rolling.rename(
            columns={c: f"away_{c}" for c in rolling.columns if c not in ("fixture_id", "team_id")}
        )
        df = df.merge(
            home_rolling,
            left_on=["fixture_id", "home_team_id"],
            right_on=["fixture_id", "team_id"],
            how="left",
        ).drop(columns=["team_id"], errors="ignore")
        df = df.merge(
            away_rolling,
            left_on=["fixture_id", "away_team_id"],
            right_on=["fixture_id", "team_id"],
            how="left",
        ).drop(columns=["team_id"], errors="ignore")
    else:
        logger.warning("Rolling features not found at %s", rolling_path)

    # Squad features — join for home and away teams
    squad_path = Path(squad_path)
    if squad_path.exists():
        squad = pd.read_csv(squad_path)
        home_squad = squad.rename(
            columns={c: f"home_{c}" for c in squad.columns if c not in ("team_id", "season")}
        )
        away_squad = squad.rename(
            columns={c: f"away_{c}" for c in squad.columns if c not in ("team_id", "season")}
        )
        df = df.merge(
            home_squad,
            left_on=["home_team_id", "season"],
            right_on=["team_id", "season"],
            how="left",
        ).drop(columns=["team_id"], errors="ignore")
        df = df.merge(
            away_squad,
            left_on=["away_team_id", "season"],
            right_on=["team_id", "season"],
            how="left",
        ).drop(columns=["team_id"], errors="ignore")
    else:
        logger.warning("Squad features not found at %s", squad_path)

    # H2H features
    h2h_path = Path(h2h_path)
    if h2h_path.exists():
        h2h = pd.read_csv(h2h_path)
        df = df.merge(h2h, on="fixture_id", how="left")
    else:
        logger.warning("H2H features not found at %s", h2h_path)

    # Tournament features — join for home and away teams
    tournament_path = Path(tournament_path)
    if tournament_path.exists():
        tourn = pd.read_csv(tournament_path)
        home_tourn = tourn.rename(
            columns={c: f"home_{c}" for c in tourn.columns if c not in ("fixture_id", "team_id")}
        )
        away_tourn = tourn.rename(
            columns={c: f"away_{c}" for c in tourn.columns if c not in ("fixture_id", "team_id")}
        )
        df = df.merge(
            home_tourn,
            left_on=["fixture_id", "home_team_id"],
            right_on=["fixture_id", "team_id"],
            how="left",
        ).drop(columns=["team_id"], errors="ignore")
        df = df.merge(
            away_tourn,
            left_on=["fixture_id", "away_team_id"],
            right_on=["fixture_id", "team_id"],
            how="left",
        ).drop(columns=["team_id"], errors="ignore")
    else:
        logger.warning("Tournament features not found at %s", tournament_path)

    # Differential features
    if "home_points_per_game_l10" in df.columns:
        df["form_diff"] = df["home_points_per_game_l10"] - df["away_points_per_game_l10"]
    if "home_goals_scored_avg_l10" in df.columns:
        df["goals_scored_avg_diff"] = (
            df["home_goals_scored_avg_l10"] - df["away_goals_scored_avg_l10"]
        )
    df["rank_diff"] = df["home_fifa_rank"] - df["away_fifa_rank"]
    if "home_squad_avg_rating" in df.columns:
        df["squad_rating_diff"] = df["home_squad_avg_rating"] - df["away_squad_avg_rating"]
    if "home_top5_league_ratio" in df.columns:
        df["top5_ratio_diff"] = df["home_top5_league_ratio"] - df["away_top5_league_ratio"]

    # Labels
    df["goal_diff"] = df["home_goals"] - df["away_goals"]

    # Drop incomplete matches
    df = df.dropna(subset=["home_goals", "away_goals", "outcome"])

    # Save
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    # Log summary
    logger.info("Training table: %d rows, %d columns", len(df), len(df.columns))
    logger.info("Date range: %s to %s", df["date"].min(), df["date"].max())
    logger.info("Outcome distribution:\n%s", df["outcome"].value_counts().to_string())
    missing_pct = (df.isna().sum() / len(df) * 100).sort_values(ascending=False)
    cols_with_missing = missing_pct[missing_pct > 0]
    if not cols_with_missing.empty:
        logger.info("Columns with missing values:\n%s", cols_with_missing.to_string())

    return df
