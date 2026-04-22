"""Assemble flat training table from all feature sources.

Covers Steps 2.5 (match context), 2.6 (FIFA rankings), and 2.7 (join).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.features import io

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
_DEFAULT_ELO = 1300  # approximate average Elo rating for unrated teams

# API-Football status codes that mean "not yet played, still scheduled".
# Excludes CANC/PST/ABD so we don't predict matches that are never happening.
_UPCOMING_STATUSES = {"NS", "TBD"}


def _filter_upcoming(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only fixtures that are genuinely scheduled but not yet played."""
    mask = df["outcome"].isna()
    if "status" in df.columns:
        mask &= df["status"].isin(_UPCOMING_STATUSES)
    return df[mask].reset_index(drop=True)


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
    if not io.exists(path):
        logger.warning("FIFA rankings file not found at %s — ranking features will be null", path)
        return pd.DataFrame(columns=["team_id", "rank", "rank_date"])
    df = io.read_csv(path)
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
# Elo Ratings
# ------------------------------------------------------------------


def _load_elo_ratings(path: str | Path = EXTERNAL_DIR / "elo_ratings.csv") -> pd.DataFrame:
    """Load Elo ratings and prepare for lookup."""
    if not io.exists(path):
        logger.warning("Elo ratings file not found at %s — Elo features will be null", path)
        return pd.DataFrame(columns=["team_id", "elo_rating", "elo_date"])
    df = io.read_csv(path)
    df["elo_date"] = pd.to_datetime(df["elo_date"], utc=True)
    return df.sort_values("elo_date")


def _lookup_elo(team_id: int, match_date: pd.Timestamp, elo_df: pd.DataFrame) -> float:
    """Find the most recent Elo rating for a team before the match date."""
    mask = (elo_df["team_id"] == team_id) & (elo_df["elo_date"] <= match_date)
    matches = elo_df.loc[mask]
    if matches.empty:
        return _DEFAULT_ELO
    return float(matches.iloc[-1]["elo_rating"])


def add_elo_ratings(
    fixtures: pd.DataFrame,
    elo_path: str | Path = EXTERNAL_DIR / "elo_ratings.csv",
) -> pd.DataFrame:
    """Add Elo rating columns to fixtures."""
    elo = _load_elo_ratings(elo_path)
    df = fixtures.copy()
    df["date"] = pd.to_datetime(df["date"], utc=True)

    if elo.empty:
        df["home_elo"] = _DEFAULT_ELO
        df["away_elo"] = _DEFAULT_ELO
    else:
        df["home_elo"] = df.apply(
            lambda r: _lookup_elo(int(r["home_team_id"]), r["date"], elo),
            axis=1,
        )
        df["away_elo"] = df.apply(
            lambda r: _lookup_elo(int(r["away_team_id"]), r["date"], elo),
            axis=1,
        )
    return df


# ------------------------------------------------------------------
# Step 2.7 — Assemble Flat Training Table
# ------------------------------------------------------------------


def _assemble_national_table(
    fixtures_path: str | Path,
    rolling_path: str | Path,
    squad_path: str | Path,
    h2h_path: str | Path,
    tournament_path: str | Path,
    rankings_path: str | Path,
    elo_path: str | Path,
) -> pd.DataFrame:
    """Return the national fixtures table joined with every feature source.

    No label filtering; no disk write. Callers apply their own filter and
    persist — see ``build_training_table`` and ``build_inference_table``.
    """
    df = io.read_csv(fixtures_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)
    logger.info("Backbone: %d fixtures", len(df))

    df = add_match_context(df)
    df = add_fifa_rankings(df, rankings_path)
    df = add_elo_ratings(df, elo_path)

    if io.exists(rolling_path):
        rolling = io.read_csv(rolling_path)
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

    if io.exists(squad_path):
        squad = io.read_csv(squad_path)
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

    if io.exists(h2h_path):
        h2h = io.read_csv(h2h_path)
        df = df.merge(h2h, on="fixture_id", how="left")
    else:
        logger.warning("H2H features not found at %s", h2h_path)

    if io.exists(tournament_path):
        tourn = io.read_csv(tournament_path)
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

    if "home_points_per_game_l10" in df.columns:
        df["form_diff"] = df["home_points_per_game_l10"] - df["away_points_per_game_l10"]
    if "home_goals_scored_avg_l10" in df.columns:
        df["goals_scored_avg_diff"] = (
            df["home_goals_scored_avg_l10"] - df["away_goals_scored_avg_l10"]
        )
    df["rank_diff"] = df["home_fifa_rank"] - df["away_fifa_rank"]
    if "home_elo" in df.columns:
        df["elo_diff"] = df["home_elo"] - df["away_elo"]
    if "home_squad_avg_rating" in df.columns:
        df["squad_rating_diff"] = df["home_squad_avg_rating"] - df["away_squad_avg_rating"]
    if "home_top5_league_ratio" in df.columns:
        df["top5_ratio_diff"] = df["home_top5_league_ratio"] - df["away_top5_league_ratio"]

    df["goal_diff"] = df["home_goals"] - df["away_goals"]
    return df


def build_training_table(
    fixtures_path: str | Path = PROCESSED_DIR / "all_fixtures.csv",
    rolling_path: str | Path = PROCESSED_DIR / "features_rolling.csv",
    squad_path: str | Path = PROCESSED_DIR / "features_squad.csv",
    h2h_path: str | Path = PROCESSED_DIR / "features_h2h.csv",
    tournament_path: str | Path = PROCESSED_DIR / "features_tournament.csv",
    rankings_path: str | Path = EXTERNAL_DIR / "fifa_rankings.csv",
    elo_path: str | Path = EXTERNAL_DIR / "elo_ratings.csv",
    output_path: str | Path = PROCESSED_DIR / "training_table.csv",
) -> pd.DataFrame:
    """Completed fixtures joined with features + labels — the training set."""
    df = _assemble_national_table(
        fixtures_path, rolling_path, squad_path, h2h_path,
        tournament_path, rankings_path, elo_path,
    )
    df = df.dropna(subset=["home_goals", "away_goals", "outcome"])

    io.write_csv(output_path, df)

    logger.info("Training table: %d rows, %d columns", len(df), len(df.columns))
    logger.info("Date range: %s to %s", df["date"].min(), df["date"].max())
    logger.info("Outcome distribution:\n%s", df["outcome"].value_counts().to_string())
    missing_pct = (df.isna().sum() / len(df) * 100).sort_values(ascending=False)
    cols_with_missing = missing_pct[missing_pct > 0]
    if not cols_with_missing.empty:
        logger.info("Columns with missing values:\n%s", cols_with_missing.to_string())

    return df


def build_inference_table(
    fixtures_path: str | Path = PROCESSED_DIR / "all_fixtures.csv",
    rolling_path: str | Path = PROCESSED_DIR / "features_rolling.csv",
    squad_path: str | Path = PROCESSED_DIR / "features_squad.csv",
    h2h_path: str | Path = PROCESSED_DIR / "features_h2h.csv",
    tournament_path: str | Path = PROCESSED_DIR / "features_tournament.csv",
    rankings_path: str | Path = EXTERNAL_DIR / "fifa_rankings.csv",
    elo_path: str | Path = EXTERNAL_DIR / "elo_ratings.csv",
    output_path: str | Path = PROCESSED_DIR / "inference_table.csv",
) -> pd.DataFrame:
    """Upcoming fixtures (outcome not yet known) with features joined — the serving set."""
    df = _assemble_national_table(
        fixtures_path, rolling_path, squad_path, h2h_path,
        tournament_path, rankings_path, elo_path,
    )
    df = _filter_upcoming(df)

    io.write_csv(output_path, df)

    logger.info("Inference table: %d upcoming fixtures, %d columns", len(df), len(df.columns))
    if len(df):
        logger.info("Date range: %s to %s", df["date"].min(), df["date"].max())

    return df


# ------------------------------------------------------------------
# Club training table
# ------------------------------------------------------------------


def _add_rest_days(fixtures: pd.DataFrame) -> pd.DataFrame:
    """Add home_rest_days / away_rest_days columns from prior fixtures per team."""
    df = fixtures.copy()
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)

    # One row per (team, match) with that team's date
    home = df[["fixture_id", "date", "home_team_id"]].rename(
        columns={"home_team_id": "team_id"}
    )
    away = df[["fixture_id", "date", "away_team_id"]].rename(
        columns={"away_team_id": "team_id"}
    )
    long = pd.concat([home, away], ignore_index=True).sort_values(["team_id", "date"])
    long["prev_date"] = long.groupby("team_id")["date"].shift(1)
    long["rest_days"] = (long["date"] - long["prev_date"]).dt.total_seconds() / 86400.0

    home_rest = long.rename(columns={"team_id": "home_team_id", "rest_days": "home_rest_days"})[
        ["fixture_id", "home_team_id", "home_rest_days"]
    ]
    away_rest = long.rename(columns={"team_id": "away_team_id", "rest_days": "away_rest_days"})[
        ["fixture_id", "away_team_id", "away_rest_days"]
    ]
    df = df.merge(home_rest, on=["fixture_id", "home_team_id"], how="left")
    df = df.merge(away_rest, on=["fixture_id", "away_team_id"], how="left")
    return df


def _assemble_club_table(
    fixtures_path: str | Path,
    rolling_path: str | Path,
    squad_path: str | Path,
    h2h_path: str | Path,
) -> pd.DataFrame:
    """Return the club fixtures table joined with every feature source (no filter)."""
    df = io.read_csv(fixtures_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)
    logger.info("Club backbone: %d fixtures", len(df))

    df = _add_rest_days(df)
    df["match_weight"] = 1.0
    df["neutral_venue"] = False
    df["is_knockout"] = False

    if io.exists(rolling_path):
        rolling = io.read_csv(rolling_path)
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
        logger.warning("Club rolling features not found at %s", rolling_path)

    if io.exists(squad_path):
        squad = io.read_csv(squad_path)
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
        logger.warning("Club squad features not found at %s", squad_path)

    if io.exists(h2h_path):
        h2h = io.read_csv(h2h_path)
        df = df.merge(h2h, on="fixture_id", how="left")
    else:
        logger.warning("Club H2H features not found at %s", h2h_path)

    if "home_points_per_game_l10" in df.columns:
        df["form_diff"] = df["home_points_per_game_l10"] - df["away_points_per_game_l10"]
    if "home_goals_scored_avg_l10" in df.columns:
        df["goals_scored_avg_diff"] = (
            df["home_goals_scored_avg_l10"] - df["away_goals_scored_avg_l10"]
        )
    if "home_squad_avg_rating" in df.columns:
        df["squad_rating_diff"] = df["home_squad_avg_rating"] - df["away_squad_avg_rating"]
    if "home_rest_days" in df.columns:
        df["rest_days_diff"] = df["home_rest_days"] - df["away_rest_days"]

    df["goal_diff"] = df["home_goals"] - df["away_goals"]
    return df


def build_club_training_table(
    fixtures_path: str | Path = PROCESSED_DIR / "all_fixtures_club.csv",
    rolling_path: str | Path = PROCESSED_DIR / "features_rolling_club.csv",
    squad_path: str | Path = PROCESSED_DIR / "features_squad_club.csv",
    h2h_path: str | Path = PROCESSED_DIR / "features_h2h_club.csv",
    output_path: str | Path = PROCESSED_DIR / "training_table_club.csv",
) -> pd.DataFrame:
    """Completed club fixtures joined with features + labels.

    Club pipeline differs from national: no FIFA rankings, no Elo, no tournament
    stage features, and home advantage is real (not a neutral venue).
    """
    df = _assemble_club_table(fixtures_path, rolling_path, squad_path, h2h_path)
    df = df.dropna(subset=["home_goals", "away_goals", "outcome"])

    io.write_csv(output_path, df)

    logger.info("Club training table: %d rows, %d columns", len(df), len(df.columns))
    logger.info("Date range: %s to %s", df["date"].min(), df["date"].max())
    logger.info("Outcome distribution:\n%s", df["outcome"].value_counts().to_string())
    return df


def build_club_inference_table(
    fixtures_path: str | Path = PROCESSED_DIR / "all_fixtures_club.csv",
    rolling_path: str | Path = PROCESSED_DIR / "features_rolling_club.csv",
    squad_path: str | Path = PROCESSED_DIR / "features_squad_club.csv",
    h2h_path: str | Path = PROCESSED_DIR / "features_h2h_club.csv",
    output_path: str | Path = PROCESSED_DIR / "inference_table_club.csv",
) -> pd.DataFrame:
    """Upcoming club fixtures with features joined — the serving set."""
    df = _assemble_club_table(fixtures_path, rolling_path, squad_path, h2h_path)
    df = _filter_upcoming(df)

    io.write_csv(output_path, df)

    logger.info("Club inference table: %d upcoming fixtures, %d columns", len(df), len(df.columns))
    if len(df):
        logger.info("Date range: %s to %s", df["date"].min(), df["date"].max())

    return df
