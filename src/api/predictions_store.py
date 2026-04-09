"""Predictions store: generates and serves match predictions with performance tracking.

Reads all_fixtures.csv, runs model predictions for each match, compares
predicted vs actual, and computes running performance metrics.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from src.api.models import (
    MatchListResponse,
    MatchResultResponse,
    PerformanceSummaryResponse,
)
from src.models.train import (
    most_likely_score,
    outcome_probs_from_lambdas,
)

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

# Competition name lookup
_COMP_NAMES: dict[int, str] = {
    1: "World Cup",
    4: "EURO",
    5: "Nations League",
    6: "AFCON",
    7: "Gold Cup",
    9: "Copa America",
    10: "Asian Cup",
    11: "Friendlies",
}


def _derive_outcome(home_goals: int, away_goals: int) -> str:
    if home_goals > away_goals:
        return "home_win"
    if away_goals > home_goals:
        return "away_win"
    return "draw"


def _derive_outcome_from_probs(home_win: float, draw: float, away_win: float) -> str:
    mx = max(home_win, draw, away_win)
    if mx == home_win:
        return "home_win"
    if mx == draw:
        return "draw"
    return "away_win"


class PredictionsStore:
    """Generates predictions for all fixtures and tracks running performance."""

    def __init__(self, processed_dir: Path = PROCESSED_DIR) -> None:
        self.processed_dir = processed_dir
        self.matches: list[MatchResultResponse] = []
        self.performance = PerformanceSummaryResponse(
            total_matches=0,
            completed_matches=0,
            correct_outcomes=0,
            correct_scores=0,
            outcome_accuracy=0.0,
            score_accuracy=0.0,
            avg_mae=0.0,
        )
        self.loaded = False

    def load(
        self,
        model_home: object | None,
        model_away: object | None,
        scaler: object | None,
        selected_features: list[str] | None = None,
    ) -> None:
        """Load fixtures, generate predictions, and compute performance."""
        fixtures_path = self.processed_dir / "all_fixtures.csv"
        training_path = self.processed_dir / "training_table.csv"

        if not fixtures_path.exists():
            logger.warning("all_fixtures.csv not found — no matches to serve")
            return

        # Use training table if available (has features), else raw fixtures
        if training_path.exists() and model_home is not None:
            self._load_with_model(training_path, model_home, model_away, scaler, selected_features)
        else:
            self._load_fixtures_only(fixtures_path)

        self._compute_performance()
        self.loaded = True
        logger.info(
            "Loaded %d matches (%d completed, outcome acc: %.1f%%)",
            len(self.matches),
            self.performance.completed_matches,
            self.performance.outcome_accuracy * 100,
        )

    def _load_with_model(
        self,
        training_path: Path,
        model_home: object,
        model_away: object,
        scaler: object | None,
        selected_features: list[str] | None,
    ) -> None:
        """Load training table and generate predictions using the model."""
        df = pd.read_csv(training_path)
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df = df.sort_values("date", ascending=False).reset_index(drop=True)

        # Identify feature columns
        from src.models.train import get_feature_columns

        feature_cols = selected_features or get_feature_columns(df)
        feature_cols = [c for c in feature_cols if c in df.columns]

        X = df[feature_cols].fillna(df[feature_cols].median())
        X_transformed = scaler.transform(X) if scaler is not None else X.values

        lambda_home = np.clip(model_home.predict(X_transformed), 0.01, 10.0)
        lambda_away = np.clip(model_away.predict(X_transformed), 0.01, 10.0)

        for i, (_, row) in enumerate(df.iterrows()):
            lh, la = float(lambda_home[i]), float(lambda_away[i])
            probs = outcome_probs_from_lambdas(lh, la)
            pred_score = most_likely_score(lh, la)
            pred_outcome = _derive_outcome_from_probs(
                probs["home_win"], probs["draw"], probs["away_win"]
            )

            actual_hg = int(row["home_goals"]) if pd.notna(row["home_goals"]) else None
            actual_ag = int(row["away_goals"]) if pd.notna(row["away_goals"]) else None
            actual_score = f"{actual_hg}-{actual_ag}" if actual_hg is not None else None
            actual_outcome = (
                _derive_outcome(actual_hg, actual_ag) if actual_hg is not None else None
            )

            correct_outcome = pred_outcome == actual_outcome if actual_outcome is not None else None
            correct_score = pred_score == actual_score if actual_score is not None else None

            self.matches.append(
                MatchResultResponse(
                    fixture_id=int(row["fixture_id"]),
                    date=str(row["date"]),
                    home_team_id=int(row["home_team_id"]),
                    home_team_name=str(row.get("home_team_name", "")),
                    away_team_id=int(row["away_team_id"]),
                    away_team_name=str(row.get("away_team_name", "")),
                    predicted_home_goals=round(lh, 2),
                    predicted_away_goals=round(la, 2),
                    predicted_score=pred_score,
                    actual_home_goals=actual_hg,
                    actual_away_goals=actual_ag,
                    actual_score=actual_score,
                    predicted_outcome=pred_outcome,
                    actual_outcome=actual_outcome,
                    correct_outcome=correct_outcome,
                    correct_score=correct_score,
                    home_win_prob=round(probs["home_win"], 4),
                    draw_prob=round(probs["draw"], 4),
                    away_win_prob=round(probs["away_win"], 4),
                    league_name=_COMP_NAMES.get(int(row.get("league_id", 0)), ""),
                    round=str(row.get("round", "")),
                )
            )

    def _load_fixtures_only(self, fixtures_path: Path) -> None:
        """Load raw fixtures without model predictions (fallback)."""
        df = pd.read_csv(fixtures_path)
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df = df.sort_values("date", ascending=False).reset_index(drop=True)

        for _, row in df.iterrows():
            actual_hg = int(row["home_goals"]) if pd.notna(row["home_goals"]) else None
            actual_ag = int(row["away_goals"]) if pd.notna(row["away_goals"]) else None
            actual_score = f"{actual_hg}-{actual_ag}" if actual_hg is not None else None
            actual_outcome = (
                _derive_outcome(actual_hg, actual_ag) if actual_hg is not None else None
            )

            self.matches.append(
                MatchResultResponse(
                    fixture_id=int(row["fixture_id"]),
                    date=str(row["date"]),
                    home_team_id=int(row["home_team_id"]),
                    home_team_name=str(row.get("home_team_name", "")),
                    away_team_id=int(row["away_team_id"]),
                    away_team_name=str(row.get("away_team_name", "")),
                    predicted_home_goals=0.0,
                    predicted_away_goals=0.0,
                    predicted_score="0-0",
                    actual_home_goals=actual_hg,
                    actual_away_goals=actual_ag,
                    actual_score=actual_score,
                    predicted_outcome="draw",
                    actual_outcome=actual_outcome,
                    correct_outcome=None,
                    correct_score=None,
                    home_win_prob=0.33,
                    draw_prob=0.34,
                    away_win_prob=0.33,
                    league_name=_COMP_NAMES.get(int(row.get("league_id", 0)), ""),
                    round=str(row.get("round", "")),
                )
            )

    def _compute_performance(self) -> None:
        """Compute running performance summary from match results."""
        completed = [m for m in self.matches if m.actual_outcome is not None]
        correct_outcomes = sum(1 for m in completed if m.correct_outcome)
        correct_scores = sum(1 for m in completed if m.correct_score)

        mae_sum = 0.0
        for m in completed:
            if m.actual_home_goals is not None and m.actual_away_goals is not None:
                mae_sum += (
                    abs(m.predicted_home_goals - m.actual_home_goals)
                    + abs(m.predicted_away_goals - m.actual_away_goals)
                ) / 2

        n_completed = len(completed)
        self.performance = PerformanceSummaryResponse(
            total_matches=len(self.matches),
            completed_matches=n_completed,
            correct_outcomes=correct_outcomes,
            correct_scores=correct_scores,
            outcome_accuracy=correct_outcomes / n_completed if n_completed > 0 else 0.0,
            score_accuracy=correct_scores / n_completed if n_completed > 0 else 0.0,
            avg_mae=mae_sum / n_completed if n_completed > 0 else 0.0,
        )

    def get_match(self, fixture_id: int) -> MatchResultResponse | None:
        """Look up a single match by fixture ID."""
        for m in self.matches:
            if m.fixture_id == fixture_id:
                return m
        return None

    def get_response(self) -> MatchListResponse:
        """Return the full match list with performance summary."""
        return MatchListResponse(matches=self.matches, performance=self.performance)


@lru_cache
def get_predictions_store() -> PredictionsStore:
    """Singleton predictions store."""
    return PredictionsStore()
