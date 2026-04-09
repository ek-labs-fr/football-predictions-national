"""Tests for rolling team performance features."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from src.features.rolling import (
    _rolling_features_for_team,
    _team_match_history,
    compute_rolling_features,
)

if TYPE_CHECKING:
    from pathlib import Path

# Sample fixtures for testing
_FIXTURES = pd.DataFrame(
    [
        {
            "fixture_id": 1,
            "date": "2022-01-01",
            "home_team_id": 10,
            "away_team_id": 20,
            "home_goals": 2,
            "away_goals": 1,
        },
        {
            "fixture_id": 2,
            "date": "2022-02-01",
            "home_team_id": 20,
            "away_team_id": 10,
            "home_goals": 0,
            "away_goals": 0,
        },
        {
            "fixture_id": 3,
            "date": "2022-03-01",
            "home_team_id": 10,
            "away_team_id": 30,
            "home_goals": 1,
            "away_goals": 3,
        },
        {
            "fixture_id": 4,
            "date": "2022-04-01",
            "home_team_id": 10,
            "away_team_id": 20,
            "home_goals": 1,
            "away_goals": 0,
        },
    ]
)


class TestTeamMatchHistory:
    def test_reshapes_correctly(self) -> None:
        history = _team_match_history(_FIXTURES)
        # 4 fixtures × 2 teams each = 8 rows
        assert len(history) == 8
        assert "goals_scored" in history.columns
        assert "win" in history.columns

    def test_home_team_goals(self) -> None:
        history = _team_match_history(_FIXTURES)
        # Team 10, fixture 1 (home): scored 2, conceded 1
        row = history[(history["team_id"] == 10) & (history["fixture_id"] == 1)].iloc[0]
        assert row["goals_scored"] == 2
        assert row["goals_conceded"] == 1
        assert row["win"] == 1

    def test_away_team_goals(self) -> None:
        history = _team_match_history(_FIXTURES)
        # Team 20, fixture 1 (away): scored 1, conceded 2
        row = history[(history["team_id"] == 20) & (history["fixture_id"] == 1)].iloc[0]
        assert row["goals_scored"] == 1
        assert row["goals_conceded"] == 2
        assert row["loss"] == 1


class TestRollingFeatures:
    def test_first_match_has_no_features(self) -> None:
        history = _team_match_history(_FIXTURES)
        team10 = history[history["team_id"] == 10].sort_values("date").reset_index(drop=True)
        feats = _rolling_features_for_team(team10)
        first = feats.iloc[0]
        assert first["matches_available"] == 0
        assert pd.isna(first["win_rate_l10"])

    def test_second_match_uses_first(self) -> None:
        history = _team_match_history(_FIXTURES)
        team10 = history[history["team_id"] == 10].sort_values("date").reset_index(drop=True)
        feats = _rolling_features_for_team(team10)
        second = feats.iloc[1]
        assert second["matches_available"] == 1
        # Team 10 won fixture 1 (2-1)
        assert second["win_rate_l10"] == 1.0
        assert second["goals_scored_avg_l10"] == 2.0

    def test_form_string(self) -> None:
        history = _team_match_history(_FIXTURES)
        team10 = history[history["team_id"] == 10].sort_values("date").reset_index(drop=True)
        feats = _rolling_features_for_team(team10)
        # After 3 matches (W, D, L), 4th match should have form "WDL"
        fourth = feats.iloc[3]
        assert fourth["form_last5"] == "WDL"

    def test_no_leakage(self) -> None:
        """Rolling features must only use data from before the match."""
        history = _team_match_history(_FIXTURES)
        team10 = history[history["team_id"] == 10].sort_values("date").reset_index(drop=True)
        feats = _rolling_features_for_team(team10)
        # Third match (fixture 3): team 10 loses 1-3.
        # Features should NOT include fixture 3's result.
        third = feats.iloc[2]
        assert third["matches_available"] == 2
        # Prior: W (2-1), D (0-0) → avg goals scored = 1.0
        assert third["goals_scored_avg_l10"] == 1.0


class TestComputeRollingFeatures:
    def test_end_to_end(self, tmp_path: Path) -> None:
        fixtures_path = tmp_path / "fixtures.csv"
        _FIXTURES.to_csv(fixtures_path, index=False)
        output_path = tmp_path / "rolling.csv"

        result = compute_rolling_features(fixtures_path, output_path)

        assert len(result) == 8  # 4 fixtures × 2 teams
        assert output_path.exists()
        saved = pd.read_csv(output_path)
        assert len(saved) == 8
