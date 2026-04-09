"""Tests for in-tournament features."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from src.features.tournament import compute_tournament_features

if TYPE_CHECKING:
    from pathlib import Path

_FIXTURES = pd.DataFrame(
    [
        {
            "fixture_id": 1,
            "date": "2022-11-21",
            "league_id": 1,
            "season": 2022,
            "round": "Group A - 1",
            "home_team_id": 10,
            "away_team_id": 20,
            "home_goals": 2,
            "away_goals": 0,
            "status": "FT",
        },
        {
            "fixture_id": 2,
            "date": "2022-11-25",
            "league_id": 1,
            "season": 2022,
            "round": "Group A - 2",
            "home_team_id": 10,
            "away_team_id": 30,
            "home_goals": 1,
            "away_goals": 1,
            "status": "FT",
        },
    ]
)


class TestComputeTournamentFeatures:
    def test_first_match_zeros(self, tmp_path: Path) -> None:
        fixtures_path = tmp_path / "fixtures.csv"
        _FIXTURES.to_csv(fixtures_path, index=False)
        output_path = tmp_path / "tourn.csv"

        result = compute_tournament_features(
            fixtures_path, tmp_path / "nonexistent_events.csv", output_path
        )

        # Fixture 1, team 10: first match in tournament
        row = result[(result["fixture_id"] == 1) & (result["team_id"] == 10)].iloc[0]
        assert row["matches_played_in_tournament"] == 0
        assert row["tournament_goals_scored_so_far"] == 0
        assert pd.isna(row["days_since_last_match"])

    def test_second_match_accumulates(self, tmp_path: Path) -> None:
        fixtures_path = tmp_path / "fixtures.csv"
        _FIXTURES.to_csv(fixtures_path, index=False)
        output_path = tmp_path / "tourn.csv"

        result = compute_tournament_features(
            fixtures_path, tmp_path / "nonexistent_events.csv", output_path
        )

        # Fixture 2, team 10: should have stats from fixture 1
        row = result[(result["fixture_id"] == 2) & (result["team_id"] == 10)].iloc[0]
        assert row["matches_played_in_tournament"] == 1
        assert row["tournament_goals_scored_so_far"] == 2
        assert row["tournament_goals_conceded_so_far"] == 0
        assert row["days_since_last_match"] == 4  # Nov 25 - Nov 21

    def test_new_team_starts_fresh(self, tmp_path: Path) -> None:
        fixtures_path = tmp_path / "fixtures.csv"
        _FIXTURES.to_csv(fixtures_path, index=False)
        output_path = tmp_path / "tourn.csv"

        result = compute_tournament_features(
            fixtures_path, tmp_path / "nonexistent_events.csv", output_path
        )

        # Fixture 2, team 30: first match in tournament
        row = result[(result["fixture_id"] == 2) & (result["team_id"] == 30)].iloc[0]
        assert row["matches_played_in_tournament"] == 0

    def test_output_saved(self, tmp_path: Path) -> None:
        fixtures_path = tmp_path / "fixtures.csv"
        _FIXTURES.to_csv(fixtures_path, index=False)
        output_path = tmp_path / "tourn.csv"

        compute_tournament_features(fixtures_path, tmp_path / "nonexistent_events.csv", output_path)
        assert output_path.exists()
        saved = pd.read_csv(output_path)
        # 2 fixtures × 2 teams each = 4 rows
        assert len(saved) == 4
