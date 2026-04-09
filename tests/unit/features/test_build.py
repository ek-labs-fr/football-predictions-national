"""Tests for training table assembly and match context features."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from src.features.build import add_fifa_rankings, add_match_context, build_training_table

if TYPE_CHECKING:
    from pathlib import Path

_FIXTURES = pd.DataFrame(
    [
        {
            "fixture_id": 1,
            "date": "2022-12-18",
            "league_id": 1,
            "season": 2022,
            "stage": "final",
            "home_team_id": 26,
            "away_team_id": 2,
            "home_goals": 3,
            "away_goals": 3,
            "outcome": "draw",
        },
        {
            "fixture_id": 2,
            "date": "2022-11-22",
            "league_id": 1,
            "season": 2022,
            "stage": "group",
            "home_team_id": 10,
            "away_team_id": 20,
            "home_goals": 1,
            "away_goals": 0,
            "outcome": "home_win",
        },
        {
            "fixture_id": 3,
            "date": "2022-06-01",
            "league_id": 11,
            "season": 2022,
            "stage": "group",
            "home_team_id": 10,
            "away_team_id": 2,
            "home_goals": 0,
            "away_goals": 2,
            "outcome": "away_win",
        },
    ]
)


class TestAddMatchContext:
    def test_knockout_flag(self) -> None:
        result = add_match_context(_FIXTURES)
        assert result.iloc[0]["is_knockout"] == True  # noqa: E712 — numpy bool
        assert result.iloc[1]["is_knockout"] == False  # noqa: E712

    def test_match_weight(self) -> None:
        result = add_match_context(_FIXTURES)
        # Final in World Cup: 1.0 * 1.0 = 1.0
        assert result.iloc[0]["match_weight"] == 1.0
        # Group in World Cup: 0.8 * 1.0 = 0.8
        assert result.iloc[1]["match_weight"] == 0.8
        # Group in Friendlies: 0.8 * 0.2 = 0.16
        assert abs(result.iloc[2]["match_weight"] - 0.16) < 0.01

    def test_neutral_venue(self) -> None:
        result = add_match_context(_FIXTURES)
        assert result.iloc[0]["neutral_venue"] == True  # noqa: E712
        assert result.iloc[2]["neutral_venue"] == False  # noqa: E712


class TestAddFifaRankings:
    def test_with_rankings(self, tmp_path: Path) -> None:
        rankings = pd.DataFrame(
            [
                {"team_id": 26, "rank": 3, "rank_date": "2022-11-01"},
                {"team_id": 2, "rank": 4, "rank_date": "2022-11-01"},
            ]
        )
        rankings_path = tmp_path / "rankings.csv"
        rankings.to_csv(rankings_path, index=False)

        result = add_fifa_rankings(_FIXTURES, rankings_path)
        row = result[result["fixture_id"] == 1].iloc[0]
        assert row["home_fifa_rank"] == 3
        assert row["away_fifa_rank"] == 4

    def test_missing_file_defaults(self, tmp_path: Path) -> None:
        result = add_fifa_rankings(_FIXTURES, tmp_path / "nonexistent.csv")
        assert (result["home_fifa_rank"] == 150).all()
        assert (result["away_fifa_rank"] == 150).all()


class TestBuildTrainingTable:
    def test_minimal_build(self, tmp_path: Path) -> None:
        fixtures_path = tmp_path / "fixtures.csv"
        _FIXTURES.to_csv(fixtures_path, index=False)
        output_path = tmp_path / "training.csv"

        result = build_training_table(
            fixtures_path=fixtures_path,
            rolling_path=tmp_path / "nonexistent_rolling.csv",
            squad_path=tmp_path / "nonexistent_squad.csv",
            h2h_path=tmp_path / "nonexistent_h2h.csv",
            tournament_path=tmp_path / "nonexistent_tourn.csv",
            rankings_path=tmp_path / "nonexistent_rankings.csv",
            output_path=output_path,
        )

        assert len(result) == 3
        assert "match_weight" in result.columns
        assert "rank_diff" in result.columns
        assert "goal_diff" in result.columns
        assert output_path.exists()
