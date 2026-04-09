"""Tests for squad quality features."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from src.features.squad import _is_top5, compute_squad_features

if TYPE_CHECKING:
    from pathlib import Path

_PLAYERS = pd.DataFrame(
    [
        {
            "player_id": 1,
            "player_name": "Player A",
            "team_id": 10,
            "season": 2022,
            "age": 28,
            "nationality": "France",
            "position": "Forward",
            "club_league": "Premier League",
            "appearances": 10,
            "goals": 5,
            "assists": 3,
            "yellow_cards": 1,
            "red_cards": 0,
            "rating": "7.5",
        },
        {
            "player_id": 2,
            "player_name": "Player B",
            "team_id": 10,
            "season": 2022,
            "age": 24,
            "nationality": "France",
            "position": "Midfielder",
            "club_league": "Ligue 1",
            "appearances": 8,
            "goals": 2,
            "assists": 5,
            "yellow_cards": 2,
            "red_cards": 0,
            "rating": "8.1",
        },
        {
            "player_id": 3,
            "player_name": "Player C",
            "team_id": 10,
            "season": 2022,
            "age": 30,
            "nationality": "France",
            "position": "Defender",
            "club_league": "MLS",
            "appearances": 6,
            "goals": 0,
            "assists": 1,
            "yellow_cards": 3,
            "red_cards": 1,
            "rating": "6.8",
        },
    ]
)


class TestIsTop5:
    def test_premier_league(self) -> None:
        assert _is_top5("Premier League") is True

    def test_case_insensitive(self) -> None:
        assert _is_top5("la liga") is True

    def test_non_top5(self) -> None:
        assert _is_top5("MLS") is False

    def test_none(self) -> None:
        assert _is_top5(None) is False


class TestComputeSquadFeatures:
    def test_basic(self, tmp_path: Path) -> None:
        players_path = tmp_path / "players.csv"
        _PLAYERS.to_csv(players_path, index=False)
        output_path = tmp_path / "squad.csv"

        result = compute_squad_features(players_path, output_path)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["team_id"] == 10
        assert row["season"] == 2022
        # avg age: (28 + 24 + 30) / 3 ≈ 27.33
        assert abs(row["squad_avg_age"] - 27.33) < 0.1
        # 2 out of 3 in top-5 leagues
        assert abs(row["top5_league_ratio"] - 2 / 3) < 0.01
        # Total goals: 5 + 2 + 0 = 7
        assert row["squad_goals_club_season"] == 7
        # Player B has rating 8.1 >= 8.0
        assert row["star_player_present"] == True  # noqa: E712 — numpy bool
        assert output_path.exists()
