"""Tests for head-to-head features."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from src.features.h2h import compute_h2h_features

if TYPE_CHECKING:
    from pathlib import Path

_FIXTURES = pd.DataFrame(
    [
        {
            "fixture_id": 100,
            "date": "2022-12-01",
            "home_team_id": 10,
            "away_team_id": 20,
            "home_goals": 2,
            "away_goals": 1,
        },
    ]
)

_H2H_RAW = pd.DataFrame(
    [
        {
            "fixture_id": 1,
            "date": "2020-01-01",
            "home_team_id": 10,
            "away_team_id": 20,
            "home_goals": 1,
            "away_goals": 0,
        },
        {
            "fixture_id": 2,
            "date": "2020-06-01",
            "home_team_id": 20,
            "away_team_id": 10,
            "home_goals": 2,
            "away_goals": 2,
        },
        {
            "fixture_id": 3,
            "date": "2021-01-01",
            "home_team_id": 10,
            "away_team_id": 20,
            "home_goals": 0,
            "away_goals": 1,
        },
    ]
)


class TestComputeH2HFeatures:
    def test_enough_history(self, tmp_path: Path) -> None:
        fixtures_path = tmp_path / "fixtures.csv"
        _FIXTURES.to_csv(fixtures_path, index=False)
        h2h_path = tmp_path / "h2h.csv"
        _H2H_RAW.to_csv(h2h_path, index=False)
        output_path = tmp_path / "h2h_out.csv"

        result = compute_h2h_features(fixtures_path, h2h_path, output_path)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["fixture_id"] == 100
        assert row["h2h_matches_total"] == 3
        # From perspective of team 10 (home in fixture 100):
        # Game 1: 10 home, won 1-0 → home_win
        # Game 2: 20 home, 2-2 → draw (from 10's perspective)
        # Game 3: 10 home, lost 0-1 → away_win
        assert row["h2h_home_wins"] == 1
        assert row["h2h_away_wins"] == 1
        assert row["h2h_draws"] == 1
        assert output_path.exists()

    def test_not_enough_history(self, tmp_path: Path) -> None:
        fixtures_path = tmp_path / "fixtures.csv"
        _FIXTURES.to_csv(fixtures_path, index=False)
        # Only 2 H2H matches — below threshold of 3
        h2h_path = tmp_path / "h2h.csv"
        _H2H_RAW.head(2).to_csv(h2h_path, index=False)
        output_path = tmp_path / "h2h_out.csv"

        result = compute_h2h_features(fixtures_path, h2h_path, output_path)

        row = result.iloc[0]
        assert row["h2h_matches_total"] == 0  # neutral default
        assert row["h2h_home_win_rate"] == 0.33

    def test_no_future_leakage(self, tmp_path: Path) -> None:
        """H2H should not include matches on or after the fixture date."""
        fixtures_path = tmp_path / "fixtures.csv"
        # Fixture happens on 2020-03-01 — only 1 H2H match is before that
        early_fixture = pd.DataFrame(
            [
                {
                    "fixture_id": 50,
                    "date": "2020-03-01",
                    "home_team_id": 10,
                    "away_team_id": 20,
                    "home_goals": 1,
                    "away_goals": 1,
                }
            ]
        )
        early_fixture.to_csv(fixtures_path, index=False)
        h2h_path = tmp_path / "h2h.csv"
        _H2H_RAW.to_csv(h2h_path, index=False)
        output_path = tmp_path / "h2h_out.csv"

        result = compute_h2h_features(fixtures_path, h2h_path, output_path)

        row = result.iloc[0]
        # Only 1 match before 2020-03-01 → below threshold → neutral
        assert row["h2h_matches_total"] == 0
