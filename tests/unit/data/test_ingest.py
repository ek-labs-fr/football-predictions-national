"""Unit tests for data ingestion functions (pure logic, no API calls)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.data.ingest import (
    _aggregate_events,
    _derive_outcome,
    _extract_player_row,
    _extract_team_stat_row,
    _parse_stage,
    build_team_lookup,
    fetch_events,
    fetch_fixtures,
    fetch_head_to_head,
    fetch_international_leagues,
    fetch_national_teams,
    fetch_players,
    fetch_team_statistics,
    fixtures_to_dataframe,
    merge_all_fixtures,
    pull_events,
    pull_head_to_head,
    pull_players,
    pull_team_statistics,
    save_leagues,
    save_teams,
)
from src.data.schemas import (
    Fixture,
    FixtureEvent,
    League,
    Player,
    TeamStatistics,
)

if TYPE_CHECKING:
    from pathlib import Path


# ------------------------------------------------------------------
# Stage parsing
# ------------------------------------------------------------------


class TestParseStage:
    @pytest.mark.parametrize(
        ("round_str", "expected"),
        [
            ("Group A - 1", "group"),
            ("Group C - 3", "group"),
            ("Round of 16", "round_of_16"),
            ("8th Finals", "round_of_16"),
            ("Quarter-finals", "quarterfinal"),
            ("Semi-finals", "semifinal"),
            ("Final", "final"),
            ("3rd Place Final", "third_place"),
            ("Qualifying Round", "qualifying"),
            (None, "unknown"),
            ("", "unknown"),
        ],
    )
    def test_parse_stage(self, round_str: str | None, expected: str) -> None:
        assert _parse_stage(round_str) == expected


# ------------------------------------------------------------------
# Outcome derivation
# ------------------------------------------------------------------


class TestDeriveOutcome:
    def test_home_win(self) -> None:
        assert _derive_outcome(2, 1) == "home_win"

    def test_away_win(self) -> None:
        assert _derive_outcome(0, 3) == "away_win"

    def test_draw(self) -> None:
        assert _derive_outcome(1, 1) == "draw"

    def test_none_goals(self) -> None:
        assert _derive_outcome(None, 1) is None
        assert _derive_outcome(1, None) is None


# ------------------------------------------------------------------
# Fixtures to DataFrame
# ------------------------------------------------------------------

SAMPLE_FIXTURE = {
    "fixture": {"id": 100, "date": "2022-12-18T15:00:00+00:00"},
    "league": {"id": 1, "name": "World Cup", "season": 2022, "round": "Final"},
    "teams": {
        "home": {"id": 26, "name": "Argentina"},
        "away": {"id": 2, "name": "France"},
    },
    "goals": {"home": 3, "away": 3},
    "score": {
        "halftime": {"home": 2, "away": 0},
        "fulltime": {"home": 2, "away": 2},
        "extratime": {"home": 1, "away": 1},
        "penalty": {"home": 4, "away": 2},
    },
}


class TestFixturesToDataFrame:
    def test_basic_conversion(self) -> None:
        fixture = Fixture.model_validate(SAMPLE_FIXTURE)
        df = fixtures_to_dataframe([fixture])
        assert len(df) == 1
        row = df.iloc[0]
        assert row["fixture_id"] == 100
        assert row["home_team_id"] == 26
        assert row["away_team_id"] == 2
        assert row["home_goals"] == 3
        assert row["away_goals"] == 3
        assert row["outcome"] == "draw"
        assert row["stage"] == "final"
        assert row["home_goals_ht"] == 2
        assert row["away_goals_ht"] == 0

    def test_empty_list(self) -> None:
        df = fixtures_to_dataframe([])
        assert len(df) == 0


# ------------------------------------------------------------------
# Event aggregation
# ------------------------------------------------------------------


class TestAggregateEvents:
    def test_basic_aggregation(self) -> None:
        events = [
            FixtureEvent.model_validate(
                {"type": "Goal", "detail": "Normal Goal", "team": {"id": 1, "name": "A"}}
            ),
            FixtureEvent.model_validate(
                {"type": "Goal", "detail": "Normal Goal", "team": {"id": 1, "name": "A"}}
            ),
            FixtureEvent.model_validate(
                {"type": "Card", "detail": "Yellow Card", "team": {"id": 1, "name": "A"}}
            ),
            FixtureEvent.model_validate(
                {"type": "Goal", "detail": "Penalty", "team": {"id": 2, "name": "B"}}
            ),
            FixtureEvent.model_validate(
                {"type": "Card", "detail": "Red Card", "team": {"id": 2, "name": "B"}}
            ),
        ]
        rows = _aggregate_events(999, events)
        assert len(rows) == 2
        team1 = next(r for r in rows if r["team_id"] == 1)
        team2 = next(r for r in rows if r["team_id"] == 2)
        assert team1["goals"] == 2
        assert team1["yellow_cards"] == 1
        assert team2["penalties_scored"] == 1
        assert team2["red_cards"] == 1

    def test_own_goal(self) -> None:
        events = [
            FixtureEvent.model_validate(
                {"type": "Goal", "detail": "Own Goal", "team": {"id": 1, "name": "A"}}
            ),
        ]
        rows = _aggregate_events(1, events)
        assert rows[0]["own_goals"] == 1
        assert rows[0]["goals"] == 0

    def test_empty_events(self) -> None:
        assert _aggregate_events(1, []) == []


# ------------------------------------------------------------------
# Team statistics extraction
# ------------------------------------------------------------------


class TestExtractTeamStatRow:
    def test_basic(self) -> None:
        ts = TeamStatistics.model_validate(
            {
                "team": {"id": 2, "name": "France", "national": True},
                "league": {"id": 1, "season": 2022},
                "form": "WWDWL",
                "fixtures": {
                    "played": {"total": 7},
                    "wins": {"total": 4},
                    "draws": {"total": 1},
                    "loses": {"total": 2},
                },
                "clean_sheet": {"total": 2},
                "failed_to_score": {"total": 1},
            }
        )
        row = _extract_team_stat_row(ts)
        assert row["team_id"] == 2
        assert row["form"] == "WWDWL"
        assert row["wins"] == 4
        assert row["clean_sheets"] == 2


# ------------------------------------------------------------------
# Player extraction
# ------------------------------------------------------------------


class TestExtractPlayerRow:
    def test_basic(self) -> None:
        player = Player.model_validate(
            {
                "player": {"id": 276, "name": "K. Mbappé", "age": 24, "nationality": "France"},
                "statistics": [
                    {
                        "team": {"id": 2},
                        "league": {"id": 1, "name": "World Cup", "season": 2022},
                        "games": {
                            "appearences": 7,
                            "position": "Attacker",
                            "rating": "7.8",
                        },
                        "goals": {"total": 8, "assists": 2},
                        "cards": {"yellow": 1, "red": 0},
                    }
                ],
            }
        )
        row = _extract_player_row(player, team_id=2, season=2022)
        assert row["player_id"] == 276
        assert row["goals"] == 8
        assert row["rating"] == "7.8"


# ------------------------------------------------------------------
# Reference data (with mocked client)
# ------------------------------------------------------------------


class TestFetchReferenceData:
    def test_fetch_international_leagues(self) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "response": [
                {
                    "league": {"id": 1, "name": "World Cup"},
                    "country": {"name": "World"},
                    "seasons": [],
                }
            ]
        }
        leagues = fetch_international_leagues(mock_client)
        assert len(leagues) >= 1
        assert leagues[0].league.name == "World Cup"

    def test_fetch_national_teams(self) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "response": [
                {
                    "team": {"id": 2, "name": "France", "national": True},
                    "venue": None,
                }
            ]
        }
        teams = fetch_national_teams(mock_client)
        assert len(teams) >= 1

    def test_fetch_fixtures(self) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {"response": [SAMPLE_FIXTURE]}
        fixtures = fetch_fixtures(mock_client, league_id=1, season=2022)
        assert len(fixtures) == 1
        assert fixtures[0].fixture.id == 100


class TestSaveAndLookup:
    def test_save_leagues(self, tmp_path: Path) -> None:
        league = League.model_validate(
            {"league": {"id": 1, "name": "WC"}, "country": {"name": "World"}, "seasons": []}
        )
        save_leagues([league], output_dir=tmp_path)
        assert (tmp_path / "leagues.json").exists()

    def test_save_teams(self, tmp_path: Path) -> None:
        from src.data.schemas import Team

        team = Team.model_validate({"team": {"id": 2, "name": "France"}})
        save_teams([team], output_dir=tmp_path)
        assert (tmp_path / "teams.json").exists()

    def test_build_team_lookup(self, tmp_path: Path) -> None:
        from src.data.schemas import Team

        teams = [
            Team.model_validate({"team": {"id": 2, "name": "France"}}),
            Team.model_validate({"team": {"id": 6, "name": "Brazil"}}),
        ]
        lookup = build_team_lookup(teams, output_dir=tmp_path)
        assert lookup == {"France": 2, "Brazil": 6}
        saved = json.loads((tmp_path / "team_lookup.json").read_text(encoding="utf-8"))
        assert saved == {"France": 2, "Brazil": 6}


# ------------------------------------------------------------------
# Orchestration functions (mocked client)
# ------------------------------------------------------------------

SAMPLE_FIXTURE_2 = {
    "fixture": {"id": 200, "date": "2022-11-22T10:00:00+00:00"},
    "league": {"id": 1, "name": "World Cup", "season": 2022, "round": "Group A - 1"},
    "teams": {
        "home": {"id": 10, "name": "TeamX"},
        "away": {"id": 20, "name": "TeamY"},
    },
    "goals": {"home": 1, "away": 0},
    "score": {"halftime": {"home": 0, "away": 0}},
}


def _fixtures_df() -> pd.DataFrame:
    """Build a minimal fixtures DataFrame for orchestration tests."""
    return pd.DataFrame(
        [
            {
                "fixture_id": 100,
                "date": "2022-12-18T15:00:00+00:00",
                "league_id": 1,
                "season": 2022,
                "home_team_id": 26,
                "away_team_id": 2,
                "home_goals": 3,
                "away_goals": 3,
                "outcome": "draw",
            },
        ]
    )


class TestMergeAllFixtures:
    def test_merge(self, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {"response": [SAMPLE_FIXTURE, SAMPLE_FIXTURE_2]}
        df = merge_all_fixtures(mock_client, competitions={1: [2022]}, output_dir=tmp_path)
        assert len(df) == 2
        assert (tmp_path / "all_fixtures.csv").exists()

    def test_empty(self, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {"response": []}
        df = merge_all_fixtures(mock_client, competitions={1: [2022]}, output_dir=tmp_path)
        assert len(df) == 0


class TestFetchTeamStatistics:
    def test_returns_model(self) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "response": {
                "team": {"id": 2, "name": "France", "national": True},
                "league": {"id": 1, "season": 2022},
                "form": "WWDWL",
                "fixtures": {
                    "played": {"total": 7},
                    "wins": {"total": 4},
                    "draws": {"total": 1},
                    "loses": {"total": 2},
                },
            }
        }
        ts = fetch_team_statistics(mock_client, 1, 2022, 2)
        assert ts is not None
        assert ts.form == "WWDWL"

    def test_returns_none_on_empty(self) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {"response": {}}
        assert fetch_team_statistics(mock_client, 1, 2022, 999) is None


class TestPullTeamStatistics:
    def test_pull(self, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "response": {
                "team": {"id": 26, "name": "Argentina", "national": True},
                "league": {"id": 1, "season": 2022},
                "form": "WWW",
                "fixtures": {
                    "played": {"total": 3},
                    "wins": {"total": 3},
                    "draws": {"total": 0},
                    "loses": {"total": 0},
                },
            }
        }
        df = pull_team_statistics(mock_client, _fixtures_df(), output_dir=tmp_path)
        assert len(df) >= 1


class TestFetchPlayers:
    def test_single_page(self) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "response": [
                {
                    "player": {"id": 1, "name": "Test Player"},
                    "statistics": [],
                }
            ],
            "paging": {"current": 1, "total": 1},
        }
        players = fetch_players(mock_client, team_id=2, season=2022)
        assert len(players) == 1


class TestFetchHeadToHead:
    def test_returns_fixtures(self) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {"response": [SAMPLE_FIXTURE]}
        h2h = fetch_head_to_head(mock_client, 2, 26)
        assert len(h2h) == 1


class TestPullHeadToHead:
    def test_pull(self, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {"response": [SAMPLE_FIXTURE]}
        df = pull_head_to_head(mock_client, _fixtures_df(), output_dir=tmp_path)
        assert len(df) >= 1


class TestFetchEvents:
    def test_returns_events(self) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "response": [
                {
                    "type": "Goal",
                    "detail": "Normal Goal",
                    "team": {"id": 26, "name": "Argentina"},
                    "player": {"id": 1, "name": "Player"},
                }
            ]
        }
        events = fetch_events(mock_client, fixture_id=100)
        assert len(events) == 1


class TestPullEvents:
    def test_pull(self, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "response": [
                {
                    "type": "Goal",
                    "detail": "Normal Goal",
                    "team": {"id": 26, "name": "Argentina"},
                    "player": {"id": 1, "name": "P"},
                }
            ]
        }
        df = pull_events(mock_client, _fixtures_df(), min_year=2020, output_dir=tmp_path)
        assert len(df) >= 1


class TestPullPlayers:
    def test_pull(self, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "response": [
                {
                    "player": {"id": 1, "name": "Test", "age": 25, "nationality": "Test"},
                    "statistics": [
                        {
                            "team": {"id": 26},
                            "league": {"id": 1, "name": "WC", "season": 2022},
                            "games": {"appearences": 3, "position": "Forward", "rating": "7.0"},
                            "goals": {"total": 2, "assists": 1},
                            "cards": {"yellow": 0, "red": 0},
                        }
                    ],
                }
            ],
            "paging": {"current": 1, "total": 1},
        }
        df = pull_players(mock_client, _fixtures_df(), min_year=2020, output_dir=tmp_path)
        assert len(df) >= 1
