"""Tests for Pydantic schema parsing of API-Football responses."""

from __future__ import annotations

import json
from pathlib import Path

from src.data.schemas import (
    Fixture,
    FixtureEvent,
    League,
    Player,
    Team,
)

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"


def _load(name: str) -> dict:  # type: ignore[type-arg]
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


class TestLeagueSchema:
    def test_parse_league(self) -> None:
        data = _load("sample_league.json")
        league = League.model_validate(data)
        assert league.league.id == 1
        assert league.league.name == "World Cup"
        assert league.country.name == "World"
        assert len(league.seasons) == 2
        assert league.seasons[0].year == 2022

    def test_missing_optional_fields(self) -> None:
        minimal = {"league": {"id": 99, "name": "Test Cup"}, "country": {"name": "Test"}}
        league = League.model_validate(minimal)
        assert league.league.id == 99
        assert league.seasons == []


class TestTeamSchema:
    def test_parse_team(self) -> None:
        data = _load("sample_team.json")
        team = Team.model_validate(data)
        assert team.team.id == 2
        assert team.team.name == "France"
        assert team.team.national is True
        assert team.venue is not None
        assert team.venue.capacity == 81338

    def test_missing_venue(self) -> None:
        minimal = {"team": {"id": 1, "name": "Brazil"}}
        team = Team.model_validate(minimal)
        assert team.venue is None


class TestFixtureSchema:
    def test_parse_fixture(self) -> None:
        data = _load("sample_fixture.json")
        fixture = Fixture.model_validate(data)
        assert fixture.fixture.id == 855748
        assert fixture.teams.home.name == "Argentina"
        assert fixture.teams.away.name == "France"
        assert fixture.goals.home == 3
        assert fixture.goals.away == 3
        assert fixture.score is not None
        assert fixture.score.penalty is not None
        assert fixture.score.penalty.home == 4

    def test_fixture_with_nulls(self) -> None:
        minimal = {
            "fixture": {"id": 1},
            "league": {"id": 1},
            "teams": {
                "home": {"id": 10, "name": "TeamA"},
                "away": {"id": 20, "name": "TeamB"},
            },
            "goals": {},
        }
        fixture = Fixture.model_validate(minimal)
        assert fixture.goals.home is None
        assert fixture.score is None


class TestPlayerSchema:
    def test_parse_player(self) -> None:
        data = _load("sample_player.json")
        player = Player.model_validate(data)
        assert player.player.id == 276
        assert player.player.name == "K. Mbappé"
        assert len(player.statistics) == 1
        stat = player.statistics[0]
        assert stat.goals is not None
        assert stat.goals.total == 8
        assert stat.games is not None
        assert stat.games.rating == "7.8"

    def test_player_no_stats(self) -> None:
        minimal = {"player": {"id": 1, "name": "Unknown"}}
        player = Player.model_validate(minimal)
        assert player.statistics == []


class TestFixtureEventSchema:
    def test_parse_event(self) -> None:
        data = _load("sample_event.json")
        event = FixtureEvent.model_validate(data)
        assert event.type == "Goal"
        assert event.detail == "Normal Goal"
        assert event.player is not None
        assert event.player.name == "L. Messi"
        assert event.time is not None
        assert event.time.elapsed == 23

    def test_minimal_event(self) -> None:
        event = FixtureEvent.model_validate({})
        assert event.type is None
        assert event.team is None
