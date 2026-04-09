"""Pydantic models for API-Football v3 response objects."""

from __future__ import annotations

from datetime import datetime  # noqa: TCH003 — needed at runtime by Pydantic

from pydantic import BaseModel, Field

# ------------------------------------------------------------------
# Leagues
# ------------------------------------------------------------------


class Country(BaseModel):
    name: str
    code: str | None = None
    flag: str | None = None


class Season(BaseModel):
    year: int
    start: str | None = None
    end: str | None = None
    current: bool = False


class LeagueInfo(BaseModel):
    id: int
    name: str
    type: str | None = None
    logo: str | None = None


class League(BaseModel):
    league: LeagueInfo
    country: Country
    seasons: list[Season] = Field(default_factory=list)


# ------------------------------------------------------------------
# Teams
# ------------------------------------------------------------------


class TeamInfo(BaseModel):
    id: int
    name: str
    code: str | None = None
    country: str | None = None
    founded: int | None = None
    national: bool = False
    logo: str | None = None


class Venue(BaseModel):
    id: int | None = None
    name: str | None = None
    city: str | None = None
    capacity: int | None = None


class Team(BaseModel):
    team: TeamInfo
    venue: Venue | None = None


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


class FixtureInfo(BaseModel):
    id: int
    referee: str | None = None
    timezone: str | None = None
    date: datetime | None = None
    timestamp: int | None = None


class FixtureStatus(BaseModel):
    long: str | None = None
    short: str | None = None
    elapsed: int | None = None


class FixtureTeam(BaseModel):
    id: int
    name: str
    logo: str | None = None
    winner: bool | None = None


class FixtureTeams(BaseModel):
    home: FixtureTeam
    away: FixtureTeam


class Goals(BaseModel):
    home: int | None = None
    away: int | None = None


class ScoreEntry(BaseModel):
    home: int | None = None
    away: int | None = None


class Score(BaseModel):
    halftime: ScoreEntry | None = None
    fulltime: ScoreEntry | None = None
    extratime: ScoreEntry | None = None
    penalty: ScoreEntry | None = None


class FixtureLeague(BaseModel):
    id: int
    name: str | None = None
    country: str | None = None
    logo: str | None = None
    flag: str | None = None
    season: int | None = None
    round: str | None = None


class Fixture(BaseModel):
    fixture: FixtureInfo
    league: FixtureLeague
    teams: FixtureTeams
    goals: Goals
    score: Score | None = None


# ------------------------------------------------------------------
# Team Statistics (per league/season)
# ------------------------------------------------------------------


class GoalStats(BaseModel):
    total: int | None = None
    average: str | None = None


class GoalStatBlock(BaseModel):
    home: GoalStats | None = None
    away: GoalStats | None = None
    total: GoalStats | None = None


class WDLStat(BaseModel):
    home: int | None = None
    away: int | None = None
    total: int | None = None


class FixturesBlock(BaseModel):
    played: WDLStat | None = None
    wins: WDLStat | None = None
    draws: WDLStat | None = None
    loses: WDLStat | None = None


class TeamStatistics(BaseModel):
    league: FixtureLeague | None = None
    team: TeamInfo | None = None
    form: str | None = None
    fixtures: FixturesBlock | None = None
    goals: dict[str, GoalStatBlock | None] | None = Field(default=None, alias="goals")
    clean_sheet: WDLStat | None = None
    failed_to_score: WDLStat | None = None


# ------------------------------------------------------------------
# Players
# ------------------------------------------------------------------


class PlayerInfo(BaseModel):
    id: int
    name: str
    firstname: str | None = None
    lastname: str | None = None
    age: int | None = None
    nationality: str | None = None
    height: str | None = None
    weight: str | None = None
    photo: str | None = None


class PlayerStatGames(BaseModel):
    appearences: int | None = None  # API typo is intentional
    lineups: int | None = None
    minutes: int | None = None
    position: str | None = None
    rating: str | None = None


class PlayerStatGoals(BaseModel):
    total: int | None = None
    conceded: int | None = None
    assists: int | None = None


class PlayerStatCards(BaseModel):
    yellow: int | None = None
    red: int | None = None


class PlayerStatLeague(BaseModel):
    id: int | None = None
    name: str | None = None
    country: str | None = None
    season: int | None = None


class PlayerStatTeam(BaseModel):
    id: int | None = None
    name: str | None = None


class PlayerStatEntry(BaseModel):
    team: PlayerStatTeam | None = None
    league: PlayerStatLeague | None = None
    games: PlayerStatGames | None = None
    goals: PlayerStatGoals | None = None
    cards: PlayerStatCards | None = None


class Player(BaseModel):
    player: PlayerInfo
    statistics: list[PlayerStatEntry] = Field(default_factory=list)


# ------------------------------------------------------------------
# Head-to-Head (reuses Fixture model — response is a list of Fixture)
# ------------------------------------------------------------------

HeadToHead = list[Fixture]


# ------------------------------------------------------------------
# Fixture Events
# ------------------------------------------------------------------


class EventTime(BaseModel):
    elapsed: int | None = None
    extra: int | None = None


class EventTeam(BaseModel):
    id: int | None = None
    name: str | None = None
    logo: str | None = None


class EventPlayer(BaseModel):
    id: int | None = None
    name: str | None = None


class FixtureEvent(BaseModel):
    time: EventTime | None = None
    team: EventTeam | None = None
    player: EventPlayer | None = None
    assist: EventPlayer | None = None
    type: str | None = None
    detail: str | None = None
    comments: str | None = None


# ------------------------------------------------------------------
# Generic API response wrapper
# ------------------------------------------------------------------


class APIResponse(BaseModel):
    get: str | None = None
    parameters: dict[str, str] | None = None
    errors: list[str] | dict[str, str] = Field(default_factory=list)
    results: int = 0
    paging: dict[str, int] | None = None
    response: list[dict] = Field(default_factory=list)  # type: ignore[type-arg]
