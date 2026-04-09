"""Pydantic request/response schemas for the prediction API."""

from __future__ import annotations

from pydantic import BaseModel, Field

# ------------------------------------------------------------------
# Predictions
# ------------------------------------------------------------------


class PredictRequest(BaseModel):
    """Request body for POST /predict."""

    home_team_id: int = Field(..., description="API-Football team ID for the home team")
    away_team_id: int = Field(..., description="API-Football team ID for the away team")
    league_id: int = Field(1, description="Competition league ID (default: World Cup)")
    match_date: str | None = Field(None, description="Match date (ISO format), defaults to today")


class ScorelineProbability(BaseModel):
    """Probability of a specific scoreline."""

    home_goals: int
    away_goals: int
    probability: float


class PredictResponse(BaseModel):
    """Response body for predictions."""

    home_team_id: int
    away_team_id: int
    lambda_home: float = Field(..., description="Expected goals for home team")
    lambda_away: float = Field(..., description="Expected goals for away team")
    most_likely_score: str = Field(..., description="Most probable scoreline, e.g. '1-0'")
    home_win: float = Field(..., description="P(home win)")
    draw: float = Field(..., description="P(draw)")
    away_win: float = Field(..., description="P(away win)")
    top_scorelines: list[ScorelineProbability] = Field(
        default_factory=list, description="Top 5 most likely scorelines"
    )


# ------------------------------------------------------------------
# Tournament Simulation
# ------------------------------------------------------------------


class SimulateRequest(BaseModel):
    """Request body for POST /simulate/tournament."""

    groups: dict[str, list[int]] = Field(
        ..., description="Group name → list of team IDs, e.g. {'A': [1, 2, 3, 4]}"
    )
    n_sims: int = Field(10000, description="Number of Monte Carlo simulations", ge=100, le=100000)


class TeamSimulationResult(BaseModel):
    """Per-team tournament simulation result."""

    team_id: int
    group_win_prob: float
    advance_prob: float
    qf_prob: float
    sf_prob: float
    final_prob: float
    champion_prob: float


class SimulateResponse(BaseModel):
    """Response body for tournament simulation."""

    n_sims: int
    results: list[TeamSimulationResult]


# ------------------------------------------------------------------
# Teams
# ------------------------------------------------------------------


class TeamResponse(BaseModel):
    """Response body for a single team."""

    id: int
    name: str
    country: str | None = None
    national: bool = True
    logo: str | None = None
    fifa_rank: int | None = None


class TeamListResponse(BaseModel):
    """Response body for team listing."""

    teams: list[TeamResponse]
    total: int


# ------------------------------------------------------------------
# Match listing with performance tracking
# ------------------------------------------------------------------


class MatchResultResponse(BaseModel):
    """A single match with predicted and actual scores."""

    fixture_id: int
    date: str
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
    predicted_home_goals: float
    predicted_away_goals: float
    predicted_score: str
    actual_home_goals: int | None = None
    actual_away_goals: int | None = None
    actual_score: str | None = None
    predicted_outcome: str
    actual_outcome: str | None = None
    correct_outcome: bool | None = None
    correct_score: bool | None = None
    home_win_prob: float
    draw_prob: float
    away_win_prob: float
    league_name: str = ""
    round: str = ""


class PerformanceSummaryResponse(BaseModel):
    """Running algorithm performance stats."""

    total_matches: int
    completed_matches: int
    correct_outcomes: int
    correct_scores: int
    outcome_accuracy: float
    score_accuracy: float
    avg_mae: float


class MatchListResponse(BaseModel):
    """Response body for GET /matches."""

    matches: list[MatchResultResponse]
    performance: PerformanceSummaryResponse


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------


class HealthResponse(BaseModel):
    """Response body for GET /health."""

    status: str = "ok"
    model_loaded: bool = False
    version: str = "0.1.0"
