"""Prediction endpoints: POST /predict and GET /predictions/{fixture_id}."""

from __future__ import annotations

import numpy as np
from fastapi import APIRouter, Depends, HTTPException

from src.api.dependencies import FeatureStore, ModelStore, get_feature_store, get_model_store
from src.api.models import (
    MatchListResponse,
    MatchResultResponse,
    PredictRequest,
    PredictResponse,
    ScorelineProbability,
)
from src.api.predictions_store import PredictionsStore, get_predictions_store
from src.models.train import predict_match, scoreline_matrix

router = APIRouter(tags=["predictions"])


@router.post("/predict", response_model=PredictResponse)
def predict(
    req: PredictRequest,
    model_store: ModelStore = Depends(get_model_store),
    feature_store: FeatureStore = Depends(get_feature_store),
) -> PredictResponse:
    """Predict the scoreline and outcome probabilities for a match."""
    if not model_store.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")

    # Validate team IDs exist
    home_name = feature_store.get_team_name(req.home_team_id)
    away_name = feature_store.get_team_name(req.away_team_id)
    if home_name is None:
        raise HTTPException(status_code=404, detail=f"Home team {req.home_team_id} not found")
    if away_name is None:
        raise HTTPException(status_code=404, detail=f"Away team {req.away_team_id} not found")

    # Build a minimal feature vector
    # In production, this would query the feature store for rolling/squad/H2H features.
    # For now, use a zero vector with the correct number of features.
    n_features = len(model_store.selected_features) if model_store.selected_features else 1
    features = np.zeros(n_features)

    try:
        lambda_home, lambda_away = model_store.predict(features)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}") from e

    result = predict_match(lambda_home, lambda_away)

    # Top 5 most likely scorelines
    mat = scoreline_matrix(lambda_home, lambda_away)
    flat_indices = np.argsort(mat.ravel())[::-1][:5]
    top_scorelines = []
    for idx in flat_indices:
        h, a = divmod(idx, mat.shape[1])
        prob = round(float(mat[h, a]), 4)
        top_scorelines.append(
            ScorelineProbability(home_goals=int(h), away_goals=int(a), probability=prob)
        )

    return PredictResponse(
        home_team_id=req.home_team_id,
        away_team_id=req.away_team_id,
        lambda_home=result["lambda_home"],
        lambda_away=result["lambda_away"],
        most_likely_score=result["most_likely_score"],
        home_win=result["home_win"],
        draw=result["draw"],
        away_win=result["away_win"],
        top_scorelines=top_scorelines,
    )


@router.get("/matches", response_model=MatchListResponse)
def list_matches(
    pred_store: PredictionsStore = Depends(get_predictions_store),
) -> MatchListResponse:
    """List all matches with predicted and actual scores, plus running performance."""
    return pred_store.get_response()


@router.get("/matches/{fixture_id}", response_model=MatchResultResponse)
def get_match(
    fixture_id: int,
    pred_store: PredictionsStore = Depends(get_predictions_store),
) -> MatchResultResponse:
    """Get a single match prediction by fixture ID."""
    match = pred_store.get_match(fixture_id)
    if match is None:
        raise HTTPException(status_code=404, detail=f"Match {fixture_id} not found")
    return match
