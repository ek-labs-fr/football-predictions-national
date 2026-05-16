"""Tournament simulation endpoint."""

from __future__ import annotations

import numpy as np
from fastapi import APIRouter, Depends, HTTPException

from src.api.dependencies import ModelStore, get_model_store
from src.api.models import SimulateRequest, SimulateResponse, TeamSimulationResult
from src.models.simulate import simulate_tournament

router = APIRouter(prefix="/simulate", tags=["simulation"])


@router.post("/tournament", response_model=SimulateResponse)
def simulate_tournament_endpoint(
    req: SimulateRequest,
    model_store: ModelStore = Depends(get_model_store),
) -> SimulateResponse:
    """Run Monte Carlo tournament simulation."""
    if not model_store.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")

    # Validate groups have at least 3 teams each
    for group_name, team_ids in req.groups.items():
        if len(team_ids) < 3:
            raise HTTPException(
                status_code=422,
                detail=f"Group {group_name} must have at least 3 teams, got {len(team_ids)}",
            )

    def get_lambdas(home_id: int, away_id: int) -> tuple[float, float]:
        """Get predicted λ values for a matchup using a zero feature vector."""
        n_features = len(model_store.selected_features) if model_store.selected_features else 1
        features = np.zeros(n_features)
        return model_store.predict(features)

    rng = np.random.default_rng(42)
    # Tournament simulation is currently WC-only (league_id=1). Look up the
    # competition-specific ρ rather than the cross-bucket default — for WC
    # fixtures the WC bucket is what's calibrated against the WC draw rate.
    rho = model_store.rho_config.lookup(1)
    result_df = simulate_tournament(
        groups=req.groups,
        get_lambdas=get_lambdas,
        n_sims=req.n_sims,
        rho=rho,
        rng=rng,
    )

    results = [TeamSimulationResult(**row) for row in result_df.to_dict(orient="records")]

    return SimulateResponse(n_sims=req.n_sims, results=results)
