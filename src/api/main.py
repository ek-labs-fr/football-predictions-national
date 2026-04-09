"""FastAPI application entrypoint with CORS, lifespan, and router includes."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator  # noqa: TCH003
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.dependencies import get_feature_store, get_model_store
from src.api.predictions_store import get_predictions_store
from src.api.routes import health, predictions, simulate, teams

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Load models and feature store on startup."""
    logger.info("Loading model artefacts...")
    model_store = get_model_store()
    model_store.load()
    if model_store.loaded:
        logger.info("Models loaded successfully")
    else:
        logger.warning("No models found — predictions will return 503")

    feature_store = get_feature_store()
    feature_store.load()
    logger.info("Feature store loaded (%d teams)", len(feature_store.team_lookup))

    logger.info("Loading predictions store...")
    pred_store = get_predictions_store()
    pred_store.load(
        model_home=model_store.model_home,
        model_away=model_store.model_away,
        scaler=model_store.scaler,
        selected_features=model_store.selected_features or None,
    )
    logger.info("Predictions store loaded (%d matches)", len(pred_store.matches))

    yield

    logger.info("Shutting down")


app = FastAPI(
    title="Football Predictions National",
    description="Poisson-based national team match scoreline predictions and tournament simulation",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — allow all origins in development, restrict in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include route modules
app.include_router(health.router)
app.include_router(predictions.router)
app.include_router(teams.router)
app.include_router(simulate.router)
