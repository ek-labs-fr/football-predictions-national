"""Shared dependencies for the prediction API: model loader, feature store."""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

import joblib
import numpy as np  # noqa: TCH002

from src.models.calibrate import RhoConfig, load_rho_config

logger = logging.getLogger(__name__)

ARTEFACTS_DIR = Path("artefacts")
PROCESSED_DIR = Path("data/processed")


class ModelStore:
    """Loads and holds trained model artefacts in memory."""

    def __init__(self, artefacts_dir: Path = ARTEFACTS_DIR) -> None:
        self.artefacts_dir = artefacts_dir
        self.model_home: Any = None
        self.model_away: Any = None
        self.scaler: Any = None
        self.rho_config: RhoConfig = RhoConfig(default=0.0)
        self.selected_features: list[str] = []
        self.loaded = False

    @property
    def rho(self) -> float:
        """Default (cross-bucket) ρ — kept for callers that need a scalar."""
        return self.rho_config.default

    def load(self) -> None:
        """Load all model artefacts from disk."""
        d = self.artefacts_dir

        home_path = d / "model_home_calibrated.pkl"
        away_path = d / "model_away_calibrated.pkl"
        scaler_path = d / "scaler.pkl"
        rho_path = d / "rho.json"
        features_path = d / "selected_features.pkl"

        if home_path.exists() and away_path.exists():
            self.model_home = joblib.load(home_path)
            self.model_away = joblib.load(away_path)
            logger.info("Loaded calibrated model pair")
        else:
            # Fall back to any available model
            for prefix in ("xgboost_poisson", "lightgbm_poisson", "poisson_linear"):
                h = d / f"{prefix}_home.pkl"
                a = d / f"{prefix}_away.pkl"
                if h.exists() and a.exists():
                    self.model_home = joblib.load(h)
                    self.model_away = joblib.load(a)
                    logger.info("Loaded model pair: %s", prefix)
                    break

        if scaler_path.exists():
            self.scaler = joblib.load(scaler_path)
            logger.info("Loaded scaler")

        if rho_path.exists():
            payload = json.loads(rho_path.read_text(encoding="utf-8"))
            self.rho_config = load_rho_config(payload)
            if self.rho_config.by_bucket:
                logger.info(
                    "Loaded RhoConfig (default=%.4f, buckets=%s)",
                    self.rho_config.default,
                    {b: round(v, 4) for b, v in self.rho_config.by_bucket.items()},
                )
            else:
                logger.info("Loaded ρ = %.4f (scalar)", self.rho_config.default)

        if features_path.exists():
            self.selected_features = joblib.load(features_path)
            logger.info("Loaded %d selected features", len(self.selected_features))

        self.loaded = self.model_home is not None and self.model_away is not None

    def predict(self, features: np.ndarray) -> tuple[float, float]:
        """Predict home/away λ from a feature vector.

        Parameters
        ----------
        features:
            1D or 2D array of feature values.

        Returns
        -------
        (lambda_home, lambda_away)
        """
        if not self.loaded:
            raise RuntimeError("Models not loaded — call load() first")

        X = features.reshape(1, -1) if features.ndim == 1 else features
        if self.scaler is not None:
            X = self.scaler.transform(X)

        lh = float(self.model_home.predict(X)[0])
        la = float(self.model_away.predict(X)[0])
        return max(lh, 0.01), max(la, 0.01)


class FeatureStore:
    """Loads processed data for building feature rows at inference time."""

    def __init__(self, processed_dir: Path = PROCESSED_DIR) -> None:
        self.processed_dir = processed_dir
        self.team_lookup: dict[str, int] = {}
        self.team_lookup_reverse: dict[int, str] = {}
        self.loaded = False

    def load(self) -> None:
        """Load team lookup and any other reference data."""
        lookup_path = self.processed_dir / "team_lookup.json"
        if lookup_path.exists():
            self.team_lookup = json.loads(lookup_path.read_text(encoding="utf-8"))
            self.team_lookup_reverse = {v: k for k, v in self.team_lookup.items()}
            logger.info("Loaded team lookup (%d teams)", len(self.team_lookup))
        self.loaded = True

    def get_team_name(self, team_id: int) -> str | None:
        """Look up team name by ID."""
        return self.team_lookup_reverse.get(team_id)

    def get_team_id(self, name: str) -> int | None:
        """Look up team ID by name."""
        return self.team_lookup.get(name)


@lru_cache
def get_model_store() -> ModelStore:
    """Singleton model store for FastAPI dependency injection."""
    return ModelStore()


@lru_cache
def get_feature_store() -> FeatureStore:
    """Singleton feature store for FastAPI dependency injection."""
    return FeatureStore()
