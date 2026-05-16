"""Tests for FastAPI endpoints using httpx TestClient."""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
from fastapi.testclient import TestClient

from src.api.dependencies import FeatureStore, ModelStore, get_feature_store, get_model_store
from src.api.main import app
from src.models.calibrate import RhoConfig

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


def _mock_model_store(loaded: bool = True) -> ModelStore:
    store = ModelStore()
    store.loaded = loaded
    store.model_home = MagicMock()
    store.model_away = MagicMock()
    store.model_home.predict.return_value = np.array([1.5])
    store.model_away.predict.return_value = np.array([1.2])
    store.scaler = None
    store.rho_config = RhoConfig(default=0.05)
    store.selected_features = ["f1", "f2"]
    return store


def _mock_feature_store() -> FeatureStore:
    store = FeatureStore()
    store.team_lookup = {"France": 2, "Brazil": 6, "Argentina": 26, "Germany": 25}
    store.team_lookup_reverse = {v: k for k, v in store.team_lookup.items()}
    store.loaded = True
    return store


def _get_client(model_loaded: bool = True) -> TestClient:
    """Create a test client with mocked dependencies."""
    mock_model = _mock_model_store(loaded=model_loaded)
    mock_features = _mock_feature_store()

    app.dependency_overrides[get_model_store] = lambda: mock_model
    app.dependency_overrides[get_feature_store] = lambda: mock_features

    client = TestClient(app)
    return client


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------


class TestHealth:
    def test_health_ok(self) -> None:
        client = _get_client()
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["model_loaded"] is True

    def test_health_no_model(self) -> None:
        client = _get_client(model_loaded=False)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["model_loaded"] is False


# ------------------------------------------------------------------
# Teams
# ------------------------------------------------------------------


class TestTeams:
    def test_list_teams(self) -> None:
        client = _get_client()
        resp = client.get("/teams")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 4
        names = {t["name"] for t in data["teams"]}
        assert "France" in names
        assert "Brazil" in names

    def test_get_team(self) -> None:
        client = _get_client()
        resp = client.get("/teams/2")
        assert resp.status_code == 200
        assert resp.json()["name"] == "France"

    def test_get_team_not_found(self) -> None:
        client = _get_client()
        resp = client.get("/teams/999")
        assert resp.status_code == 404


# ------------------------------------------------------------------
# Predictions
# ------------------------------------------------------------------


class TestPredictions:
    def test_predict_success(self) -> None:
        client = _get_client()
        resp = client.post("/predict", json={"home_team_id": 2, "away_team_id": 6})
        assert resp.status_code == 200
        data = resp.json()
        assert data["home_team_id"] == 2
        assert data["away_team_id"] == 6
        assert "lambda_home" in data
        assert "lambda_away" in data
        assert "most_likely_score" in data
        assert "home_win" in data
        assert "draw" in data
        assert "away_win" in data
        assert len(data["top_scorelines"]) == 5
        # Probabilities should sum to ~1
        total = data["home_win"] + data["draw"] + data["away_win"]
        assert abs(total - 1.0) < 0.01

    def test_predict_unknown_team(self) -> None:
        client = _get_client()
        resp = client.post("/predict", json={"home_team_id": 999, "away_team_id": 6})
        assert resp.status_code == 404

    def test_predict_model_not_loaded(self) -> None:
        client = _get_client(model_loaded=False)
        resp = client.post("/predict", json={"home_team_id": 2, "away_team_id": 6})
        assert resp.status_code == 503


# ------------------------------------------------------------------
# Simulation
# ------------------------------------------------------------------


class TestSimulation:
    def test_simulate_tournament(self) -> None:
        client = _get_client()
        resp = client.post(
            "/simulate/tournament",
            json={
                "groups": {"A": [2, 6, 25, 26]},
                "n_sims": 100,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["n_sims"] == 100
        assert len(data["results"]) == 4
        for r in data["results"]:
            assert "champion_prob" in r
            assert 0 <= r["advance_prob"] <= 1

    def test_simulate_model_not_loaded(self) -> None:
        client = _get_client(model_loaded=False)
        resp = client.post(
            "/simulate/tournament",
            json={"groups": {"A": [2, 6, 25, 26]}, "n_sims": 100},
        )
        assert resp.status_code == 503

    def test_simulate_invalid_group(self) -> None:
        client = _get_client()
        resp = client.post(
            "/simulate/tournament",
            json={"groups": {"A": [2, 6]}, "n_sims": 100},  # only 2 teams
        )
        assert resp.status_code == 422
