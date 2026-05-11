"""Tests for the inference version-stamping behavior."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from src.features import io as feature_io
from src.inference.predict import (
    _DECISION_RULE_VERSION,
    _store_prediction,
    _to_records,
    _top_k_scorelines,
)
from src.models.calibrate import _bivariate_poisson_matrix

if TYPE_CHECKING:
    from pathlib import Path


class TestVersionStamping:
    def _row(self) -> pd.Series:
        return pd.Series(
            {
                "fixture_id": 12345,
                "lambda_home": 1.55,
                "lambda_away": 0.77,
                "predicted_score": "1-0",
                "p_home_win": 0.55,
                "p_draw": 0.27,
                "p_away_win": 0.18,
                "predicted_outcome": "home_win",
                "top_scorelines": [
                    {"score": "1-0", "probability": 0.2150},
                    {"score": "1-1", "probability": 0.1750},
                    {"score": "2-0", "probability": 0.1670},
                ],
            }
        )

    def test_decision_rule_version_constant(self) -> None:
        assert _DECISION_RULE_VERSION == "argmax_v0"

    def test_store_writes_decision_rule_and_trained_at(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("DATA_BUCKET", raising=False)
        feature_io._client.cache_clear()

        payload = _store_prediction(
            fid=12345,
            prediction_row=self._row(),
            backfill=False,
            model_trained_at="2026-04-26T20:41:12+00:00",
        )

        assert payload["decision_rule_version"] == "argmax_v0"
        assert payload["model_trained_at"] == "2026-04-26T20:41:12+00:00"
        assert payload["fixture_id"] == 12345

        on_disk = feature_io.read_json("predictions/12345.json")
        assert on_disk["decision_rule_version"] == "argmax_v0"
        assert on_disk["model_trained_at"] == "2026-04-26T20:41:12+00:00"

    def test_store_handles_missing_trained_at(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("DATA_BUCKET", raising=False)
        feature_io._client.cache_clear()

        payload = _store_prediction(
            fid=99999,
            prediction_row=self._row(),
            backfill=False,
            model_trained_at=None,
        )

        assert payload["model_trained_at"] is None
        assert payload["decision_rule_version"] == "argmax_v0"

    def test_store_persists_top_scorelines(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("DATA_BUCKET", raising=False)
        feature_io._client.cache_clear()

        _store_prediction(
            fid=42,
            prediction_row=self._row(),
            backfill=False,
            model_trained_at=None,
        )

        on_disk = feature_io.read_json("predictions/42.json")
        assert "top_scorelines" in on_disk
        assert len(on_disk["top_scorelines"]) == 3
        assert on_disk["top_scorelines"][0]["score"] == "1-0"
        assert on_disk["top_scorelines"][0]["probability"] == 0.2150


class TestTopKScorelines:
    def test_returns_three_entries_sorted_descending(self) -> None:
        mat = _bivariate_poisson_matrix(1.3, 1.1, -0.106)
        top = _top_k_scorelines(mat)
        assert len(top) == 3
        probs = [t["probability"] for t in top]
        assert probs == sorted(probs, reverse=True)

    def test_first_entry_matches_argmax(self) -> None:
        mat = _bivariate_poisson_matrix(1.3, 1.1, -0.106)
        top = _top_k_scorelines(mat)
        idx = np.unravel_index(mat.argmax(), mat.shape)
        assert top[0]["score"] == f"{int(idx[0])}-{int(idx[1])}"

    def test_each_entry_has_score_and_probability(self) -> None:
        mat = _bivariate_poisson_matrix(1.3, 1.1, -0.106)
        for entry in _top_k_scorelines(mat):
            assert set(entry.keys()) == {"score", "probability"}
            h_str, a_str = entry["score"].split("-")
            assert int(h_str) >= 0
            assert int(a_str) >= 0
            assert 0.0 <= entry["probability"] <= 1.0

    def test_probabilities_use_python_floats(self) -> None:
        """JSON-serializable: not numpy types."""
        mat = _bivariate_poisson_matrix(1.3, 1.1, -0.106)
        for entry in _top_k_scorelines(mat):
            assert isinstance(entry["probability"], float)
            assert not isinstance(entry["probability"], np.floating)

    def test_strong_home_favorite_top_includes_1_0(self) -> None:
        """Sanity: with λ_h=1.3, λ_a=0.5, 1-0 should be in the top cells."""
        mat = _bivariate_poisson_matrix(1.3, 0.5, -0.1)
        scores = {t["score"] for t in _top_k_scorelines(mat)}
        assert "1-0" in scores


class TestToRecordsBackwardCompat:
    """Legacy frozen predictions lack ``top_scorelines``; serialization must
    survive without emitting invalid JSON (``NaN``)."""

    def test_legacy_row_emits_none_for_top_scorelines(self) -> None:
        import json

        df = pd.DataFrame(
            [
                {
                    "fixture_id": 1,
                    "predicted_score": "1-0",
                    "top_scorelines": [{"score": "1-0", "probability": 0.21}],
                },
                {
                    "fixture_id": 2,
                    "predicted_score": "0-0",
                    "top_scorelines": np.nan,
                },
            ]
        )
        records = _to_records(df, ["fixture_id", "predicted_score", "top_scorelines"])

        assert records[0]["top_scorelines"] == [{"score": "1-0", "probability": 0.21}]
        assert records[1]["top_scorelines"] is None
        json.dumps(records)  # must not raise


class TestLastModified:
    def test_returns_iso_timestamp_for_existing_file(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        monkeypatch.delenv("DATA_BUCKET", raising=False)
        feature_io._client.cache_clear()

        f = tmp_path / "model.pkl"
        f.write_bytes(b"x")
        ts = feature_io.last_modified(f)

        assert ts is not None
        assert ts.endswith("+00:00")
        assert "T" in ts
        assert len(ts) == 25

    def test_returns_none_for_missing_file(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        monkeypatch.delenv("DATA_BUCKET", raising=False)
        feature_io._client.cache_clear()

        assert feature_io.last_modified(tmp_path / "missing.pkl") is None
