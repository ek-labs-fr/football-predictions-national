"""Tests for bivariate-Poisson calibration and per-competition ρ resolution."""

from __future__ import annotations

import json

import numpy as np
import pytest
from scipy.stats import poisson

from src.models.calibrate import (
    DEFAULT_BUCKETS,
    RhoConfig,
    _bivariate_poisson_matrix,
    _fit_rho_brier,
    fit_rho_per_bucket,
    load_rho_config,
    outcome_probs_bivariate,
    save_rho_config,
)


class TestBivariatePoissonMatrix:
    def test_rho_zero_equals_independent_outer_product(self) -> None:
        lh, la = 1.3, 1.1
        mat = _bivariate_poisson_matrix(lh, la, 0.0)
        expected = np.outer(
            poisson.pmf(np.arange(11), lh),
            poisson.pmf(np.arange(11), la),
        )
        np.testing.assert_allclose(mat, expected, atol=1e-12)

    def test_matrix_sums_to_one_after_renormalization(self) -> None:
        for rho in (-0.3, -0.1, 0.1, 0.3):
            mat = _bivariate_poisson_matrix(1.3, 1.1, rho)
            assert abs(mat.sum() - 1.0) < 1e-9

    def test_positive_rho_inflates_diagonal(self) -> None:
        lh, la = 1.3, 1.1
        base = np.trace(_bivariate_poisson_matrix(lh, la, 0.0))
        pos = np.trace(_bivariate_poisson_matrix(lh, la, 0.2))
        assert pos > base

    def test_negative_rho_deflates_diagonal(self) -> None:
        lh, la = 1.3, 1.1
        base = np.trace(_bivariate_poisson_matrix(lh, la, 0.0))
        neg = np.trace(_bivariate_poisson_matrix(lh, la, -0.2))
        assert neg < base


class TestRhoConfigLookup:
    def test_default_returned_for_unknown_league(self) -> None:
        cfg = RhoConfig(default=-0.1)
        assert cfg.lookup(999) == pytest.approx(-0.1)

    def test_default_returned_for_none_league(self) -> None:
        cfg = RhoConfig(default=-0.1)
        assert cfg.lookup(None) == pytest.approx(-0.1)

    def test_bucket_resolved_when_league_mapped(self) -> None:
        cfg = RhoConfig(
            default=0.05,
            by_bucket={"wc": -0.10, "continental": 0.10},
            league_to_bucket={1: "wc", 4: "continental", 9: "continental"},
        )
        assert cfg.lookup(1) == pytest.approx(-0.10)
        assert cfg.lookup(4) == pytest.approx(0.10)
        assert cfg.lookup(9) == pytest.approx(0.10)

    def test_bucket_falls_back_to_default_if_missing_value(self) -> None:
        cfg = RhoConfig(
            default=0.05,
            by_bucket={"wc": -0.10},
            league_to_bucket={1: "wc", 99: "ghost-bucket"},
        )
        # Mapped to a bucket that has no ρ → default
        assert cfg.lookup(99) == pytest.approx(0.05)
        # Mapped league not in league_to_bucket → default
        assert cfg.lookup(42) == pytest.approx(0.05)

    def test_string_or_int_league_id_works(self) -> None:
        cfg = RhoConfig(
            default=0.0,
            by_bucket={"wc": -0.10},
            league_to_bucket={1: "wc"},
        )
        # Lookup accepts int-castable inputs
        assert cfg.lookup(1) == pytest.approx(-0.10)


class TestRhoConfigSerialization:
    def test_legacy_scalar_payload_roundtrips_through_loader(self) -> None:
        cfg = load_rho_config({"rho": -0.106})
        assert cfg.default == pytest.approx(-0.106)
        assert cfg.by_bucket == {}
        assert cfg.league_to_bucket == {}
        # Legacy scalar → every lookup returns the same value
        for lid in (1, 4, 999, None):
            assert cfg.lookup(lid) == pytest.approx(-0.106)

    def test_per_bucket_payload_roundtrips(self) -> None:
        payload = {
            "rho_default": 0.05,
            "rho_by_bucket": {"wc": -0.10, "continental": 0.10},
            "bucket_league_ids": {"wc": [1], "continental": [4, 6, 9]},
        }
        cfg = load_rho_config(payload)
        assert cfg.default == pytest.approx(0.05)
        assert cfg.by_bucket == {"wc": -0.10, "continental": 0.10}
        assert cfg.league_to_bucket == {
            1: "wc",
            4: "continental",
            6: "continental",
            9: "continental",
        }

        # And the inverse direction
        new_payload = cfg.to_payload()
        assert new_payload["rho_default"] == pytest.approx(0.05)
        assert new_payload["rho_by_bucket"] == {"wc": -0.10, "continental": 0.10}
        assert sorted(new_payload["bucket_league_ids"]["continental"]) == [4, 6, 9]

    def test_invalid_payload_raises(self) -> None:
        with pytest.raises(ValueError, match="must contain"):
            load_rho_config({})

    def test_save_rho_config_writes_new_schema(self, tmp_path) -> None:
        cfg = RhoConfig(
            default=0.05,
            by_bucket={"wc": -0.10},
            league_to_bucket={1: "wc"},
        )
        out = tmp_path / "rho.json"
        save_rho_config(cfg, out)
        loaded = json.loads(out.read_text(encoding="utf-8"))
        assert loaded["rho_default"] == pytest.approx(0.05)
        assert loaded["rho_by_bucket"] == {"wc": -0.10}
        assert loaded["bucket_league_ids"] == {"wc": [1]}


class TestFitRhoPerBucket:
    def _synth(
        self,
        n: int,
        league_ids: np.ndarray,
        draw_rate: float,
        seed: int,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Synthesise λs + draw indicators with a target draw rate."""
        rng = np.random.default_rng(seed)
        lh = rng.uniform(0.8, 1.8, n)
        la = rng.uniform(0.8, 1.8, n)
        is_draw = (rng.random(n) < draw_rate).astype(float)
        return lh, la, is_draw

    def test_fitter_returns_per_bucket_values(self) -> None:
        # 200 WC (low draw rate) + 200 continental (high draw rate)
        league_ids = np.array([1] * 200 + [4] * 200)
        lh_wc, la_wc, draw_wc = self._synth(200, league_ids[:200], draw_rate=0.10, seed=1)
        lh_cn, la_cn, draw_cn = self._synth(200, league_ids[200:], draw_rate=0.40, seed=2)
        lh = np.concatenate([lh_wc, lh_cn])
        la = np.concatenate([la_wc, la_cn])
        is_draw = np.concatenate([draw_wc, draw_cn])

        cfg = fit_rho_per_bucket(
            lambdas_home=lh,
            lambdas_away=la,
            is_draw=is_draw,
            league_ids=league_ids,
            buckets={"wc": [1], "continental": [4]},
            min_samples=30,
        )

        assert "wc" in cfg.by_bucket
        assert "continental" in cfg.by_bucket
        # Low-draw bucket should fit negative ρ; high-draw bucket positive ρ.
        assert cfg.by_bucket["wc"] < 0
        assert cfg.by_bucket["continental"] > 0
        # Bucket ρ differs from cross-bucket default — that's the whole point.
        assert cfg.by_bucket["wc"] != pytest.approx(cfg.default, abs=1e-3)

    def test_small_bucket_falls_back_to_default(self, caplog) -> None:
        # 5 rows in WC bucket, 200 in continental — wc should fall back.
        league_ids = np.array([1] * 5 + [4] * 200)
        lh, la, is_draw = self._synth(205, league_ids, draw_rate=0.25, seed=3)

        with caplog.at_level("WARNING"):
            cfg = fit_rho_per_bucket(
                lambdas_home=lh,
                lambdas_away=la,
                is_draw=is_draw,
                league_ids=league_ids,
                buckets={"wc": [1], "continental": [4]},
                min_samples=30,
            )

        assert cfg.by_bucket["wc"] == pytest.approx(cfg.default)
        assert any("falling back" in r.message for r in caplog.records)

    def test_default_buckets_constant_contains_wc_and_continental(self) -> None:
        assert DEFAULT_BUCKETS["wc"] == [1]
        # Continental majors: Euro=4, AFCON=6, Asian Cup=7, Copa=9, Gold Cup=22
        assert set(DEFAULT_BUCKETS["continental"]) == {4, 6, 7, 9, 22}


class TestFitRhoBrierLossDirection:
    """The Brier-on-draws loss should fit ρ in the right direction.

    If the observed draw rate exceeds the independence-implied rate, the
    fitted ρ should be positive (inflate diagonal). If below, negative.
    """

    def test_high_draw_rate_yields_positive_rho(self) -> None:
        n = 300
        lh = np.full(n, 1.3)
        la = np.full(n, 1.1)
        # Force ~50% draws — far higher than the independent-Poisson rate
        is_draw = (np.arange(n) % 2 == 0).astype(float)
        rho, _ = _fit_rho_brier(lh, la, is_draw)
        assert rho > 0

    def test_low_draw_rate_yields_negative_rho(self) -> None:
        n = 300
        lh = np.full(n, 1.3)
        la = np.full(n, 1.1)
        # Force ~5% draws — well below the independent-Poisson rate
        is_draw = (np.arange(n) % 20 == 0).astype(float)
        rho, _ = _fit_rho_brier(lh, la, is_draw)
        assert rho < 0


class TestOutcomeProbsBivariate:
    def test_probabilities_sum_to_one(self) -> None:
        for rho in (-0.3, 0.0, 0.3):
            p = outcome_probs_bivariate(1.3, 1.1, rho)
            assert abs(p["home_win"] + p["draw"] + p["away_win"] - 1.0) < 1e-9

    def test_symmetric_lambdas_give_symmetric_win_probs(self) -> None:
        p = outcome_probs_bivariate(1.2, 1.2, -0.1)
        assert abs(p["home_win"] - p["away_win"]) < 1e-9
