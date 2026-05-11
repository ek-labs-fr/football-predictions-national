"""Tests for bivariate-Poisson calibration."""

from __future__ import annotations

import numpy as np
from scipy.stats import poisson

from src.models.calibrate import _bivariate_poisson_matrix, outcome_probs_bivariate


class TestBivariatePoissonMatrix:
    """Sign-convention and shape guarantees for the scoreline matrix.

    The card's reading (§6.4) is that positive ρ should *inflate* the
    diagonal (more draws than independence) and negative ρ should *deflate*
    it. Production ρ for national mode is −0.106, so the diagonal should
    carry less mass than the independent-Poisson baseline.
    """

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

    def test_independent_matrix_truncation_residual_is_small(self) -> None:
        mat = _bivariate_poisson_matrix(1.3, 1.1, 0.0)
        assert abs(mat.sum() - 1.0) < 1e-6

    def test_positive_rho_inflates_diagonal_relative_to_independence(self) -> None:
        lh, la = 1.3, 1.1
        base_diag = np.trace(_bivariate_poisson_matrix(lh, la, 0.0))
        pos_diag = np.trace(_bivariate_poisson_matrix(lh, la, 0.2))
        assert pos_diag > base_diag

    def test_negative_rho_deflates_diagonal_relative_to_independence(self) -> None:
        lh, la = 1.3, 1.1
        base_diag = np.trace(_bivariate_poisson_matrix(lh, la, 0.0))
        neg_diag = np.trace(_bivariate_poisson_matrix(lh, la, -0.2))
        assert neg_diag < base_diag

    def test_production_rho_deflates_diagonal(self) -> None:
        """ρ = −0.106 (national-mode production) must reduce draw mass."""
        lh, la = 1.3, 1.1
        base_diag = np.trace(_bivariate_poisson_matrix(lh, la, 0.0))
        prod_diag = np.trace(_bivariate_poisson_matrix(lh, la, -0.10609858))
        assert prod_diag < base_diag

    def test_draw_probability_is_monotone_in_rho(self) -> None:
        lh, la = 1.3, 1.1
        rhos = [-0.4, -0.2, 0.0, 0.2, 0.4]
        diagonals = [np.trace(_bivariate_poisson_matrix(lh, la, r)) for r in rhos]
        for a, b in zip(diagonals, diagonals[1:], strict=False):
            assert a < b, f"diagonal not monotone in ρ: {diagonals}"

    def test_off_diagonal_mass_moves_opposite_to_diagonal(self) -> None:
        lh, la = 1.3, 1.1
        base = _bivariate_poisson_matrix(lh, la, 0.0)
        deflated = _bivariate_poisson_matrix(lh, la, -0.2)
        base_off = base.sum() - np.trace(base)
        deflated_off = deflated.sum() - np.trace(deflated)
        assert deflated_off > base_off


class TestOutcomeProbsBivariate:
    def test_probabilities_sum_to_one(self) -> None:
        for rho in (-0.3, -0.1, 0.0, 0.1, 0.3):
            probs = outcome_probs_bivariate(1.3, 1.1, rho)
            total = probs["home_win"] + probs["draw"] + probs["away_win"]
            assert abs(total - 1.0) < 1e-9

    def test_positive_rho_raises_draw_probability(self) -> None:
        baseline = outcome_probs_bivariate(1.3, 1.1, 0.0)
        inflated = outcome_probs_bivariate(1.3, 1.1, 0.2)
        assert inflated["draw"] > baseline["draw"]

    def test_negative_rho_lowers_draw_probability(self) -> None:
        baseline = outcome_probs_bivariate(1.3, 1.1, 0.0)
        deflated = outcome_probs_bivariate(1.3, 1.1, -0.2)
        assert deflated["draw"] < baseline["draw"]

    def test_symmetric_lambdas_give_symmetric_win_probabilities(self) -> None:
        probs = outcome_probs_bivariate(1.2, 1.2, -0.1)
        assert abs(probs["home_win"] - probs["away_win"]) < 1e-9
