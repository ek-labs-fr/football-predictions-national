"""Probability calibration with bivariate Poisson correlation (Step 3.7).

Fits a correlation parameter ρ to correct draw probabilities by adjusting
the independent Poisson assumption. Supports a global scalar ρ (legacy) and
a per-competition mapping that routes matches to bucket-specific ρ values
via ``RhoConfig`` — necessary because different competition types have
structurally different draw rates (WC ~22% vs continental majors ~26-28%).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import joblib
import numpy as np
from scipy.optimize import minimize_scalar
from scipy.stats import poisson

from src.models.train import SplitData, TrainedModel, predict_lambdas

logger = logging.getLogger(__name__)

ARTEFACTS_DIR = Path("artefacts")
_MAX_GOALS = 10

# Default per-competition buckets for national mode. League IDs:
#   1=World Cup, 4=Euro, 6=AFCON, 7=Asian Cup, 9=Copa América, 22=Gold Cup.
# Friendlies, qualifying, Nations League, Olympics (≈480) and everything else
# fall back to the default ρ — they're either too sparse per-competition to
# fit reliably, or too heterogeneous to share a single ρ usefully.
DEFAULT_BUCKETS: dict[str, list[int]] = {
    "wc": [1],
    "continental": [4, 6, 7, 9, 22],
}


def _bivariate_poisson_matrix(
    lambda_home: float,
    lambda_away: float,
    rho: float,
    max_goals: int = _MAX_GOALS,
) -> np.ndarray:
    """Compute bivariate Poisson scoreline matrix with correlation ρ.

    Uses the diagonal-inflation method: the independent Poisson matrix is
    adjusted by shifting probability mass to/from the diagonal (draws).
    """
    # Independent Poisson
    home_probs = poisson.pmf(np.arange(max_goals + 1), lambda_home)
    away_probs = poisson.pmf(np.arange(max_goals + 1), lambda_away)
    mat = np.outer(home_probs, away_probs)

    # Apply correlation: inflate diagonal by ρ factor
    if rho != 0:
        for i in range(max_goals + 1):
            mat[i, i] *= 1 + rho

        # Renormalize
        mat = mat / mat.sum()

    return mat


def outcome_probs_bivariate(lambda_home: float, lambda_away: float, rho: float) -> dict[str, float]:
    """Derive outcome probabilities from bivariate Poisson."""
    mat = _bivariate_poisson_matrix(lambda_home, lambda_away, rho)
    p_home = float(np.tril(mat, -1).sum())
    p_draw = float(np.trace(mat))
    p_away = float(np.triu(mat, 1).sum())
    total = p_home + p_draw + p_away
    return {"home_win": p_home / total, "draw": p_draw / total, "away_win": p_away / total}


def fit_rho(
    model: TrainedModel,
    split: SplitData,
    calibration_frac: float = 0.15,
) -> float:
    """Fit the bivariate Poisson correlation parameter ρ on a calibration set.

    Uses the last calibration_frac of the training data as calibration set.
    Minimizes the Brier score of draw predictions.
    """
    n = len(split.X_train)
    cal_start = int(n * (1 - calibration_frac))
    X_cal = split.X_train.iloc[cal_start:]
    y_home_cal = split.y_home_train.iloc[cal_start:].values
    y_away_cal = split.y_away_train.iloc[cal_start:].values

    lh, la = predict_lambdas(model, X_cal, split)

    # Actual draw indicator
    is_draw = (y_home_cal == y_away_cal).astype(float)

    def loss(rho: float) -> float:
        brier_sum = 0.0
        for i in range(len(X_cal)):
            probs = outcome_probs_bivariate(lh[i], la[i], rho)
            brier_sum += (probs["draw"] - is_draw[i]) ** 2
        return brier_sum / len(X_cal)

    result = minimize_scalar(loss, bounds=(-0.5, 0.5), method="bounded")
    rho = float(result.x)
    logger.info("Fitted ρ = %.4f (Brier loss = %.4f)", rho, result.fun)
    return rho


def save_calibration(
    model: TrainedModel,
    rho: float,
    artefacts_dir: Path = ARTEFACTS_DIR,
) -> None:
    """Save calibrated model artefacts (scalar ρ — legacy save path)."""
    artefacts_dir.mkdir(parents=True, exist_ok=True)

    if model.model_home is not None:
        joblib.dump(model.model_home, artefacts_dir / "model_home_calibrated.pkl")
    if model.model_away is not None:
        joblib.dump(model.model_away, artefacts_dir / "model_away_calibrated.pkl")
    if model.scaler is not None:
        joblib.dump(model.scaler, artefacts_dir / "scaler.pkl")

    rho_path = artefacts_dir / "rho.json"
    rho_path.write_text(json.dumps({"rho": rho}, indent=2), encoding="utf-8")
    logger.info("Saved calibration artefacts to %s (ρ=%.4f)", artefacts_dir, rho)


# ----------------------------------------------------------------------
# Competition-conditional ρ
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RhoConfig:
    """Resolves the bivariate-Poisson correlation ρ for a given fixture.

    Backward-compatible with the legacy scalar ``{"rho": ...}`` shape; in that
    case ``by_bucket`` and ``league_to_bucket`` are empty and every lookup
    returns ``default``.
    """

    default: float
    by_bucket: dict[str, float] = field(default_factory=dict)
    league_to_bucket: dict[int, str] = field(default_factory=dict)

    def lookup(self, league_id: int | None) -> float:
        """Return ρ for ``league_id``, falling back to ``default`` if unmapped."""
        if league_id is None:
            return self.default
        bucket = self.league_to_bucket.get(int(league_id))
        if bucket is None:
            return self.default
        return self.by_bucket.get(bucket, self.default)

    def to_payload(self) -> dict:
        """Serialise to the on-disk JSON shape."""
        bucket_league_ids: dict[str, list[int]] = {}
        for lid, bucket in self.league_to_bucket.items():
            bucket_league_ids.setdefault(bucket, []).append(int(lid))
        for bucket in bucket_league_ids:
            bucket_league_ids[bucket] = sorted(bucket_league_ids[bucket])
        return {
            "rho_default": self.default,
            "rho_by_bucket": dict(self.by_bucket),
            "bucket_league_ids": bucket_league_ids,
        }


def load_rho_config(payload: dict) -> RhoConfig:
    """Parse a rho.json payload supporting both legacy and per-competition schemas.

    Legacy:        ``{"rho": -0.106}``
    Per-bucket:    ``{"rho_default": ..., "rho_by_bucket": {...},
                      "bucket_league_ids": {...}}``
    """
    if "rho_default" in payload:
        default = float(payload["rho_default"])
        by_bucket = {str(k): float(v) for k, v in payload.get("rho_by_bucket", {}).items()}
        league_to_bucket: dict[int, str] = {}
        for bucket, lids in payload.get("bucket_league_ids", {}).items():
            for lid in lids:
                league_to_bucket[int(lid)] = str(bucket)
        return RhoConfig(default=default, by_bucket=by_bucket, league_to_bucket=league_to_bucket)
    if "rho" not in payload:
        raise ValueError("rho.json must contain either 'rho' (legacy) or 'rho_default'")
    return RhoConfig(default=float(payload["rho"]))


def _fit_rho_brier(
    lambdas_home: np.ndarray,
    lambdas_away: np.ndarray,
    is_draw: np.ndarray,
) -> tuple[float, float]:
    """Minimise Brier-on-draws over ρ ∈ [-0.5, 0.5]. Returns (ρ, loss)."""

    def loss(rho: float) -> float:
        preds = np.array(
            [
                outcome_probs_bivariate(h, a, rho)["draw"]
                for h, a in zip(lambdas_home, lambdas_away, strict=True)
            ]
        )
        return float(np.mean((preds - is_draw) ** 2))

    result = minimize_scalar(loss, bounds=(-0.5, 0.5), method="bounded")
    return float(result.x), float(result.fun)


def fit_rho_per_bucket(
    lambdas_home: np.ndarray,
    lambdas_away: np.ndarray,
    is_draw: np.ndarray,
    league_ids: np.ndarray,
    buckets: dict[str, list[int]] | None = None,
    min_samples: int = 30,
) -> RhoConfig:
    """Fit ρ per bucket plus a cross-bucket default ρ used as the fallback.

    Buckets with fewer than ``min_samples`` matching rows inherit the default
    ρ (logged as a warning) — the alternative is a noisy in-bucket fit that
    overfits a handful of draws.
    """
    if buckets is None:
        buckets = DEFAULT_BUCKETS
    league_to_bucket = {int(lid): b for b, lids in buckets.items() for lid in lids}

    default_rho, default_loss = _fit_rho_brier(lambdas_home, lambdas_away, is_draw)
    logger.info("Default ρ = %+.4f (Brier=%.4f, N=%d)", default_rho, default_loss, len(is_draw))

    by_bucket: dict[str, float] = {}
    for bucket, lids in buckets.items():
        mask = np.isin(league_ids, lids)
        n = int(mask.sum())
        if n < min_samples:
            logger.warning(
                "Bucket %r has only %d rows (< %d) — falling back to default ρ",
                bucket,
                n,
                min_samples,
            )
            by_bucket[bucket] = default_rho
            continue
        rho_b, loss_b = _fit_rho_brier(lambdas_home[mask], lambdas_away[mask], is_draw[mask])
        logger.info("Bucket %r ρ = %+.4f (Brier=%.4f, N=%d)", bucket, rho_b, loss_b, n)
        by_bucket[bucket] = rho_b

    return RhoConfig(default=default_rho, by_bucket=by_bucket, league_to_bucket=league_to_bucket)


def save_rho_config(rho_config: RhoConfig, path: Path) -> None:
    """Write rho.json in the per-competition schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rho_config.to_payload(), indent=2), encoding="utf-8")
    logger.info("Saved RhoConfig → %s: %s", path, rho_config.to_payload())
