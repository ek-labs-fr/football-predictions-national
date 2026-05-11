"""Diagnostic: refit ρ on World Cup fixtures only and compare draw mass.

Loads the production national-mode artefacts and the training table,
filters to WC fixtures (league_id=1, excluding the WC 2022 holdout),
predicts λ_home/λ_away, and:

  1. Reports the observed WC draw rate (actual draws / total fixtures).
  2. Reports the model-implied draw rate under independence (ρ=0) and
     under production ρ (-0.106).
  3. Refits ρ restricted to WC fixtures, minimising Brier-on-draws —
     same loss the production calibration uses.

Output goes to ``outputs/rho_wc_only_diagnostic.txt``. Production
``artefacts/rho.json`` is NOT overwritten.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar

from src.models.calibrate import outcome_probs_bivariate
from src.models.train import _make_holdout_masks, get_feature_columns

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


_TRAINING_TABLE = Path("data/processed/training_table.csv")
_MODEL_HOME = Path("artefacts/model_final_home.pkl")
_MODEL_AWAY = Path("artefacts/model_final_away.pkl")
_PROD_RHO = Path("artefacts/rho.json")
_OUTPUT = Path("outputs/rho_wc_only_diagnostic.txt")
_WC_LEAGUE_ID = 1


def _predicted_draw_rate(lh: np.ndarray, la: np.ndarray, rho: float) -> float:
    return float(
        np.mean([outcome_probs_bivariate(h, a, rho)["draw"] for h, a in zip(lh, la, strict=True)])
    )


def _brier_draw_loss(rho: float, lh: np.ndarray, la: np.ndarray, is_draw: np.ndarray) -> float:
    preds = np.array(
        [outcome_probs_bivariate(h, a, rho)["draw"] for h, a in zip(lh, la, strict=True)]
    )
    return float(np.mean((preds - is_draw) ** 2))


def main() -> None:
    df = pd.read_csv(_TRAINING_TABLE)
    df["date"] = pd.to_datetime(df["date"], utc=True)

    train_mask, _test_mask = _make_holdout_masks(df, mode="national")
    train_df = df[train_mask].copy()
    feature_cols = get_feature_columns(train_df, mode="national")
    medians = train_df[feature_cols].median()

    wc = train_df[train_df["league_id"] == _WC_LEAGUE_ID].copy()
    logger.info("WC fixtures in train (pre WC 2022 holdout): %d", len(wc))
    if wc.empty:
        raise SystemExit("no WC fixtures in train_df — abort")

    model_home = joblib.load(_MODEL_HOME)
    model_away = joblib.load(_MODEL_AWAY)
    prod_rho = float(json.loads(_PROD_RHO.read_text())["rho"])

    x_in = wc[feature_cols].fillna(medians)
    lh = np.clip(model_home.predict(x_in), 0.01, 10.0)
    la = np.clip(model_away.predict(x_in), 0.01, 10.0)
    is_draw = (wc["home_goals"].values == wc["away_goals"].values).astype(float)

    observed = float(is_draw.mean())
    pred_independence = _predicted_draw_rate(lh, la, 0.0)
    pred_prod_rho = _predicted_draw_rate(lh, la, prod_rho)

    result = minimize_scalar(
        _brier_draw_loss,
        bounds=(-0.5, 0.5),
        method="bounded",
        args=(lh, la, is_draw),
    )
    wc_rho = float(result.x)
    pred_wc_rho = _predicted_draw_rate(lh, la, wc_rho)

    lines = [
        "=== ρ Diagnostic: World Cup-only refit (national mode) ===",
        f"WC fixtures used: {len(wc)} (league_id=1, excl. WC 2022 holdout)",
        f"Date range: {wc['date'].min().date()} .. {wc['date'].max().date()}",
        "",
        f"Observed WC draw rate:                  "
        f"  {observed:.4f} ({int(is_draw.sum())}/{len(wc)})",
        f"Model-implied (independence, ρ=0):        {pred_independence:.4f}",
        f"Model-implied (production ρ={prod_rho:+.4f}):  {pred_prod_rho:.4f}",
        f"Model-implied (WC-only refit ρ={wc_rho:+.4f}): {pred_wc_rho:.4f}",
        "",
        f"WC-only refit ρ = {wc_rho:+.6f}  (Brier-on-draws = {float(result.fun):.6f})",
        f"Production ρ    = {prod_rho:+.6f}",
        "",
        "Interpretation:",
        f"  Δ observed − independence  = {observed - pred_independence:+.4f}",
        f"  Δ observed − production    = {observed - pred_prod_rho:+.4f}",
        f"  Δ observed − wc-refit      = {observed - pred_wc_rho:+.4f}",
        "",
        "Sign convention check: positive ρ inflates the diagonal (more draws).",
        "  If observed > independence, expect ρ > 0 (more draws than indep.)",
        "  If observed < independence, expect ρ < 0 (fewer draws than indep.)",
    ]
    report = "\n".join(lines)
    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT.write_text(report, encoding="utf-8")
    # Windows default console is cp1252 — write bytes via stdout.buffer so
    # the Greek ρ glyph in the report doesn't crash the script.
    sys.stdout.buffer.write((report + "\n").encode("utf-8"))


if __name__ == "__main__":
    main()
