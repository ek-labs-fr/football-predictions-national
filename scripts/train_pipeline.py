"""Full training pipeline: rebuild features, train models, evaluate, calibrate, explain.

Usage:
    uv run python scripts/train_pipeline.py [--skip-features] [--skip-shap]

Steps:
  1. Rebuild training table from all feature sources
  2. Create time-based train/test split (WC 2022 holdout)
  3. Train baseline models (mean goals, rank-only Poisson, majority class)
  4. Train candidate models (Poisson linear, XGBoost Poisson, LightGBM Poisson,
     XGBoost classifier, logistic regression)
  5. Evaluate all models on holdout test set
  6. Calibrate best Poisson model (fit bivariate ρ)
  7. Save final model artefacts
  8. Compute SHAP explanations for best tree model
"""

from __future__ import annotations

import argparse
import logging
import sys

from src.features.build import build_training_table
from src.models.calibrate import fit_rho, save_calibration
from src.models.evaluate import evaluate_all, get_classification_report
from src.models.explain import compute_shap_values, generate_shap_plots, save_shap_artefacts
from src.models.train import (
    create_split,
    save_model,
    train_baselines,
    train_candidates,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main(skip_features: bool = False, skip_shap: bool = False) -> None:
    # Step 1 — Rebuild training table
    if skip_features:
        logger.info("=== Skipping feature rebuild (--skip-features) ===")
    else:
        logger.info("=== Step 1: Rebuilding training table ===")
        build_training_table()

    # Step 2 — Train/test split
    logger.info("=== Step 2: Creating train/test split ===")
    split = create_split()

    # Step 3 — Baselines
    logger.info("=== Step 3: Training baseline models ===")
    baselines = train_baselines(split)

    # Step 4 — Candidates
    logger.info("=== Step 4: Training candidate models ===")
    candidates = train_candidates(split)

    all_models = baselines + candidates

    # Step 5 — Evaluate
    logger.info("=== Step 5: Evaluating all models ===")
    comparison = evaluate_all(all_models, split)

    # Print classification report for best model
    poisson_models = [m for m in all_models if m.is_poisson and m.model_home is not None]
    if poisson_models:
        # Pick best by MAE
        best_name = comparison.loc[
            comparison["model"].isin([m.name for m in poisson_models]),
            ["model", "mae_avg"],
        ].dropna().sort_values("mae_avg").iloc[0]["model"]
        best_model = next(m for m in poisson_models if m.name == best_name)
        logger.info("Best Poisson model: %s", best_model.name)

        report = get_classification_report(best_model, split)
        logger.info("Classification report:\n%s", report)

        # Step 6 — Calibrate
        logger.info("=== Step 6: Calibrating %s ===", best_model.name)
        rho = fit_rho(best_model, split)
        save_calibration(best_model, rho)

        # Step 7 — Save final model
        logger.info("=== Step 7: Saving final model artefacts ===")
        save_model(best_model)
        # Also save as the canonical "final" model
        best_model_copy = best_model
        best_model_copy.name = "model_final"
        save_model(best_model_copy)

        # Step 8 — SHAP
        if skip_shap:
            logger.info("=== Step 8: Skipped (--skip-shap) ===")
        else:
            logger.info("=== Step 8: Computing SHAP explanations ===")
            try:
                import shap

                explainer = shap.TreeExplainer(best_model.model_home)
                shap_vals = compute_shap_values(best_model.model_home, split.X_test, "home")
                save_shap_artefacts(
                    explainer, shap_vals, split.feature_cols
                )
                generate_shap_plots(shap_vals, split.X_test)
            except Exception as e:
                logger.warning("SHAP computation failed: %s", e)
    else:
        logger.warning("No Poisson models with fitted home model — skipping calibration/SHAP")

    # Save all models
    for m in all_models:
        if m.name != "model_final":
            save_model(m)

    logger.info("=== Training pipeline complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full model training pipeline")
    parser.add_argument(
        "--skip-features", action="store_true", help="Skip feature rebuild (use existing table)"
    )
    parser.add_argument("--skip-shap", action="store_true", help="Skip SHAP computation")
    args = parser.parse_args()
    try:
        main(skip_features=args.skip_features, skip_shap=args.skip_shap)
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(1)
