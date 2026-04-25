"""Full training pipeline: rebuild features, train models, evaluate, calibrate, explain.

Usage:
    uv run python scripts/train_pipeline.py [--mode national|club] [--skip-features] [--skip-shap]

Steps:
  1. Rebuild training table from all feature sources
  2. Create time-based train/test split
     - national: WC 2022 holdout
     - club: most recent completed domestic season holdout
  3. Train baseline models (mean goals, rank-only Poisson, majority class)
  4. Train candidate models (Poisson linear, XGBoost Poisson, LightGBM Poisson,
     XGBoost classifier, logistic regression)
  5. Evaluate all models on holdout test set
  6. Calibrate best Poisson model (fit bivariate ρ)
  7. Save final model artefacts
  8. Compute SHAP explanations for best tree model

Artefacts land in `artefacts/` for national mode and `artefacts/club/` for club mode.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.features.build import build_club_training_table, build_training_table
from src.models.calibrate import fit_rho, save_calibration
from src.models.evaluate import evaluate_all, get_classification_report
from src.models.train import (
    TrainedModel,
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


_TRAINING_TABLE_BY_MODE = {
    "national": Path("data/processed/training_table.csv"),
    "club": Path("data/processed/training_table_club.csv"),
}

_ARTEFACTS_BY_MODE = {
    "national": Path("artefacts"),
    "club": Path("artefacts/club"),
}

_HISTORY_PATH = Path("outputs/training_history.csv")


def _append_history(
    comparison: pd.DataFrame,
    mode: str,
    best_model_name: str | None,
    history_path: Path = _HISTORY_PATH,
) -> None:
    sha = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip() or "unknown"

    row = comparison.copy()
    row.insert(0, "timestamp", datetime.now(UTC).isoformat(timespec="seconds"))
    row.insert(1, "mode", mode)
    row.insert(2, "git_sha", sha)
    row["is_best"] = row["model"] == best_model_name

    history_path.parent.mkdir(parents=True, exist_ok=True)
    row.to_csv(
        history_path,
        mode="a",
        header=not history_path.exists(),
        index=False,
    )
    logger.info("Appended %d rows to %s", len(row), history_path)


def main(mode: str, skip_features: bool = False, skip_shap: bool = False) -> None:
    training_table_path = _TRAINING_TABLE_BY_MODE[mode]
    artefacts_dir = _ARTEFACTS_BY_MODE[mode]
    artefacts_dir.mkdir(parents=True, exist_ok=True)

    # Step 1 — Rebuild training table
    if skip_features:
        logger.info("=== Skipping feature rebuild (--skip-features) ===")
    else:
        logger.info("=== Step 1: Rebuilding %s training table ===", mode)
        if mode == "national":
            build_training_table()
        else:
            build_club_training_table()

    # Step 2 — Train/test split
    logger.info("=== Step 2: Creating train/test split (mode=%s) ===", mode)
    split = create_split(training_table_path=training_table_path, mode=mode)

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
    comparison_path = artefacts_dir / "comparison.csv"
    comparison.to_csv(comparison_path, index=False)
    logger.info("Saved model comparison to %s", comparison_path)

    # Pick best Poisson model
    poisson_models = [m for m in all_models if m.is_poisson and m.model_home is not None]
    best_model: TrainedModel | None = None
    if poisson_models:
        best_name = comparison.loc[
            comparison["model"].isin([m.name for m in poisson_models]),
            ["model", "mae_avg"],
        ].dropna().sort_values("mae_avg").iloc[0]["model"]
        best_model = next(m for m in poisson_models if m.name == best_name)

    _append_history(comparison, mode, best_model.name if best_model else None)

    if best_model is not None:
        logger.info("Best Poisson model: %s", best_model.name)

        report = get_classification_report(best_model, split)
        logger.info("Classification report:\n%s", report)

        # Step 6 — Calibrate
        logger.info("=== Step 6: Calibrating %s ===", best_model.name)
        rho = fit_rho(best_model, split)
        save_calibration(best_model, rho, artefacts_dir=artefacts_dir)

        # Step 7 — Save final model
        logger.info("=== Step 7: Saving final model artefacts ===")
        save_model(best_model, artefacts_dir=artefacts_dir)
        best_model_copy = best_model
        best_model_copy.name = "model_final"
        save_model(best_model_copy, artefacts_dir=artefacts_dir)

        # Step 8 — SHAP
        if skip_shap:
            logger.info("=== Step 8: Skipped (--skip-shap) ===")
        else:
            logger.info("=== Step 8: Computing SHAP explanations ===")
            try:
                import shap

                from src.models.explain import (
                    compute_shap_values,
                    generate_shap_plots,
                    save_shap_artefacts,
                )

                explainer = shap.TreeExplainer(best_model.model_home)
                shap_vals = compute_shap_values(best_model.model_home, split.X_test, "home")
                save_shap_artefacts(
                    explainer,
                    shap_vals,
                    split.feature_cols,
                    artefacts_dir=artefacts_dir,
                )
                generate_shap_plots(shap_vals, split.X_test)
            except Exception as e:  # noqa: BLE001
                logger.warning("SHAP computation failed: %s", e)
    else:
        logger.warning("No Poisson models with fitted home model — skipping calibration/SHAP")

    # Save all models
    for m in all_models:
        if m.name != "model_final":
            save_model(m, artefacts_dir=artefacts_dir)

    logger.info("=== Training pipeline complete (mode=%s) ===", mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full model training pipeline")
    parser.add_argument(
        "--mode",
        choices=list(_TRAINING_TABLE_BY_MODE),
        default="national",
        help="Training mode: 'national' (WC 2022 holdout) or 'club' (latest completed season)",
    )
    parser.add_argument(
        "--skip-features", action="store_true", help="Skip feature rebuild (use existing table)"
    )
    parser.add_argument("--skip-shap", action="store_true", help="Skip SHAP computation")
    args = parser.parse_args()
    try:
        main(mode=args.mode, skip_features=args.skip_features, skip_shap=args.skip_shap)
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(1)
