"""Optuna tuning + SHAP explainability for the club XGBoost Poisson model.

Usage:
    ./.venv/Scripts/python.exe -m scripts.tune_and_explain_club \
        [--n-trials 50] [--timeout 1800]

Runs Optuna hyperparameter search over XGBoost Poisson (home and away share
the same param set, as in tune.py), retrains XGBoost with the best params,
evaluates on the club holdout, then computes SHAP for the retrained home
model and saves plots.

Outputs:
    artefacts/club/best_params.json
    artefacts/club/xgboost_poisson_tuned_{home,away}.pkl
    artefacts/club/shap_explainer.pkl
    artefacts/club/shap_feature_importance.csv
    outputs/club/shap_*.png
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import joblib
import pandas as pd
from xgboost import XGBRegressor

from src.models.evaluate import evaluate_all
from src.models.train import (
    PROCESSED_DIR,
    SplitData,
    TrainedModel,
    create_split,
    save_model,
)
from src.models.tune import tune_xgboost

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

CLUB_TRAINING_TABLE = PROCESSED_DIR / "training_table_club.csv"
CLUB_ARTEFACTS = Path("artefacts/club")
CLUB_OUTPUTS = Path("outputs/club")


def _retrain_xgboost(split: SplitData, best_params: dict) -> TrainedModel:
    """Retrain XGBoost Poisson home/away on full training data with tuned params."""
    params = {
        "objective": "count:poisson",
        "verbosity": 0,
        **best_params,
    }
    t0 = time.time()
    model_h = XGBRegressor(**params)
    model_a = XGBRegressor(**params)
    model_h.fit(split.X_train, split.y_home_train, sample_weight=split.w_train)
    model_a.fit(split.X_train, split.y_away_train, sample_weight=split.w_train)

    return TrainedModel(
        name="xgboost_poisson_tuned",
        model_home=model_h,
        model_away=model_a,
        is_poisson=True,
        train_time=time.time() - t0,
        metadata={"params": best_params},
    )


def _run_shap(model: TrainedModel, split: SplitData) -> None:
    """Compute SHAP on the tuned XGBoost home model; save artefacts + plots."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import shap

    CLUB_ARTEFACTS.mkdir(parents=True, exist_ok=True)
    CLUB_OUTPUTS.mkdir(parents=True, exist_ok=True)

    explainer = shap.TreeExplainer(model.model_home)
    shap_values = explainer(split.X_test)
    logger.info("Computed SHAP for home model (%d samples)", len(split.X_test))

    joblib.dump(explainer, CLUB_ARTEFACTS / "shap_explainer.pkl")

    mean_abs = np.abs(shap_values.values).mean(axis=0)
    importance = (
        pd.DataFrame({"feature": split.feature_cols, "mean_abs_shap": mean_abs})
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )
    importance.to_csv(CLUB_ARTEFACTS / "shap_feature_importance.csv", index=False)
    logger.info("Top 10 features by SHAP importance:\n%s", importance.head(10).to_string())

    plt.figure(figsize=(10, 8))
    shap.summary_plot(shap_values, split.X_test, show=False)
    plt.tight_layout()
    plt.savefig(CLUB_OUTPUTS / "shap_summary_home.png", dpi=150)
    plt.close()

    plt.figure(figsize=(10, 8))
    shap.plots.bar(shap_values, show=False)
    plt.tight_layout()
    plt.savefig(CLUB_OUTPUTS / "shap_bar_importance.png", dpi=150)
    plt.close()

    top5_idx = np.argsort(mean_abs)[-5:][::-1]
    for idx in top5_idx:
        feat = split.X_test.columns[idx]
        plt.figure(figsize=(8, 6))
        shap.dependence_plot(int(idx), shap_values.values, split.X_test, show=False)
        plt.tight_layout()
        plt.savefig(CLUB_OUTPUTS / f"shap_dependence_{feat}.png", dpi=150)
        plt.close()

    logger.info("Saved SHAP plots to %s", CLUB_OUTPUTS)


def main(n_trials: int, timeout: int) -> None:
    logger.info("=== Loading club split ===")
    split = create_split(training_table_path=CLUB_TRAINING_TABLE, mode="club")

    logger.info("=== Running Optuna (n_trials=%d, timeout=%ds) ===", n_trials, timeout)
    best = tune_xgboost(
        X=split.X_train.values,
        y_home=split.y_home_train.values,
        y_away=split.y_away_train.values,
        sample_weight=split.w_train.values,
        n_trials=n_trials,
        timeout=timeout,
        artefacts_dir=CLUB_ARTEFACTS,
    )
    logger.info("Best params: %s", json.dumps(best, indent=2))

    logger.info("=== Retraining XGBoost with tuned params ===")
    tuned = _retrain_xgboost(split, best)
    save_model(tuned, artefacts_dir=CLUB_ARTEFACTS)

    logger.info("=== Evaluating tuned model vs other candidates ===")
    comparison = evaluate_all([tuned], split)
    logger.info("Tuned model:\n%s", comparison.to_string(index=False))
    comparison.to_csv(CLUB_ARTEFACTS / "comparison_tuned.csv", index=False)

    logger.info("=== Computing SHAP ===")
    _run_shap(tuned, split)

    logger.info("=== Done ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tune and explain club XGBoost Poisson")
    parser.add_argument("--n-trials", type=int, default=50, help="Optuna trial count")
    parser.add_argument("--timeout", type=int, default=1800, help="Optuna timeout (seconds)")
    args = parser.parse_args()
    try:
        main(n_trials=args.n_trials, timeout=args.timeout)
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(1)
