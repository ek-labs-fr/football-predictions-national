"""Batch-predict scorelines and outcome probabilities for upcoming fixtures.

Reads `data/processed/inference_table{,_club}.parquet`, loads the latest
`model_final_*.pkl` + `rho.json` from `artefacts/{,club}`, and writes
predictions to `outputs/predictions_{national_wc2026,club}.csv`.

Usage:
    uv run python scripts/predict_inference.py [--mode national|club|all]
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Callable

import joblib
import numpy as np
import pandas as pd

from src.models.calibrate import _bivariate_poisson_matrix
from src.models.train import get_feature_columns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROCESSED = Path("data/processed")


def _wc_group_only(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["league_id"] == 1].copy()


CONFIGS: dict[str, dict] = {
    "national": {
        "training_table": PROCESSED / "training_table.csv",
        "inference_table": PROCESSED / "inference_table.parquet",
        "artefacts_dir": Path("artefacts"),
        "output": Path("outputs/predictions_national_wc2026.csv"),
        "row_filter": _wc_group_only,
    },
    "club": {
        "training_table": PROCESSED / "training_table_club.csv",
        "inference_table": PROCESSED / "inference_table_club.parquet",
        "artefacts_dir": Path("artefacts/club"),
        "output": Path("outputs/predictions_club.csv"),
        "row_filter": lambda df: df.copy(),
    },
}

OUTPUT_BASE_COLS = [
    "fixture_id",
    "date",
    "league_id",
    "round",
    "home_team_name",
    "away_team_name",
]


def _load_artefacts(artefacts_dir: Path) -> tuple:
    model_home = joblib.load(artefacts_dir / "model_final_home.pkl")
    model_away = joblib.load(artefacts_dir / "model_final_away.pkl")
    scaler_path = artefacts_dir / "model_final_scaler.pkl"
    scaler = joblib.load(scaler_path) if scaler_path.exists() else None
    rho = json.loads((artefacts_dir / "rho.json").read_text(encoding="utf-8"))["rho"]
    return model_home, model_away, scaler, rho


def predict_mode(mode: str) -> pd.DataFrame:
    cfg = CONFIGS[mode]

    train_df = pd.read_csv(cfg["training_table"])
    feature_cols = get_feature_columns(train_df, mode=mode)
    medians = train_df[feature_cols].median()

    model_home, model_away, scaler, rho = _load_artefacts(cfg["artefacts_dir"])

    inf = cfg["row_filter"](pd.read_parquet(cfg["inference_table"]))

    missing = [c for c in feature_cols if c not in inf.columns]
    if missing:
        raise KeyError(f"[{mode}] inference table missing feature columns: {missing[:5]}...")

    X = inf[feature_cols].fillna(medians)
    X_input = scaler.transform(X) if scaler is not None else X.values

    lh = np.clip(model_home.predict(X_input), 0.01, 10.0)
    la = np.clip(model_away.predict(X_input), 0.01, 10.0)

    scores: list[str] = []
    p_h: list[float] = []
    p_d: list[float] = []
    p_a: list[float] = []
    for h, a in zip(lh, la, strict=True):
        mat = _bivariate_poisson_matrix(h, a, rho)
        idx = np.unravel_index(mat.argmax(), mat.shape)
        scores.append(f"{int(idx[0])}-{int(idx[1])}")
        p_h.append(float(np.tril(mat, -1).sum()))
        p_d.append(float(np.trace(mat)))
        p_a.append(float(np.triu(mat, 1).sum()))

    cols = [c for c in OUTPUT_BASE_COLS if c in inf.columns]
    out = inf[cols].copy()
    out["lambda_home"] = np.round(lh, 3)
    out["lambda_away"] = np.round(la, 3)
    out["most_likely_score"] = scores
    out["p_home_win"] = np.round(p_h, 4)
    out["p_draw"] = np.round(p_d, 4)
    out["p_away_win"] = np.round(p_a, 4)
    out = out.sort_values("date").reset_index(drop=True)

    cfg["output"].parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(cfg["output"], index=False)

    logger.info(
        "[%s] %d fixtures predicted (rho=%.4f) -> %s",
        mode, len(out), rho, cfg["output"],
    )
    return out


def main(modes: list[str]) -> None:
    for m in modes:
        predict_mode(m)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["national", "club", "all"],
        default="all",
    )
    args = parser.parse_args()
    main(["national", "club"] if args.mode == "all" else [args.mode])
