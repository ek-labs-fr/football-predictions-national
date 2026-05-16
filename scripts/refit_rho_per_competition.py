"""Refit ρ per competition bucket against the candidate-expanded model.

Loads the candidate-expanded LightGBM Poisson pair from
``artefacts/candidate_expanded/``, reproduces the expanded-training corpus
(date < 2024-06-14 minus the six post-WC-2022 holdout tournaments), then
fits one ρ per league-id bucket plus a cross-bucket default.

The fit uses Brier-on-draws — the same loss the production calibration
uses — but restricted to in-bucket rows so each bucket's ρ reflects that
competition type's actual draw structure (WC ~22%, continental majors
~26-28% based on the 2026-05-11 WC-only diagnostic and the
``retrain_expanded_comparison.txt`` evidence).

Output: rewrites ``artefacts/candidate_expanded/rho.json`` in the new
per-competition schema. Production ``artefacts/rho.json`` is NOT touched.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.models.calibrate import (
    DEFAULT_BUCKETS,
    fit_rho_per_bucket,
    save_rho_config,
)
from src.models.train import get_feature_columns

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


_TRAINING_TABLE = Path("data/processed/training_table.csv")
_CANDIDATE_DIR = Path("artefacts/candidate_expanded")
_MODEL_HOME = _CANDIDATE_DIR / "model_final_home.pkl"
_MODEL_AWAY = _CANDIDATE_DIR / "model_final_away.pkl"
_RHO_OUT = _CANDIDATE_DIR / "rho.json"
_REPORT = Path("outputs/rho_per_competition_fit.txt")

# Mirror retrain_expanded.py's cutoff + holdout exclusions so the calibration
# set is exactly the rows the candidate model was trained on.
_EXPAND_CUTOFF = pd.Timestamp("2024-06-14", tz="UTC")
_HOLDOUTS: list[tuple[str, int, int]] = [
    ("WC 2022", 1, 2022),
    ("Euro 2024", 4, 2024),
    ("Copa America 2024", 9, 2024),
    ("Olympics Men 2024", 480, 2024),
    ("Gold Cup 2025", 22, 2025),
    ("AFCON 2025", 6, 2025),
]


def _load_training_corpus() -> pd.DataFrame:
    df = pd.read_csv(_TRAINING_TABLE)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)

    mask = df["date"] < _EXPAND_CUTOFF
    for _, lid, season in _HOLDOUTS:
        mask &= ~((df["league_id"] == lid) & (df["season"] == season))
    return df[mask].copy()


def _predict_lambdas(
    model_home: object, model_away: object, x: pd.DataFrame
) -> tuple[np.ndarray, np.ndarray]:
    lh = np.clip(model_home.predict(x), 0.01, 10.0)
    la = np.clip(model_away.predict(x), 0.01, 10.0)
    return lh, la


def main() -> None:
    if not _MODEL_HOME.exists() or not _MODEL_AWAY.exists():
        raise SystemExit(
            f"candidate model not found at {_CANDIDATE_DIR}/ — "
            "run scripts/retrain_expanded.py first"
        )

    train_df = _load_training_corpus()
    feature_cols = get_feature_columns(train_df, mode="national")
    medians = train_df[feature_cols].median()
    x_train = train_df[feature_cols].fillna(medians)

    model_home = joblib.load(_MODEL_HOME)
    model_away = joblib.load(_MODEL_AWAY)
    lh, la = _predict_lambdas(model_home, model_away, x_train)
    is_draw = (train_df["home_goals"].values == train_df["away_goals"].values).astype(float)
    league_ids = train_df["league_id"].astype(int).values

    rho_config = fit_rho_per_bucket(
        lambdas_home=lh,
        lambdas_away=la,
        is_draw=is_draw,
        league_ids=league_ids,
        buckets=DEFAULT_BUCKETS,
    )

    # Per-bucket draw rates for the report — useful sanity check that the
    # fitted ρ moves draw mass in the same direction as the observed gap.
    bucket_stats: list[tuple[str, int, float]] = []
    for bucket, lids in DEFAULT_BUCKETS.items():
        m = np.isin(league_ids, lids)
        n = int(m.sum())
        rate = float(is_draw[m].mean()) if n else float("nan")
        bucket_stats.append((bucket, n, rate))
    other_mask = ~np.isin(
        league_ids,
        sum(DEFAULT_BUCKETS.values(), start=[]),
    )
    bucket_stats.append(
        (
            "other (→ default)",
            int(other_mask.sum()),
            float(is_draw[other_mask].mean()),
        )
    )

    lines: list[str] = [
        "=== Per-competition ρ refit (candidate_expanded) ===",
        f"Training corpus: N={len(train_df)} "
        f"({train_df['date'].min().date()} .. {train_df['date'].max().date()})",
        "",
        "Observed draw rate by bucket (training rows only):",
    ]
    for bucket, n, rate in bucket_stats:
        lines.append(f"  {bucket:<22s} N={n:>4d}  draw_rate={rate:.4f}")

    lines += [
        "",
        f"Fitted ρ_default = {rho_config.default:+.4f}",
        "Fitted ρ by bucket:",
    ]
    for bucket, rho in sorted(rho_config.by_bucket.items()):
        lines.append(f"  {bucket:<22s} ρ = {rho:+.4f}")

    lines += [
        "",
        f"League-id → bucket mapping ({len(rho_config.league_to_bucket)} entries):",
    ]
    for lid in sorted(rho_config.league_to_bucket):
        lines.append(f"  league_id={lid:<4d} → {rho_config.league_to_bucket[lid]}")

    save_rho_config(rho_config, _RHO_OUT)
    lines += ["", f"Wrote {_RHO_OUT}"]

    report = "\n".join(lines)
    _REPORT.parent.mkdir(parents=True, exist_ok=True)
    _REPORT.write_text(report, encoding="utf-8")
    sys.stdout.buffer.write((report + "\n").encode("utf-8"))


if __name__ == "__main__":
    main()
