"""Retrain on expanded corpus and compare against production training cutoff.

Production train mask uses `date < 2022-11-20`, so 1,229 post-WC-2022 fixtures
sit unused. This script retrains two LightGBM Poisson pairs with identical
hyperparameters but different cutoffs:

  Model A (production-equivalent):  train < 2022-11-20
  Model B (expanded):                train < 2024-06-14

and evaluates both on three holdouts: WC 2022 (production reference, unseen
by A only), Euro 2024 (unseen by both), Copa America 2024 (unseen by both).

Output: outputs/retrain_expanded_comparison.txt. Production
artefacts/model_final_* are NOT touched; candidates go to
artefacts/candidate_expanded/ for follow-up inspection.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor, early_stopping
from scipy.optimize import minimize_scalar
from sklearn.metrics import brier_score_loss, log_loss, mean_absolute_error

from src.features import io
from src.models.calibrate import _bivariate_poisson_matrix
from src.models.train import get_feature_columns

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

_OUTPUT = Path("outputs/retrain_expanded_comparison.txt")
_CANDIDATE_DIR = Path("artefacts/candidate_expanded")

_HYPERPARAMS = dict(
    objective="poisson",
    n_estimators=500,
    learning_rate=0.05,
    max_depth=5,
    subsample=0.8,
    colsample_bytree=0.8,
    verbosity=-1,
)


@dataclass
class TrainedPair:
    name: str
    model_home: LGBMRegressor
    model_away: LGBMRegressor
    rho: float
    feature_cols: list[str]
    medians: pd.Series
    train_n: int
    train_date_max: pd.Timestamp


def _train_pair(
    train_df: pd.DataFrame,
    feature_cols: list[str],
    medians: pd.Series,
    name: str,
) -> TrainedPair:
    x_all = train_df[feature_cols].fillna(medians)  # noqa: N806 — feature matrix
    y_h = train_df["home_goals"].astype(int)
    y_a = train_df["away_goals"].astype(int)
    w = train_df["match_weight"]

    split_idx = int(len(x_all) * 0.8)
    x_tr, x_ev = x_all.iloc[:split_idx], x_all.iloc[split_idx:]
    y_h_tr, y_h_ev = y_h.iloc[:split_idx], y_h.iloc[split_idx:]
    y_a_tr, y_a_ev = y_a.iloc[:split_idx], y_a.iloc[split_idx:]
    w_tr = w.iloc[:split_idx]

    model_h = LGBMRegressor(**_HYPERPARAMS)
    model_a = LGBMRegressor(**_HYPERPARAMS)
    model_h.fit(
        x_tr,
        y_h_tr,
        sample_weight=w_tr,
        eval_set=[(x_ev, y_h_ev)],
        callbacks=[early_stopping(50, verbose=False)],
    )
    model_a.fit(
        x_tr,
        y_a_tr,
        sample_weight=w_tr,
        eval_set=[(x_ev, y_a_ev)],
        callbacks=[early_stopping(50, verbose=False)],
    )

    # Calibrate ρ on last 15% of train
    cal_idx = int(len(x_all) * 0.85)
    x_cal = x_all.iloc[cal_idx:]
    is_draw_cal = (y_h.iloc[cal_idx:].values == y_a.iloc[cal_idx:].values).astype(float)
    lh_cal = np.clip(model_h.predict(x_cal), 0.01, 10.0)
    la_cal = np.clip(model_a.predict(x_cal), 0.01, 10.0)

    def _draw_brier(rho: float) -> float:
        preds = []
        for lh, la in zip(lh_cal, la_cal, strict=True):
            mat = _bivariate_poisson_matrix(lh, la, rho)
            preds.append(float(np.trace(mat)))
        preds_arr = np.asarray(preds)
        return float(np.mean((preds_arr - is_draw_cal) ** 2))

    result = minimize_scalar(_draw_brier, bounds=(-0.5, 0.5), method="bounded")
    rho = float(result.x)

    return TrainedPair(
        name=name,
        model_home=model_h,
        model_away=model_a,
        rho=rho,
        feature_cols=feature_cols,
        medians=medians,
        train_n=len(train_df),
        train_date_max=train_df["date"].max(),
    )


def _evaluate(pair: TrainedPair, holdout: pd.DataFrame) -> dict:
    x_in = holdout[pair.feature_cols].fillna(pair.medians)
    lh = np.clip(pair.model_home.predict(x_in), 0.01, 10.0)
    la = np.clip(pair.model_away.predict(x_in), 0.01, 10.0)
    y_h = holdout["home_goals"].astype(int).values
    y_a = holdout["away_goals"].astype(int).values

    pred_scores: list[str] = []
    p_h: list[float] = []
    p_d: list[float] = []
    p_a: list[float] = []
    for lh_i, la_i in zip(lh, la, strict=True):
        mat = _bivariate_poisson_matrix(lh_i, la_i, pair.rho)
        idx = np.unravel_index(mat.argmax(), mat.shape)
        pred_scores.append(f"{int(idx[0])}-{int(idx[1])}")
        p_h.append(float(np.tril(mat, -1).sum()))
        p_d.append(float(np.trace(mat)))
        p_a.append(float(np.triu(mat, 1).sum()))

    actual_scores = [f"{int(h)}-{int(a)}" for h, a in zip(y_h, y_a, strict=True)]
    exact_acc = float(np.mean(np.array(pred_scores) == np.array(actual_scores)))

    # 0=away_win, 1=draw, 2=home_win
    y_outcome_idx = np.where(y_h > y_a, 2, np.where(y_h < y_a, 0, 1))
    probs = np.column_stack([p_a, p_d, p_h])
    pred_outcome_idx = probs.argmax(axis=1)
    outcome_acc = float(np.mean(pred_outcome_idx == y_outcome_idx))

    probs_clipped = np.clip(probs, 1e-7, 1 - 1e-7)
    probs_normed = probs_clipped / probs_clipped.sum(axis=1, keepdims=True)
    ll = float(log_loss(y_outcome_idx, probs_normed, labels=[0, 1, 2]))

    brier = 0.0
    for cls in range(3):
        y_bin = (y_outcome_idx == cls).astype(float)
        brier += brier_score_loss(y_bin, probs_normed[:, cls])
    brier /= 3

    # RPS (ordered: away, draw, home)
    rps_sum = 0.0
    for i in range(len(holdout)):
        true_one_hot = np.zeros(3)
        true_one_hot[int(y_outcome_idx[i])] = 1.0
        cum_pred = np.cumsum(probs[i])
        cum_true = np.cumsum(true_one_hot)
        rps_sum += float(np.sum((cum_pred - cum_true) ** 2)) / 2.0
    rps = rps_sum / len(holdout)

    mae_h = mean_absolute_error(y_h, np.round(lh))
    mae_a = mean_absolute_error(y_a, np.round(la))

    from collections import Counter

    top_scores = Counter(pred_scores).most_common(5)

    return dict(
        n=len(holdout),
        mae_avg=float((mae_h + mae_a) / 2),
        exact_scoreline_acc=exact_acc,
        wdl_acc=outcome_acc,
        rps=rps,
        log_loss=ll,
        brier=brier,
        top_scores=top_scores,
        rho=pair.rho,
    )


def main() -> None:
    out: list[str] = []

    def emit(s: str) -> None:
        out.append(s)
        sys.stdout.buffer.write((s + "\n").encode("utf-8"))

    df = io.read_csv("data/processed/training_table.csv")
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)

    def _slice(league_id: int, season: int) -> pd.DataFrame:
        return df[(df["league_id"] == league_id) & (df["season"] == season)].copy()

    def _describe(label: str, sub: pd.DataFrame) -> str:
        if sub.empty:
            return f"{label:<22s} N=0 (empty)"
        return (
            f"{label:<22s} N={len(sub):>3d} "
            f"({sub['date'].min().date()} .. {sub['date'].max().date()})"
        )

    holdout_specs: list[tuple[str, int, int]] = [
        ("WC 2022", 1, 2022),
        ("Euro 2024", 4, 2024),
        ("Copa America 2024", 9, 2024),
        ("Olympics Men 2024", 480, 2024),
        ("Gold Cup 2025", 22, 2025),
        ("AFCON 2025", 6, 2025),
    ]
    holdouts: list[tuple[str, pd.DataFrame]] = []
    for label, lid, season in holdout_specs:
        sub = _slice(lid, season)
        emit(_describe(label, sub))
        if not sub.empty:
            holdouts.append((label, sub))
    emit("")

    # Model A: production-equivalent — train on date < 2022-11-20
    train_a = df[df["date"] < pd.Timestamp("2022-11-20", tz="UTC")].copy()
    feat_cols_a = get_feature_columns(train_a, mode="national")
    medians_a = train_a[feat_cols_a].median()
    pair_a = _train_pair(train_a, feat_cols_a, medians_a, "production_equivalent")

    # Model B: expanded — train on date < 2024-06-14, but always exclude
    # the holdout (league_id, season) combos even if a few of their fixtures
    # accidentally pre-date the cutoff.
    expand_cutoff = pd.Timestamp("2024-06-14", tz="UTC")
    train_b_mask = df["date"] < expand_cutoff
    for _label, lid, season in holdout_specs:
        train_b_mask &= ~((df["league_id"] == lid) & (df["season"] == season))
    train_b = df[train_b_mask].copy()
    feat_cols_b = get_feature_columns(train_b, mode="national")
    medians_b = train_b[feat_cols_b].median()
    pair_b = _train_pair(train_b, feat_cols_b, medians_b, "expanded")

    emit("=" * 90)
    emit("TRAINED PAIRS")
    emit("=" * 90)
    emit(
        f"  A (production-equivalent): N_train={pair_a.train_n}, "
        f"last_train_date={pair_a.train_date_max.date()}, ρ={pair_a.rho:+.4f}"
    )
    emit(
        f"  B (expanded):              N_train={pair_b.train_n}, "
        f"last_train_date={pair_b.train_date_max.date()}, ρ={pair_b.rho:+.4f}"
    )
    emit("")

    # Evaluate each pair on every holdout.
    # Note: model B has seen WC 2022 in train (cutoff is 2024-06-14).
    # So WC 2022 metrics for B reflect train performance, not generalization.
    results: dict[tuple[str, str], dict] = {}
    for pair in (pair_a, pair_b):
        for label, ho in holdouts:
            if ho.empty:
                continue
            results[(pair.name, label)] = _evaluate(pair, ho)

    emit("=" * 90)
    emit("SIDE-BY-SIDE METRICS")
    emit("=" * 90)
    emit("")
    cols = ["N", "MAE_avg", "Exact-acc", "W/D/L-acc", "RPS", "LogLoss", "Brier"]
    header = f"{'Model':<25s} {'Holdout':<22s} " + " ".join(f"{c:>9s}" for c in cols)
    emit(header)
    emit("-" * len(header))
    for pair_name in ("production_equivalent", "expanded"):
        for label, _ho in holdouts:
            if (pair_name, label) not in results:
                continue
            r = results[(pair_name, label)]
            note = " *seen-in-train" if pair_name == "expanded" and label == "WC 2022" else ""
            emit(
                f"{pair_name:<25s} {label + note:<22s} "
                f"{r['n']:>9d} "
                f"{r['mae_avg']:>9.4f} "
                f"{r['exact_scoreline_acc']:>9.4f} "
                f"{r['wdl_acc']:>9.4f} "
                f"{r['rps']:>9.4f} "
                f"{r['log_loss']:>9.4f} "
                f"{r['brier']:>9.4f}"
            )
    emit("")

    emit("=" * 90)
    emit("SCORELINE DISTRIBUTION ON UNSEEN HOLDOUTS")
    emit("=" * 90)
    for pair_name in ("production_equivalent", "expanded"):
        for label, _ho in holdouts:
            if (pair_name, label) not in results:
                continue
            if pair_name == "expanded" and label == "WC 2022":
                continue  # seen in train, skip
            r = results[(pair_name, label)]
            emit(f"\n  {pair_name} on {label} (ρ={r['rho']:+.4f}):")
            for s, c in r["top_scores"]:
                pct = 100.0 * c / r["n"]
                emit(f"    {s:>6s}  {c:>4d}  ({pct:5.1f}%)")
    emit("")

    # Save candidate B for later inspection (don't touch production)
    _CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pair_b.model_home, _CANDIDATE_DIR / "model_final_home.pkl")
    joblib.dump(pair_b.model_away, _CANDIDATE_DIR / "model_final_away.pkl")
    (_CANDIDATE_DIR / "rho.json").write_text(f'{{"rho": {pair_b.rho}}}', encoding="utf-8")
    emit(f"Candidate expanded model saved to {_CANDIDATE_DIR}/ (not loaded by production).")

    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT.write_text("\n".join(out), encoding="utf-8")


if __name__ == "__main__":
    main()
