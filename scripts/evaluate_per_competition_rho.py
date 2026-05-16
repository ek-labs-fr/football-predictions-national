"""Three-way comparison: production vs expanded(global ρ) vs expanded(per-bucket ρ).

Loads the production and candidate-expanded LightGBM Poisson pairs along
with their respective rho.json files (the candidate must have been refit
to the per-competition schema via scripts/refit_rho_per_competition.py
first) and evaluates each configuration on the six post-WC-2022 holdouts.

The pivotal threshold the user agreed to is: on AFCON 2025, the
expanded+per-competition-ρ predicted-1-0 share must drop below 40% (the
production-equivalent model predicts 1-0 on 61.5% of AFCON 2025 fixtures).

Output: outputs/expanded_per_competition_rho_comparison.txt. Production
artefacts are NOT touched.
"""

from __future__ import annotations

import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, mean_absolute_error

from src.models.calibrate import (
    RhoConfig,
    _bivariate_poisson_matrix,
    load_rho_config,
)
from src.models.train import get_feature_columns

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


_TRAINING_TABLE = Path("data/processed/training_table.csv")
_OUTPUT = Path("outputs/expanded_per_competition_rho_comparison.txt")
_AFCON_1_0_THRESHOLD = 0.40

_HOLDOUTS: list[tuple[str, int, int]] = [
    ("WC 2022", 1, 2022),
    ("Euro 2024", 4, 2024),
    ("Copa America 2024", 9, 2024),
    ("Olympics Men 2024", 480, 2024),
    ("Gold Cup 2025", 22, 2025),
    ("AFCON 2025", 6, 2025),
]

_PROD_CUTOFF = pd.Timestamp("2022-11-20", tz="UTC")
_EXPAND_CUTOFF = pd.Timestamp("2024-06-14", tz="UTC")


@dataclass(frozen=True)
class Configuration:
    name: str
    model_home_path: Path
    model_away_path: Path
    rho_config: RhoConfig
    medians: pd.Series
    feature_cols: list[str]
    cutoff: pd.Timestamp  # training cutoff — used to flag *seen-in-train holdouts


def _build_medians(cutoff: pd.Timestamp, df: pd.DataFrame) -> tuple[list[str], pd.Series]:
    """Reproduce the per-model feature columns + train medians at a given cutoff."""
    mask = df["date"] < cutoff
    for _, lid, season in _HOLDOUTS:
        mask &= ~((df["league_id"] == lid) & (df["season"] == season))
    train = df[mask]
    feature_cols = get_feature_columns(train, mode="national")
    medians = train[feature_cols].median()
    return feature_cols, medians


def _evaluate(
    cfg: Configuration,
    holdout: pd.DataFrame,
    label: str,
) -> dict:
    x_in = holdout[cfg.feature_cols].fillna(cfg.medians)
    model_home = joblib.load(cfg.model_home_path)
    model_away = joblib.load(cfg.model_away_path)
    lh = np.clip(model_home.predict(x_in), 0.01, 10.0)
    la = np.clip(model_away.predict(x_in), 0.01, 10.0)
    y_h = holdout["home_goals"].astype(int).values
    y_a = holdout["away_goals"].astype(int).values
    league_ids = holdout["league_id"].astype(int).values

    pred_scores: list[str] = []
    rhos_used: list[float] = []
    p_h: list[float] = []
    p_d: list[float] = []
    p_a: list[float] = []
    for lh_i, la_i, lid in zip(lh, la, league_ids, strict=True):
        rho = cfg.rho_config.lookup(int(lid))
        rhos_used.append(rho)
        mat = _bivariate_poisson_matrix(lh_i, la_i, rho)
        idx = np.unravel_index(mat.argmax(), mat.shape)
        pred_scores.append(f"{int(idx[0])}-{int(idx[1])}")
        p_h.append(float(np.tril(mat, -1).sum()))
        p_d.append(float(np.trace(mat)))
        p_a.append(float(np.triu(mat, 1).sum()))

    actual_scores = [f"{int(h)}-{int(a)}" for h, a in zip(y_h, y_a, strict=True)]
    exact_acc = float(np.mean(np.array(pred_scores) == np.array(actual_scores)))

    y_outcome_idx = np.where(y_h > y_a, 2, np.where(y_h < y_a, 0, 1))  # away, draw, home
    probs = np.column_stack([p_a, p_d, p_h])
    pred_outcome_idx = probs.argmax(axis=1)
    outcome_acc = float(np.mean(pred_outcome_idx == y_outcome_idx))

    probs_clip = np.clip(probs, 1e-7, 1 - 1e-7)
    probs_norm = probs_clip / probs_clip.sum(axis=1, keepdims=True)
    ll = float(log_loss(y_outcome_idx, probs_norm, labels=[0, 1, 2]))

    brier = 0.0
    for cls in range(3):
        y_bin = (y_outcome_idx == cls).astype(float)
        brier += brier_score_loss(y_bin, probs_norm[:, cls])
    brier /= 3

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

    return dict(
        label=label,
        n=len(holdout),
        mae_avg=float((mae_h + mae_a) / 2),
        exact_acc=exact_acc,
        wdl_acc=outcome_acc,
        rps=rps,
        log_loss=ll,
        brier=brier,
        top_scores=Counter(pred_scores).most_common(5),
        rho_used_unique=sorted({round(r, 4) for r in rhos_used}),
    )


def _load_configs() -> list[Configuration]:
    df = pd.read_csv(_TRAINING_TABLE)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)

    prod_feature_cols, prod_medians = _build_medians(_PROD_CUTOFF, df)
    exp_feature_cols, exp_medians = _build_medians(_EXPAND_CUTOFF, df)

    prod_rho = load_rho_config(json.loads(Path("artefacts/rho.json").read_text()))
    cand_rho = load_rho_config(
        json.loads(Path("artefacts/candidate_expanded/rho.json").read_text())
    )

    # Expanded-with-global-ρ — use the candidate model but a scalar RhoConfig
    # whose default matches the original (pre-refit) global value, so the
    # middle column matches retrain_expanded_comparison.txt's "expanded" row.
    expanded_global_rho = RhoConfig(default=cand_rho.default)

    return [
        Configuration(
            name="production_equivalent",
            model_home_path=Path("artefacts/model_final_home.pkl"),
            model_away_path=Path("artefacts/model_final_away.pkl"),
            rho_config=prod_rho,
            medians=prod_medians,
            feature_cols=prod_feature_cols,
            cutoff=_PROD_CUTOFF,
        ),
        Configuration(
            name="expanded_global_rho",
            model_home_path=Path("artefacts/candidate_expanded/model_final_home.pkl"),
            model_away_path=Path("artefacts/candidate_expanded/model_final_away.pkl"),
            rho_config=expanded_global_rho,
            medians=exp_medians,
            feature_cols=exp_feature_cols,
            cutoff=_EXPAND_CUTOFF,
        ),
        Configuration(
            name="expanded_per_competition_rho",
            model_home_path=Path("artefacts/candidate_expanded/model_final_home.pkl"),
            model_away_path=Path("artefacts/candidate_expanded/model_final_away.pkl"),
            rho_config=cand_rho,
            medians=exp_medians,
            feature_cols=exp_feature_cols,
            cutoff=_EXPAND_CUTOFF,
        ),
    ]


def main() -> None:
    out: list[str] = []

    def emit(line: str) -> None:
        out.append(line)
        sys.stdout.buffer.write((line + "\n").encode("utf-8"))

    df = pd.read_csv(_TRAINING_TABLE)
    df["date"] = pd.to_datetime(df["date"], utc=True)

    def _slice(league_id: int, season: int) -> pd.DataFrame:
        return df[(df["league_id"] == league_id) & (df["season"] == season)].copy()

    holdouts: list[tuple[str, pd.DataFrame]] = []
    for label, lid, season in _HOLDOUTS:
        sub = _slice(lid, season)
        if not sub.empty:
            holdouts.append((label, sub))
            emit(
                f"{label:<22s} N={len(sub):>3d} "
                f"({sub['date'].min().date()} .. {sub['date'].max().date()})"
            )
    emit("")

    configs = _load_configs()

    emit("=" * 100)
    emit("CONFIGURATIONS")
    emit("=" * 100)
    for cfg in configs:
        rc = cfg.rho_config
        if rc.by_bucket:
            bucket_str = ", ".join(f"{b}={v:+.4f}" for b, v in sorted(rc.by_bucket.items()))
            rho_desc = f"default={rc.default:+.4f}; {bucket_str}"
        else:
            rho_desc = f"scalar ρ={rc.default:+.4f}"
        emit(f"  {cfg.name:<30s} cutoff={cfg.cutoff.date()}  ρ: {rho_desc}")
    emit("")

    results: dict[tuple[str, str], dict] = {}
    for cfg in configs:
        for label, ho in holdouts:
            results[(cfg.name, label)] = _evaluate(cfg, ho, label)

    emit("=" * 100)
    emit("SIDE-BY-SIDE METRICS")
    emit("=" * 100)
    emit("")
    cols = ["N", "MAE_avg", "Exact-acc", "W/D/L-acc", "RPS", "LogLoss", "Brier"]
    header = f"{'Configuration':<32s} {'Holdout':<22s} " + " ".join(f"{c:>9s}" for c in cols)
    emit(header)
    emit("-" * len(header))
    for cfg in configs:
        for label, _ho in holdouts:
            r = results[(cfg.name, label)]
            note = " *seen-in-train" if label == "WC 2022" and cfg.cutoff == _EXPAND_CUTOFF else ""
            emit(
                f"{cfg.name:<32s} {label + note:<22s} "
                f"{r['n']:>9d} "
                f"{r['mae_avg']:>9.4f} "
                f"{r['exact_acc']:>9.4f} "
                f"{r['wdl_acc']:>9.4f} "
                f"{r['rps']:>9.4f} "
                f"{r['log_loss']:>9.4f} "
                f"{r['brier']:>9.4f}"
            )
    emit("")

    emit("=" * 100)
    emit("SCORELINE DISTRIBUTION ON UNSEEN HOLDOUTS")
    emit("=" * 100)
    for cfg in configs:
        for label, _ho in holdouts:
            if label == "WC 2022" and cfg.cutoff == _EXPAND_CUTOFF:
                continue
            r = results[(cfg.name, label)]
            rho_str = (
                ", ".join(f"{x:+.4f}" for x in r["rho_used_unique"])
                if r["rho_used_unique"]
                else "n/a"
            )
            emit(f"\n  {cfg.name} on {label} (ρ used: {rho_str}):")
            for s, c in r["top_scores"]:
                pct = 100.0 * c / r["n"]
                emit(f"    {s:>6s}  {c:>4d}  ({pct:5.1f}%)")
    emit("")

    # Promotion threshold: AFCON-2025 1-0 share must drop below 40% under the
    # expanded+per-competition ρ configuration (production is at 61.5%).
    afcon_top = results[("expanded_per_competition_rho", "AFCON 2025")]["top_scores"]
    afcon_n = results[("expanded_per_competition_rho", "AFCON 2025")]["n"]
    one_zero_share = next((c for s, c in afcon_top if s == "1-0"), 0) / afcon_n
    emit("=" * 100)
    emit("PROMOTION THRESHOLD CHECK")
    emit("=" * 100)
    emit(
        f"  AFCON 2025 '1-0' share under expanded+per-competition ρ: "
        f"{one_zero_share:.1%} (threshold: < {_AFCON_1_0_THRESHOLD:.0%})"
    )
    decision = "PASS — eligible for promotion" if one_zero_share < _AFCON_1_0_THRESHOLD else "FAIL"
    emit(f"  Decision: {decision}")

    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT.write_text("\n".join(out), encoding="utf-8")


if __name__ == "__main__":
    main()
