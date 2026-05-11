"""National-mode recent-window drift diagnostic.

Tests whether the production LightGBM Poisson model systematically
under-predicts goals on fixtures *after* the WC 2022 holdout cutoff —
the same drift pattern card §12.10 documented for club mode
(PL −15.7%, La Liga −8.4%, Ligue 1 −26.2%).

For every national fixture with status ∈ {FT, AET, PEN} and date
≥ WC 2022 start (excluding WC 2022 itself), predicts λ_h/λ_a using
the production model and compares to actual goals. Reports:

  - Overall drift % (predicted total vs actual total)
  - Per-competition drift (league_id × season buckets)
  - Per-time-window drift (yearly buckets)
  - Recent-window metrics (MAE, exact-scoreline acc, W/D/L acc)
    for comparison against the WC 2022 holdout numbers

Output: outputs/drift_national_recent.txt. No artefacts modified.
"""

from __future__ import annotations

import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.features import io
from src.models.calibrate import outcome_probs_bivariate
from src.models.train import _make_holdout_masks, get_feature_columns

_TRAINING_TABLE = "data/processed/training_table.csv"
_MODEL_HOME = Path("artefacts/model_final_home.pkl")
_MODEL_AWAY = Path("artefacts/model_final_away.pkl")
_RHO_PATH = Path("artefacts/rho.json")
_OUTPUT = Path("outputs/drift_national_recent.txt")

# Friendly labels for the competitions we'll bucket by.
_LEAGUE_NAMES = {
    1: "World Cup",
    4: "Euro Championship",
    5: "UEFA Nations League",
    6: "Copa America",
    7: "Africa Cup of Nations",
    9: "Friendlies International",
    10: "Friendlies",
    11: "CONMEBOL Qualifying",
    13: "Gold Cup",
    32: "World Cup Qualifying - Europe",
    34: "World Cup Qualifying - South America",
    36: "World Cup Qualifying - Africa",
    37: "World Cup Qualifying - Asia",
    480: "Olympics Men",
}


def _drift_block(df: pd.DataFrame, label: str) -> str:
    if df.empty:
        return f"{label}: (empty)"
    pred_total = float((df["lambda_home"] + df["lambda_away"]).sum())
    actual_total = float((df["home_goals"] + df["away_goals"]).sum())
    drift_pct = 100.0 * (pred_total - actual_total) / actual_total if actual_total else float("nan")
    mae_home = float((df["home_goals"] - df["lambda_home"]).abs().mean())
    mae_away = float((df["away_goals"] - df["lambda_away"]).abs().mean())
    return (
        f"{label:<48s}"
        f"  N={len(df):>4d}"
        f"  pred_total={pred_total:>7.1f}"
        f"  actual_total={actual_total:>5.0f}"
        f"  drift={drift_pct:+6.1f}%"
        f"  MAE_avg={(mae_home + mae_away) / 2:.3f}"
    )


def _accuracy(df: pd.DataFrame) -> tuple[float, float]:
    """Exact-scoreline acc and W/D/L acc on the rounded modal λ."""
    actual_score = (
        df["home_goals"].astype(int).astype(str) + "-" + df["away_goals"].astype(int).astype(str)
    )
    score_acc = float((df["predicted_score"] == actual_score).mean())
    actual_outcome = np.where(
        df["home_goals"] > df["away_goals"],
        "home_win",
        np.where(df["home_goals"] < df["away_goals"], "away_win", "draw"),
    )
    outcome_acc = float((df["predicted_outcome"] == actual_outcome).mean())
    return score_acc, outcome_acc


def main() -> None:
    out: list[str] = []

    def emit(s: str) -> None:
        out.append(s)
        sys.stdout.buffer.write((s + "\n").encode("utf-8"))

    df = io.read_csv(_TRAINING_TABLE)
    df["date"] = pd.to_datetime(df["date"], utc=True)

    train_mask, test_mask = _make_holdout_masks(df, mode="national")
    feature_cols = get_feature_columns(df[train_mask], mode="national")
    medians = df[train_mask][feature_cols].median()

    # Recent-window = everything not in train or test, with a known score.
    recent_mask = (~train_mask) & (~test_mask) & df["status"].isin({"FT", "AET", "PEN"})
    recent = df[recent_mask].copy().sort_values("date").reset_index(drop=True)

    model_home = joblib.load(_MODEL_HOME)
    model_away = joblib.load(_MODEL_AWAY)
    import json

    rho = float(json.loads(_RHO_PATH.read_text())["rho"])

    x_in = recent[feature_cols].fillna(medians)
    recent["lambda_home"] = np.clip(model_home.predict(x_in), 0.01, 10.0)
    recent["lambda_away"] = np.clip(model_away.predict(x_in), 0.01, 10.0)

    # Modal scoreline + outcome via bivariate Poisson for the recent rows.
    from src.models.calibrate import _bivariate_poisson_matrix

    pred_scores: list[str] = []
    pred_outcomes: list[str] = []
    for lh, la in zip(recent["lambda_home"], recent["lambda_away"], strict=True):
        mat = _bivariate_poisson_matrix(lh, la, rho)
        idx = np.unravel_index(mat.argmax(), mat.shape)
        pred_scores.append(f"{int(idx[0])}-{int(idx[1])}")
        probs = outcome_probs_bivariate(lh, la, rho)
        pred_outcomes.append(max(probs.items(), key=lambda kv: kv[1])[0])
    recent["predicted_score"] = pred_scores
    recent["predicted_outcome"] = pred_outcomes

    emit("=" * 90)
    emit("NATIONAL-MODE RECENT-WINDOW DRIFT DIAGNOSTIC")
    emit("=" * 90)
    emit("Training cutoff:  WC 2022 start (date < 2022-11-20)")
    emit("Holdout:          WC 2022 (excluded from this diagnostic)")
    emit(
        f"Recent window:    {recent['date'].min().date()} .. "
        f"{recent['date'].max().date()}  (N={len(recent)} fixtures)"
    )
    emit(f"Production ρ:     {rho:+.4f}")
    emit("")

    # Top-level
    emit("=" * 90)
    emit("OVERALL DRIFT")
    emit("=" * 90)
    emit(_drift_block(recent, "All recent fixtures"))
    emit("")

    # Per-year
    emit("=" * 90)
    emit("PER-YEAR DRIFT")
    emit("=" * 90)
    recent["year"] = recent["date"].dt.year
    for year, grp in recent.groupby("year"):
        emit(_drift_block(grp, f"{year}"))
    emit("")

    # Per-competition
    emit("=" * 90)
    emit("PER-COMPETITION DRIFT (sorted by drift magnitude)")
    emit("=" * 90)
    rows: list[tuple[float, str]] = []
    for league_id, grp in recent.groupby("league_id"):
        if len(grp) < 5:
            continue
        actual = float((grp["home_goals"] + grp["away_goals"]).sum())
        pred = float((grp["lambda_home"] + grp["lambda_away"]).sum())
        drift = 100.0 * (pred - actual) / actual if actual else 0.0
        name = _LEAGUE_NAMES.get(int(league_id), f"league_id={int(league_id)}")
        rows.append((drift, _drift_block(grp, f"{name} (id={int(league_id)})")))
    for _drift, line in sorted(rows, key=lambda x: x[0]):
        emit(line)
    emit("")

    # Accuracy metrics on the recent window
    score_acc, outcome_acc = _accuracy(recent)
    mae_home = float((recent["home_goals"] - recent["lambda_home"]).abs().mean())
    mae_away = float((recent["away_goals"] - recent["lambda_away"]).abs().mean())
    emit("=" * 90)
    emit("RECENT-WINDOW ACCURACY (vs WC 2022 holdout reference)")
    emit("=" * 90)
    emit(f"  Recent N:              {len(recent)}")
    emit(f"  MAE (avg):             {(mae_home + mae_away) / 2:.4f}   (holdout: 0.867)")
    emit(f"  Exact-scoreline acc:   {score_acc:.4f}            (holdout: 0.0781)")
    emit(f"  W/D/L accuracy:        {outcome_acc:.4f}            (holdout: 0.500)")
    emit("")

    # Scoreline distribution on recent
    emit("=" * 90)
    emit("PREDICTED-SCORELINE DISTRIBUTION (recent window)")
    emit("=" * 90)
    counts = recent["predicted_score"].value_counts().head(10)
    for score, count in counts.items():
        pct = 100.0 * count / len(recent)
        emit(f"  {score:>6s}  {count:>4d}  ({pct:5.1f}%)")
    emit("")

    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT.write_text("\n".join(out), encoding="utf-8")


if __name__ == "__main__":
    main()
