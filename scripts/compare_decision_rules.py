"""Side-by-side comparison: argmax_v0 vs outcome_conditional_v0.

Runs both decision rules on:
  1. The WC 2022 holdout (with actuals — measures accuracy)
  2. The current WC 2026 inference table (no actuals — measures diversity)

Reports for each:
  - Distribution of predicted scorelines (top-10 most frequent)
  - Number of distinct scorelines
  - For holdout: exact-scoreline accuracy, W/D/L accuracy, MAE

Does NOT modify any artefacts. Read-only diagnostic.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from src.features import io
from src.inference.predict import _load_artefacts, _predict_rows
from src.models.train import _make_holdout_masks, get_feature_columns


def _summarize_distribution(scores: pd.Series, top_n: int = 10) -> str:
    counter = Counter(scores)
    total = len(scores)
    lines = [f"  Distinct scorelines: {len(counter)} / {total} fixtures"]
    for score, count in counter.most_common(top_n):
        pct = 100.0 * count / total
        lines.append(f"    {score:>6s}  {count:>4d}  ({pct:5.1f}%)")
    if len(counter) > top_n:
        tail = sum(c for _, c in counter.most_common()[top_n:])
        lines.append(f"    other   {tail:>4d}  ({100.0 * tail / total:5.1f}%)")
    return "\n".join(lines)


def _accuracy_summary(df: pd.DataFrame) -> str:
    """Compute exact-scoreline, W/D/L, MAE on a holdout df with actuals."""
    actual_h = df["home_goals"].astype(int)
    actual_a = df["away_goals"].astype(int)
    actual_score = actual_h.astype(str) + "-" + actual_a.astype(str)
    actual_outcome = np.where(
        actual_h > actual_a, "home_win", np.where(actual_h < actual_a, "away_win", "draw")
    )

    score_acc = float((df["predicted_score"] == actual_score).mean())
    outcome_acc = float((df["predicted_outcome"] == actual_outcome).mean())
    mae_h = float((actual_h - df["lambda_home"]).abs().mean())
    mae_a = float((actual_a - df["lambda_away"]).abs().mean())
    return (
        f"  Exact-scoreline acc: {score_acc:.4f}\n"
        f"  W/D/L accuracy:      {outcome_acc:.4f}\n"
        f"  MAE (avg):           {(mae_h + mae_a) / 2:.4f}"
    )


def _run_rule(
    rows: pd.DataFrame,
    feature_cols: list[str],
    medians: pd.Series,
    artefacts: tuple,
    rule: str,
) -> pd.DataFrame:
    model_home, model_away, scaler, rho, _trained_at = artefacts
    return _predict_rows(
        rows, feature_cols, medians, model_home, model_away, scaler, rho, decision_rule=rule
    )


def main() -> None:
    out_lines: list[str] = []

    def emit(s: str) -> None:
        out_lines.append(s)
        sys.stdout.buffer.write((s + "\n").encode("utf-8"))

    train_df = io.read_csv("data/processed/training_table.csv")
    train_df["date"] = pd.to_datetime(train_df["date"], utc=True)
    feature_cols = get_feature_columns(train_df, mode="national")
    medians = train_df[feature_cols].median()

    artefacts = _load_artefacts("artefacts")

    # 1. WC 2022 holdout
    _train_mask, test_mask = _make_holdout_masks(train_df, "national")
    holdout = train_df[test_mask].copy()

    argmax = _run_rule(holdout, feature_cols, medians, artefacts, "argmax_v0")
    cond = _run_rule(holdout, feature_cols, medians, artefacts, "outcome_conditional_v0")

    emit("=" * 70)
    emit(f"WC 2022 HOLDOUT ({len(holdout)} fixtures)")
    emit("=" * 70)
    emit("")
    emit("--- argmax_v0 (current production) ---")
    emit(_summarize_distribution(argmax["predicted_score"]))
    emit(_accuracy_summary(argmax))
    emit("")
    emit("--- outcome_conditional_v0 ---")
    emit(_summarize_distribution(cond["predicted_score"]))
    emit(_accuracy_summary(cond))
    emit("")

    # 2. WC 2026 inference table (no actuals — diversity only)
    inf = io.read_parquet("data/processed/inference_table.parquet")
    wc_2026 = inf[inf["league_id"] == 1].copy()

    argmax_inf = _run_rule(wc_2026, feature_cols, medians, artefacts, "argmax_v0")
    cond_inf = _run_rule(wc_2026, feature_cols, medians, artefacts, "outcome_conditional_v0")

    emit("=" * 70)
    emit(f"WC 2026 UPCOMING ({len(wc_2026)} fixtures)")
    emit("=" * 70)
    emit("")
    emit("--- argmax_v0 ---")
    emit(_summarize_distribution(argmax_inf["predicted_score"]))
    emit("")
    emit("--- outcome_conditional_v0 ---")
    emit(_summarize_distribution(cond_inf["predicted_score"]))
    emit("")

    out_path = Path("outputs/decision_rule_comparison.txt")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(out_lines), encoding="utf-8")


if __name__ == "__main__":
    main()
