"""Data quality and leakage audit for the training table (Step 2.8).

Usage:
    uv run python scripts/validate_features.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROCESSED = Path("data/processed")
OUTPUTS = Path("outputs")


def _check(label: str, passed: bool, detail: str = "") -> bool:
    status = "PASS" if passed else "FAIL"
    msg = f"  [{status}] {label}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    return passed


def validate() -> bool:
    all_ok = True

    print("\n=== Data Quality & Leakage Audit ===\n")

    path = PROCESSED / "training_table.csv"
    if not path.exists():
        print(f"  [FAIL] {path} not found — run build_training_table() first")
        return False

    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    print(f"  Date range: {df['date'].min()} – {df['date'].max()}")

    # --- Duplicate check ---
    all_ok &= _check(
        "No duplicate rows",
        not df.duplicated().any(),
        f"{df.duplicated().sum()} duplicates" if df.duplicated().any() else "",
    )

    all_ok &= _check(
        "No duplicate fixture_ids",
        df["fixture_id"].is_unique,
        f"{df['fixture_id'].duplicated().sum()} dupes" if not df["fixture_id"].is_unique else "",
    )

    # --- Class balance ---
    print("\n  Outcome distribution:")
    for outcome, count in df["outcome"].value_counts().items():
        pct = count / len(df) * 100
        print(f"    {outcome}: {count} ({pct:.1f}%)")

    # --- Missing values ---
    print("\n  Missing values (columns with >0%):")
    missing_pct = (df.isna().sum() / len(df) * 100).sort_values(ascending=False)
    high_missing = missing_pct[missing_pct > 0]
    if high_missing.empty:
        print("    None")
    else:
        for col, pct in high_missing.items():
            flag = " *** HIGH" if pct > 30 else ""
            print(f"    {col}: {pct:.1f}%{flag}")

    flagged = missing_pct[missing_pct > 30]
    if not flagged.empty:
        _check(
            "No columns with >30% missing",
            False,
            f"{len(flagged)} columns: {list(flagged.index[:5])}",
        )

    # --- Zero-variance check ---
    numeric = df.select_dtypes(include="number")
    zero_var = numeric.columns[numeric.var() == 0].tolist()
    all_ok &= _check(
        "No zero-variance features",
        len(zero_var) == 0,
        f"{zero_var}" if zero_var else "",
    )

    # --- Feature distributions ---
    print("\n  Numeric feature summary:")
    summary = numeric.describe().T[["mean", "std", "min", "max"]]
    print(summary.to_string())

    # --- High correlations ---
    print("\n  Highly correlated pairs (|r| > 0.95):")
    corr = numeric.corr().abs()
    # Zero out diagonal and lower triangle
    upper = corr.where(pd.np.triu(pd.np.ones(corr.shape), k=1).astype(bool))  # type: ignore[attr-defined]
    high_corr_pairs: list[tuple[str, str, float]] = []
    for col in upper.columns:
        for idx in upper.index:
            val = upper.loc[idx, col]
            if pd.notna(val) and val > 0.95:
                high_corr_pairs.append((str(idx), str(col), float(val)))

    if high_corr_pairs:
        for a, b, r in high_corr_pairs[:10]:
            print(f"    {a} <-> {b}: {r:.3f}")
    else:
        print("    None")

    # --- Leakage spot check ---
    rolling_path = PROCESSED / "features_rolling.csv"
    if rolling_path.exists():
        rolling = pd.read_csv(rolling_path)
        # Verify matches_available is never negative
        if "matches_available" in rolling.columns:
            all_ok &= _check(
                "Rolling features: no negative matches_available",
                (rolling["matches_available"] >= 0).all(),
            )

    # --- Write report ---
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUTS / "data_quality_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"Training table: {len(df)} rows, {len(df.columns)} columns\n")
        f.write(f"Date range: {df['date'].min()} – {df['date'].max()}\n\n")
        f.write("Outcome distribution:\n")
        f.write(df["outcome"].value_counts().to_string())
        f.write("\n\nMissing values:\n")
        f.write(high_missing.to_string() if not high_missing.empty else "None\n")
        f.write("\n\nZero-variance columns: ")
        f.write(str(zero_var) if zero_var else "None")
        f.write(f"\n\nHigh correlation pairs: {len(high_corr_pairs)}\n")
        for a, b, r in high_corr_pairs:
            f.write(f"  {a} <-> {b}: {r:.3f}\n")
    print(f"\n  Report saved to {report_path}")

    print(f"\n  Overall: {'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
    return all_ok


if __name__ == "__main__":
    ok = validate()
    sys.exit(0 if ok else 1)
