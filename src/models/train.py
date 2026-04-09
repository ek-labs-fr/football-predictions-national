"""Model training: splits, baselines, and candidate Poisson / classification models.

Covers Steps 3.1 (split), 3.2 (baselines), 3.3 (candidates), and 3.11 (predict_match).
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression, PoissonRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder, StandardScaler

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
ARTEFACTS_DIR = Path("artefacts")
OUTPUTS_DIR = Path("outputs")

WC_2022_LEAGUE_ID = 1
WC_2022_SEASON = 2022
WC_2022_START = "2022-11-20"

# Columns that are not features
_NON_FEATURE_COLS = {
    "fixture_id",
    "date",
    "league_id",
    "season",
    "round",
    "stage",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "home_goals",
    "away_goals",
    "home_goals_ht",
    "away_goals_ht",
    "outcome",
    "status",
    "goal_diff",
    "home_form_last5",
    "away_form_last5",
    "h2h_last_winner",
}

# Maximum goals to consider in scoreline matrix
_MAX_GOALS = 10


# ------------------------------------------------------------------
# Data types
# ------------------------------------------------------------------


@dataclass
class SplitData:
    """Container for train/test split."""

    X_train: pd.DataFrame
    y_home_train: pd.Series
    y_away_train: pd.Series
    y_outcome_train: pd.Series
    w_train: pd.Series
    X_test: pd.DataFrame
    y_home_test: pd.Series
    y_away_test: pd.Series
    y_outcome_test: pd.Series
    w_test: pd.Series
    feature_cols: list[str]
    cv: TimeSeriesSplit
    label_encoder: LabelEncoder


@dataclass
class TrainedModel:
    """Container for a trained model pair or single classifier."""

    name: str
    model_home: Any = None
    model_away: Any = None
    classifier: Any = None
    scaler: StandardScaler | None = None
    is_poisson: bool = True
    train_time: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


# ------------------------------------------------------------------
# Step 3.1 — Train/Test Split
# ------------------------------------------------------------------


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    """Identify numeric feature columns (exclude labels, IDs, strings)."""
    numeric = df.select_dtypes(include="number").columns.tolist()
    return [c for c in numeric if c not in _NON_FEATURE_COLS]


def create_split(
    training_table_path: str | Path = PROCESSED_DIR / "training_table.csv",
    n_cv_splits: int = 5,
) -> SplitData:
    """Create time-based train/test split with WC 2022 as holdout.

    Returns a SplitData container with features, labels, weights, and CV splitter.
    """
    df = pd.read_csv(training_table_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)

    # Holdout: WC 2022 matches
    test_mask = (df["league_id"] == WC_2022_LEAGUE_ID) & (df["season"] == WC_2022_SEASON)
    train_mask = df["date"] < WC_2022_START

    train_df = df[train_mask].copy()
    test_df = df[test_mask].copy()

    feature_cols = get_feature_columns(df)

    # Encode outcome labels
    le = LabelEncoder()
    le.fit(["away_win", "draw", "home_win"])

    # Fill missing feature values with column median
    X_train = train_df[feature_cols].fillna(train_df[feature_cols].median())
    X_test = test_df[feature_cols].fillna(train_df[feature_cols].median())

    # Sample weights from match_weight column
    w_train = (
        train_df["match_weight"]
        if "match_weight" in train_df.columns
        else pd.Series(np.ones(len(train_df)))
    )
    w_test = (
        test_df["match_weight"]
        if "match_weight" in test_df.columns
        else pd.Series(np.ones(len(test_df)))
    )

    cv = TimeSeriesSplit(n_splits=n_cv_splits)

    logger.info(
        "Split: train=%d (up to %s), test=%d (WC 2022)",
        len(train_df),
        train_df["date"].max(),
        len(test_df),
    )
    logger.info("Features: %d columns", len(feature_cols))
    logger.info("Outcome distribution (train):\n%s", train_df["outcome"].value_counts().to_string())
    logger.info("Outcome distribution (test):\n%s", test_df["outcome"].value_counts().to_string())

    return SplitData(
        X_train=X_train,
        y_home_train=train_df["home_goals"].astype(int),
        y_away_train=train_df["away_goals"].astype(int),
        y_outcome_train=pd.Series(le.transform(train_df["outcome"]), index=train_df.index),
        w_train=w_train,
        X_test=X_test,
        y_home_test=test_df["home_goals"].astype(int),
        y_away_test=test_df["away_goals"].astype(int),
        y_outcome_test=pd.Series(le.transform(test_df["outcome"]), index=test_df.index),
        w_test=w_test,
        feature_cols=feature_cols,
        cv=cv,
        label_encoder=le,
    )


# ------------------------------------------------------------------
# Scoreline matrix utilities
# ------------------------------------------------------------------


def scoreline_matrix(
    lambda_home: float, lambda_away: float, max_goals: int = _MAX_GOALS
) -> np.ndarray:
    """Compute P(home=h, away=a) for independent Poisson."""
    home_probs = poisson.pmf(np.arange(max_goals + 1), lambda_home)
    away_probs = poisson.pmf(np.arange(max_goals + 1), lambda_away)
    return np.outer(home_probs, away_probs)


def outcome_probs_from_lambdas(
    lambda_home: float, lambda_away: float, max_goals: int = _MAX_GOALS
) -> dict[str, float]:
    """Derive P(home_win), P(draw), P(away_win) from Poisson lambdas."""
    mat = scoreline_matrix(lambda_home, lambda_away, max_goals)
    p_home = float(np.tril(mat, -1).sum())  # home > away (below diagonal)
    p_draw = float(np.trace(mat))
    p_away = float(np.triu(mat, 1).sum())  # away > home (above diagonal)
    total = p_home + p_draw + p_away
    return {"home_win": p_home / total, "draw": p_draw / total, "away_win": p_away / total}


def most_likely_score(lambda_home: float, lambda_away: float, max_goals: int = _MAX_GOALS) -> str:
    """Return the most probable scoreline as 'H-A'."""
    mat = scoreline_matrix(lambda_home, lambda_away, max_goals)
    idx = np.unravel_index(mat.argmax(), mat.shape)
    return f"{idx[0]}-{idx[1]}"


# ------------------------------------------------------------------
# Step 3.2 — Baseline Models
# ------------------------------------------------------------------


def train_baselines(split: SplitData) -> list[TrainedModel]:
    """Train baseline models for performance floor comparison."""
    models: list[TrainedModel] = []

    # Baseline 1 — Mean goals
    mean_home = float(split.y_home_train.mean())
    mean_away = float(split.y_away_train.mean())
    models.append(
        TrainedModel(
            name="baseline_mean_goals",
            is_poisson=True,
            metadata={"mean_home": mean_home, "mean_away": mean_away},
        )
    )

    # Baseline 2 — FIFA rank-only Poisson
    if "rank_diff" in split.feature_cols:
        t0 = time.time()
        rank_idx = split.feature_cols.index("rank_diff")
        X_rank_train = split.X_train.iloc[:, [rank_idx]]

        pr_home = PoissonRegressor(alpha=1.0, max_iter=500)
        pr_away = PoissonRegressor(alpha=1.0, max_iter=500)
        pr_home.fit(X_rank_train, split.y_home_train, sample_weight=split.w_train)
        pr_away.fit(X_rank_train, split.y_away_train, sample_weight=split.w_train)

        models.append(
            TrainedModel(
                name="baseline_rank_poisson",
                model_home=pr_home,
                model_away=pr_away,
                is_poisson=True,
                train_time=time.time() - t0,
                metadata={"features_used": ["rank_diff"]},
            )
        )

    # Baseline 3 — Majority class
    t0 = time.time()
    dummy = DummyClassifier(strategy="most_frequent")
    dummy.fit(split.X_train, split.y_outcome_train, sample_weight=split.w_train)
    models.append(
        TrainedModel(
            name="baseline_majority_class",
            classifier=dummy,
            is_poisson=False,
            train_time=time.time() - t0,
        )
    )

    logger.info("Trained %d baseline models", len(models))
    return models


# ------------------------------------------------------------------
# Step 3.3 — Candidate Models
# ------------------------------------------------------------------


def train_candidates(split: SplitData) -> list[TrainedModel]:
    """Train all candidate models on the full feature set."""
    models: list[TrainedModel] = []

    # --- Primary: Poisson Regression (linear) ---
    t0 = time.time()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(split.X_train)

    pr_home = PoissonRegressor(alpha=1.0, max_iter=1000)
    pr_away = PoissonRegressor(alpha=1.0, max_iter=1000)
    pr_home.fit(X_scaled, split.y_home_train, sample_weight=split.w_train)
    pr_away.fit(X_scaled, split.y_away_train, sample_weight=split.w_train)

    models.append(
        TrainedModel(
            name="poisson_linear",
            model_home=pr_home,
            model_away=pr_away,
            scaler=scaler,
            is_poisson=True,
            train_time=time.time() - t0,
        )
    )

    # --- Primary: XGBoost Poisson ---
    try:
        from xgboost import XGBRegressor

        t0 = time.time()
        xgb_home = XGBRegressor(
            objective="count:poisson",
            n_estimators=500,
            learning_rate=0.05,
            max_depth=5,
            subsample=0.8,
            colsample_bytree=0.8,
            early_stopping_rounds=50,
            eval_metric="poisson-nloglik",
            verbosity=0,
        )
        xgb_away = XGBRegressor(
            objective="count:poisson",
            n_estimators=500,
            learning_rate=0.05,
            max_depth=5,
            subsample=0.8,
            colsample_bytree=0.8,
            early_stopping_rounds=50,
            eval_metric="poisson-nloglik",
            verbosity=0,
        )
        # Use last 20% of training data as eval set for early stopping
        split_idx = int(len(split.X_train) * 0.8)
        X_tr, X_ev = split.X_train.iloc[:split_idx], split.X_train.iloc[split_idx:]
        y_h_tr, y_h_ev = split.y_home_train.iloc[:split_idx], split.y_home_train.iloc[split_idx:]
        y_a_tr, y_a_ev = split.y_away_train.iloc[:split_idx], split.y_away_train.iloc[split_idx:]
        w_tr = split.w_train.iloc[:split_idx]

        xgb_home.fit(X_tr, y_h_tr, sample_weight=w_tr, eval_set=[(X_ev, y_h_ev)])
        xgb_away.fit(X_tr, y_a_tr, sample_weight=w_tr, eval_set=[(X_ev, y_a_ev)])

        models.append(
            TrainedModel(
                name="xgboost_poisson",
                model_home=xgb_home,
                model_away=xgb_away,
                is_poisson=True,
                train_time=time.time() - t0,
            )
        )
    except ImportError:
        logger.warning("XGBoost not available — skipping xgboost_poisson")

    # --- Primary: LightGBM Poisson ---
    try:
        from lightgbm import LGBMRegressor

        t0 = time.time()
        lgb_home = LGBMRegressor(
            objective="poisson",
            n_estimators=500,
            learning_rate=0.05,
            max_depth=5,
            subsample=0.8,
            colsample_bytree=0.8,
            verbosity=-1,
        )
        lgb_away = LGBMRegressor(
            objective="poisson",
            n_estimators=500,
            learning_rate=0.05,
            max_depth=5,
            subsample=0.8,
            colsample_bytree=0.8,
            verbosity=-1,
        )
        lgb_home.fit(
            X_tr,
            y_h_tr,
            sample_weight=w_tr,
            eval_set=[(X_ev, y_h_ev)],
            callbacks=[_lgb_early_stopping(50)],
        )
        lgb_away.fit(
            X_tr,
            y_a_tr,
            sample_weight=w_tr,
            eval_set=[(X_ev, y_a_ev)],
            callbacks=[_lgb_early_stopping(50)],
        )

        models.append(
            TrainedModel(
                name="lightgbm_poisson",
                model_home=lgb_home,
                model_away=lgb_away,
                is_poisson=True,
                train_time=time.time() - t0,
            )
        )
    except ImportError:
        logger.warning("LightGBM not available — skipping lightgbm_poisson")

    # --- Secondary: XGBoost Classifier ---
    try:
        from xgboost import XGBClassifier

        t0 = time.time()
        xgb_clf = XGBClassifier(
            objective="multi:softprob",
            num_class=3,
            n_estimators=500,
            learning_rate=0.05,
            max_depth=5,
            subsample=0.8,
            colsample_bytree=0.8,
            early_stopping_rounds=50,
            eval_metric="mlogloss",
            verbosity=0,
        )
        xgb_clf.fit(
            X_tr,
            split.y_outcome_train.iloc[:split_idx],
            sample_weight=w_tr,
            eval_set=[(X_ev, split.y_outcome_train.iloc[split_idx:])],
        )
        models.append(
            TrainedModel(
                name="xgboost_classifier",
                classifier=xgb_clf,
                is_poisson=False,
                train_time=time.time() - t0,
            )
        )
    except ImportError:
        logger.warning("XGBoost not available — skipping xgboost_classifier")

    # --- Secondary: Logistic Regression ---
    t0 = time.time()
    lr = LogisticRegression(
        class_weight="balanced",
        max_iter=1000,
    )
    lr.fit(X_scaled, split.y_outcome_train, sample_weight=split.w_train)
    models.append(
        TrainedModel(
            name="logistic_regression",
            classifier=lr,
            scaler=scaler,
            is_poisson=False,
            train_time=time.time() - t0,
        )
    )

    logger.info("Trained %d candidate models", len(models))
    for m in models:
        logger.info("  %s — %.2fs", m.name, m.train_time)
    return models


def _lgb_early_stopping(stopping_rounds: int) -> Any:
    """Return LightGBM early stopping callback."""
    from lightgbm import early_stopping

    return early_stopping(stopping_rounds, verbose=False)


# ------------------------------------------------------------------
# Prediction helpers
# ------------------------------------------------------------------


def predict_lambdas(
    model: TrainedModel,
    X: pd.DataFrame,
    split: SplitData | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Predict home/away λ values for a Poisson model pair."""
    if model.scaler is not None:
        X_input = model.scaler.transform(X)
    else:
        X_input = X.values if hasattr(X, "values") else X

    if model.name == "baseline_mean_goals":
        n = len(X)
        return (
            np.full(n, model.metadata["mean_home"]),
            np.full(n, model.metadata["mean_away"]),
        )

    if model.name == "baseline_rank_poisson" and split is not None:
        rank_idx = split.feature_cols.index("rank_diff")
        X_input = X.iloc[:, [rank_idx]]

    lambda_home = model.model_home.predict(X_input)
    lambda_away = model.model_away.predict(X_input)
    return np.clip(lambda_home, 0.01, 10.0), np.clip(lambda_away, 0.01, 10.0)


def predict_outcome_probs(
    model: TrainedModel,
    X: pd.DataFrame,
    split: SplitData | None = None,
) -> np.ndarray:
    """Predict outcome probabilities [away_win, draw, home_win] for each row.

    For Poisson models, derives probabilities via scoreline matrix.
    For classifiers, uses predict_proba directly.
    """
    if model.is_poisson:
        lh, la = predict_lambdas(model, X, split)
        probs = []
        for h, a in zip(lh, la, strict=True):
            p = outcome_probs_from_lambdas(h, a)
            probs.append([p["away_win"], p["draw"], p["home_win"]])
        return np.array(probs)
    else:
        X_input = model.scaler.transform(X) if model.scaler is not None else X
        return model.classifier.predict_proba(X_input)


# ------------------------------------------------------------------
# Save / load
# ------------------------------------------------------------------


def save_model(model: TrainedModel, artefacts_dir: Path = ARTEFACTS_DIR) -> None:
    """Save a trained model to disk."""
    artefacts_dir.mkdir(parents=True, exist_ok=True)
    if model.model_home is not None:
        joblib.dump(model.model_home, artefacts_dir / f"{model.name}_home.pkl")
    if model.model_away is not None:
        joblib.dump(model.model_away, artefacts_dir / f"{model.name}_away.pkl")
    if model.classifier is not None:
        joblib.dump(model.classifier, artefacts_dir / f"{model.name}_clf.pkl")
    if model.scaler is not None:
        joblib.dump(model.scaler, artefacts_dir / f"{model.name}_scaler.pkl")
    if model.metadata:
        meta_path = artefacts_dir / f"{model.name}_meta.json"
        meta_path.write_text(json.dumps(model.metadata, indent=2), encoding="utf-8")
    logger.info("Saved model %s to %s", model.name, artefacts_dir)


# ------------------------------------------------------------------
# Step 3.11 — predict_match inference
# ------------------------------------------------------------------


def predict_match(
    lambda_home: float,
    lambda_away: float,
) -> dict[str, Any]:
    """Produce a full prediction from pre-computed λ values.

    Parameters
    ----------
    lambda_home:
        Expected goals for the home team.
    lambda_away:
        Expected goals for the away team.

    Returns
    -------
    dict with lambda_home, lambda_away, most_likely_score, home_win, draw, away_win.
    """
    probs = outcome_probs_from_lambdas(lambda_home, lambda_away)
    return {
        "lambda_home": round(lambda_home, 3),
        "lambda_away": round(lambda_away, 3),
        "most_likely_score": most_likely_score(lambda_home, lambda_away),
        "home_win": round(probs["home_win"], 4),
        "draw": round(probs["draw"], 4),
        "away_win": round(probs["away_win"], 4),
    }
