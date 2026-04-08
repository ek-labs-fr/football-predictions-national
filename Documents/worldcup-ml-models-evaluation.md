# World Cup ML Prediction — Models, Evaluation & Explainability Guide

> Which models to train, how to evaluate them rigorously, how to select the best features,
> and how to explain predictions using SHAP values.
> **Primary approach:** Poisson-based goal prediction models that predict expected goals per team,
> then derive scoreline probabilities and W/D/L outcomes via a goal matrix.
> Assumes features are clean and assembled per `worldcup-ml-data-pipeline.md`.

---

## Table of Contents

1. [Overview & Modelling Philosophy](#overview--modelling-philosophy)
2. [Baseline Models](#baseline-models)
3. [Core Models to Test](#core-models-to-test)
4. [Hyperparameter Tuning](#hyperparameter-tuning)
5. [Evaluation Strategy](#evaluation-strategy)
6. [Evaluation Metrics](#evaluation-metrics)
7. [Feature Selection](#feature-selection)
8. [Explainability with SHAP](#explainability-with-shap)
9. [Model Comparison & Selection](#model-comparison--selection)
10. [Production Readiness Checks](#production-readiness-checks)

---

## Overview & Modelling Philosophy

Predicting football matches is a **low-signal, high-noise problem**. Even the best models in the world rarely exceed 55–60% accuracy on match outcomes — upsets are real, variance is high, and football is designed to be unpredictable. The goal is not to be perfect, but to be **systematically better than a naive baseline** and to understand *why* the model makes each prediction.

**Primary modelling approach — Poisson goal prediction:**

The system predicts **expected goals per team** (λ_home, λ_away) using independent Poisson regression models. From these, all other outputs are derived:

| Output | Derivation |
|---|---|
| Scoreline probability matrix | P(h,a) = Poisson(h; λ_home) × Poisson(a; λ_away) |
| W/D/L probabilities | Sum over matrix: P(win) = Σ P(h,a) where h>a, etc. |
| Most likely scoreline | argmax over the matrix |
| Expected goal difference | λ_home − λ_away |

This approach is preferred because: (1) it respects the discrete count nature of goals, (2) it produces a full scoreline distribution needed for tournament simulation (goal difference tiebreakers), and (3) it is the standard in professional football analytics.

**Secondary models** (classification-based XGBoost/LightGBM predicting W/D/L directly) are trained for comparison and potential ensembling, but the Poisson models are the primary production system.

**Tooling:**

```python
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.model_selection import StratifiedKFold, TimeSeriesSplit
from sklearn.metrics import (
    accuracy_score, log_loss, brier_score_loss,
    classification_report, confusion_matrix,
    mean_absolute_error, mean_squared_error
)
import matplotlib.pyplot as plt
import shap
import joblib
```

---

## Baseline Models

Always establish baselines first. A sophisticated model that cannot beat a naive baseline is not useful.

### Baseline 1 — Majority Class

Predict the most common outcome in the training set every time. Football datasets typically show ~45% home win, ~25% draw, ~30% away win in international football.

```python
from sklearn.dummy import DummyClassifier

baseline_majority = DummyClassifier(strategy="most_frequent")
baseline_majority.fit(X_train, y_train)
acc_majority = accuracy_score(y_test, baseline_majority.predict(X_test))
print(f"Majority class accuracy: {acc_majority:.3f}")
```

### Baseline 2 — Prior Probability

Predict according to historical class frequencies rather than always predicting the majority.

```python
baseline_prior = DummyClassifier(strategy="prior")
baseline_prior.fit(X_train, y_train)
```

### Baseline 3 — FIFA Ranking Only

A single-feature logistic regression using only the FIFA rank differential. This represents the "common wisdom" benchmark — if your model can't beat FIFA rankings alone, something is wrong.

```python
from sklearn.linear_model import LogisticRegression

baseline_rank = LogisticRegression(max_iter=1000)
baseline_rank.fit(X_train[["rank_diff"]], y_train)
acc_rank = accuracy_score(y_test, baseline_rank.predict(X_test[["rank_diff"]]))
print(f"FIFA rank-only accuracy: {acc_rank:.3f}")
```

### Baseline 4 — Betting Market Odds (if available)

If you have access to historical bookmaker odds, converting them to implied probabilities gives an extremely strong baseline. Bookmakers aggregate vast amounts of information. If your model cannot outperform the market, consider whether the marginal value of building a model is worth the complexity.

```python
# Convert odds to implied probabilities
def odds_to_prob(home_odds, draw_odds, away_odds):
    raw = [1/home_odds, 1/draw_odds, 1/away_odds]
    total = sum(raw)
    return [r / total for r in raw]   # normalise to remove overround
```

---

## Core Models to Test

Test a diverse set of model families. Each makes different assumptions about the data and will surface different signal from the features.

### Model 1 — Logistic Regression

The interpretable linear baseline. Good at telling you which features have consistent directional effects. Serves as a sanity check: if tree models don't significantly beat logistic regression, your features may not have strong non-linear interactions.

```python
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

lr_model = Pipeline([
    ("scaler", StandardScaler()),
    ("clf", LogisticRegression(
        C=1.0,
        multi_class="multinomial",
        solver="lbfgs",
        max_iter=1000,
        class_weight="balanced",   # handles class imbalance (draws are rare)
        random_state=42
    ))
])
```

**When it wins:** When features have strong linear relationships with the outcome and there are limited training samples.

**Weaknesses:** Cannot capture non-linear interactions (e.g. "a team with high squad rating AND low rest performs poorly").

---

### Model 2 — Random Forest

A strong non-linear baseline. Naturally handles missing values via surrogate splits, is robust to outliers, and provides built-in feature importance scores.

```python
from sklearn.ensemble import RandomForestClassifier

rf_model = RandomForestClassifier(
    n_estimators=500,
    max_depth=8,
    min_samples_leaf=5,     # prevents overfitting on small football datasets
    max_features="sqrt",
    class_weight="balanced_subsample",
    random_state=42,
    n_jobs=-1
)
```

**When it wins:** When feature interactions matter but the dataset is small and regularisation via shallow trees helps.

**Weaknesses:** Slower to train at scale; not as strong as gradient boosting on tabular data.

---

### Model 3 — XGBoost (Primary Model)

The workhorse of tabular ML competitions. Handles missing values natively, is highly regularisable, and typically outperforms Random Forest on structured data. This is your most likely best performer.

```python
from xgboost import XGBClassifier

xgb_model = XGBClassifier(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=5,
    min_child_weight=3,
    subsample=0.8,
    colsample_bytree=0.8,
    gamma=0.1,
    reg_alpha=0.1,          # L1 regularisation
    reg_lambda=1.0,         # L2 regularisation
    objective="multi:softprob",
    num_class=3,
    eval_metric="mlogloss",
    use_label_encoder=False,
    random_state=42,
    n_jobs=-1
)
```

**When it wins:** Almost always on tabular data, especially when the dataset is medium-sized and features have complex interactions.

**Key advantage:** `early_stopping_rounds` prevents overfitting without manual tuning of `n_estimators`.

```python
# Use early stopping during fit
xgb_model.fit(
    X_train, y_train,
    sample_weight=w_train,
    eval_set=[(X_val, y_val)],
    early_stopping_rounds=50,
    verbose=False
)
```

---

### Model 4 — LightGBM

Faster than XGBoost, better on high-cardinality categoricals, and often comparable or superior in accuracy. Use it as a second gradient boosting candidate to cross-validate XGBoost's conclusions.

```python
from lightgbm import LGBMClassifier

lgbm_model = LGBMClassifier(
    n_estimators=500,
    learning_rate=0.05,
    num_leaves=31,
    max_depth=-1,
    min_child_samples=20,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    objective="multiclass",
    num_class=3,
    class_weight="balanced",
    random_state=42,
    n_jobs=-1,
    verbose=-1
)
```

---

### Model 5 — Poisson Goal Models (PRIMARY)

Football goals follow a Poisson distribution. This is the **primary production model**: predict expected goals for each team separately and derive scoreline probabilities, outcome probabilities, and tournament simulations from the goal distributions. This is the approach used by most professional football modelling systems (FiveThirtyEight, Opta, etc.).

#### 5a. Basic Poisson Regression

```python
from sklearn.linear_model import PoissonRegressor

# Train two separate models: one for home goals, one for away goals
poisson_home = PoissonRegressor(alpha=0.1, max_iter=300)
poisson_away = PoissonRegressor(alpha=0.1, max_iter=300)

poisson_home.fit(X_train_scaled, y_train_home_goals)
poisson_away.fit(X_train_scaled, y_train_away_goals)
```

#### 5b. Gradient Boosted Poisson (stronger variant)

Use XGBoost/LightGBM with Poisson loss for non-linear goal prediction:

```python
from xgboost import XGBRegressor

xgb_poisson_home = XGBRegressor(
    objective="count:poisson",
    n_estimators=500,
    learning_rate=0.05,
    max_depth=5,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
    n_jobs=-1,
)
xgb_poisson_away = XGBRegressor(
    objective="count:poisson",
    n_estimators=500,
    learning_rate=0.05,
    max_depth=5,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
    n_jobs=-1,
)

xgb_poisson_home.fit(X_train, y_train_home_goals, eval_set=[(X_val, y_val_home_goals)],
                      early_stopping_rounds=50, verbose=False)
xgb_poisson_away.fit(X_train, y_train_away_goals, eval_set=[(X_val, y_val_away_goals)],
                      early_stopping_rounds=50, verbose=False)
```

#### 5c. Scoreline Probability Matrix

```python
from scipy.stats import poisson
import numpy as np

def scoreline_matrix(lambda_home, lambda_away, max_goals=8):
    """Build full scoreline probability matrix from predicted λ values."""
    matrix = np.zeros((max_goals + 1, max_goals + 1))
    for h in range(max_goals + 1):
        for a in range(max_goals + 1):
            matrix[h, a] = poisson.pmf(h, lambda_home) * poisson.pmf(a, lambda_away)
    return matrix

def matrix_to_outcome_probs(matrix):
    """Sum scoreline matrix to get W/D/L probabilities."""
    p_home_win = np.triu(matrix, k=1).sum()   # above diagonal
    p_draw = np.trace(matrix)                   # diagonal
    p_away_win = np.tril(matrix, k=-1).sum()   # below diagonal
    return p_home_win, p_draw, p_away_win

def most_likely_scoreline(matrix):
    """Return the most probable scoreline."""
    idx = np.unravel_index(matrix.argmax(), matrix.shape)
    return idx[0], idx[1], matrix[idx]
```

#### 5d. Bivariate Poisson (handling goal correlation)

Independent Poisson assumes home and away goals are uncorrelated. In practice, there is weak positive correlation (open games produce more goals for both sides). A bivariate Poisson or copula-based correction can improve calibration:

```python
def bivariate_poisson_matrix(lambda_home, lambda_away, rho=0.1, max_goals=8):
    """
    Approximate bivariate Poisson via diagonal inflation.
    rho > 0 increases draw probability (positive goal correlation).
    """
    matrix = scoreline_matrix(lambda_home, lambda_away, max_goals)
    # Inflate diagonal (draws) by rho factor
    for g in range(max_goals + 1):
        inflation = rho * poisson.pmf(g, (lambda_home + lambda_away) / 2)
        matrix[g, g] += inflation
    # Re-normalise
    matrix /= matrix.sum()
    return matrix
```

**Why this is the primary model:** It produces exact score predictions needed for group stage simulation (goal difference, goals scored tiebreakers), naturally outputs calibrated probabilities, and is the industry standard for football prediction.

---

### Model 6 — Multi-Layer Perceptron (Neural Network)

Worth testing if you have enough data. Neural networks can capture higher-order feature interactions automatically, but they are prone to overfitting on small datasets and require careful regularisation.

```python
from sklearn.neural_network import MLPClassifier

mlp_model = Pipeline([
    ("scaler", StandardScaler()),
    ("clf", MLPClassifier(
        hidden_layer_sizes=(128, 64, 32),
        activation="relu",
        solver="adam",
        alpha=0.01,           # L2 regularisation
        dropout=0.2,          # use keras if you need dropout
        learning_rate="adaptive",
        max_iter=500,
        early_stopping=True,
        validation_fraction=0.1,
        random_state=42
    ))
])
```

**When it wins:** With 5,000+ training rows and many features. For World Cup data alone (~600 matches) this is likely to underperform simpler models.

---

### Model 7 — Voting Ensemble

Combine the best-performing models to reduce variance and improve robustness. Ensembles are particularly valuable in football where signal is weak and individual models are inconsistent.

```python
from sklearn.ensemble import VotingClassifier

ensemble = VotingClassifier(
    estimators=[
        ("lr",   lr_model),
        ("rf",   rf_model),
        ("xgb",  xgb_model),
        ("lgbm", lgbm_model),
    ],
    voting="soft",            # use predicted probabilities, not hard votes
    weights=[0.15, 0.20, 0.35, 0.30]   # weight better models more
)
```

---

## Hyperparameter Tuning

### Strategy: Bayesian Optimisation

Grid search is too expensive. Use Bayesian optimisation (Optuna) to efficiently search the hyperparameter space.

```python
import optuna
from sklearn.model_selection import cross_val_score

def xgb_objective(trial):
    params = {
        "n_estimators":      trial.suggest_int("n_estimators", 100, 1000),
        "learning_rate":     trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "max_depth":         trial.suggest_int("max_depth", 3, 8),
        "min_child_weight":  trial.suggest_int("min_child_weight", 1, 10),
        "subsample":         trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "gamma":             trial.suggest_float("gamma", 0, 1),
        "reg_alpha":         trial.suggest_float("reg_alpha", 1e-4, 10, log=True),
        "reg_lambda":        trial.suggest_float("reg_lambda", 1e-4, 10, log=True),
    }
    model = XGBClassifier(
        **params,
        objective="multi:softprob",
        num_class=3,
        use_label_encoder=False,
        random_state=42,
        n_jobs=-1
    )
    # Use time-series CV (see Evaluation Strategy)
    scores = cross_val_score(model, X_train, y_train, cv=tscv,
                             scoring="neg_log_loss", n_jobs=-1)
    return scores.mean()

study = optuna.create_study(direction="maximize")
study.optimize(xgb_objective, n_trials=100, timeout=3600)

best_params = study.best_params
print(f"Best log-loss: {-study.best_value:.4f}")
print(f"Best params: {best_params}")
```

---

## Evaluation Strategy

### Why Standard K-Fold is Wrong for This Problem

Random K-Fold cross-validation allows future matches to appear in the training fold and past matches in the validation fold. This causes **temporal leakage** — the model appears to perform better than it will in production. Always use time-based splitting.

### Time-Series Cross-Validation

```python
from sklearn.model_selection import TimeSeriesSplit

# Each fold trains on all past data and validates on the next time window
tscv = TimeSeriesSplit(n_splits=5, gap=0)

# Visualise the splits
for fold, (train_idx, val_idx) in enumerate(tscv.split(X_train)):
    print(f"Fold {fold+1}: "
          f"Train {df.loc[train_idx, 'date'].min().date()} → "
          f"{df.loc[train_idx, 'date'].max().date()} | "
          f"Val {df.loc[val_idx, 'date'].min().date()} → "
          f"{df.loc[val_idx, 'date'].max().date()}")
```

### Tournament-Based Holdout

For the final test evaluation, hold out **entire tournaments** rather than random matches. This is the most realistic simulation of deploying the model to predict an unseen World Cup.

```python
# Hold out the two most recent World Cups as test sets
test_wc_2022 = df[
    (df["league_id"] == 1) & (df["season"] == 2022)
]
test_wc_2018 = df[
    (df["league_id"] == 1) & (df["season"] == 2018)
]

# Train only on data strictly before each test tournament
train_for_2022 = df[df["date"] < pd.Timestamp("2022-11-20")]
train_for_2018 = df[df["date"] < pd.Timestamp("2018-06-14")]
```

### Cross-Validation Loop

```python
results = {}

for model_name, model in models.items():
    fold_metrics = []
    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]
        w_tr = w.iloc[train_idx]

        model.fit(X_tr, y_tr, sample_weight=w_tr)

        y_pred      = model.predict(X_val)
        y_proba     = model.predict_proba(X_val)

        fold_metrics.append({
            "accuracy":   accuracy_score(y_val, y_pred),
            "log_loss":   log_loss(y_val, y_proba),
            "brier":      brier_score_loss(
                              (y_val == 1).astype(int),
                              y_proba[:, 1]   # home win probability
                          ),
        })

    results[model_name] = pd.DataFrame(fold_metrics).mean()

results_df = pd.DataFrame(results).T
print(results_df.sort_values("log_loss"))
```

---

## Evaluation Metrics

Use multiple metrics — no single number captures the full picture of model quality.

### Classification Metrics

**Accuracy** — fraction of correctly predicted outcomes. Simple but misleading if class distribution is uneven.

```python
acc = accuracy_score(y_test, y_pred)
```

**Log Loss (Cross-Entropy)** — penalises confident wrong predictions heavily. The primary metric for probability-outputting models. Lower is better.

```python
ll = log_loss(y_test, y_proba)
```

**Brier Score** — mean squared error of probability predictions. Measures both calibration and sharpness. Lower is better.

```python
from sklearn.metrics import brier_score_loss
# Compute per class and average
brier_scores = []
for cls_idx in range(3):
    bs = brier_score_loss(
        (y_test == cls_idx - 1).astype(int),
        y_proba[:, cls_idx]
    )
    brier_scores.append(bs)
avg_brier = np.mean(brier_scores)
```

**Confusion Matrix** — reveals which mistake types dominate. In football, draws are the hardest class to predict.

```python
from sklearn.metrics import ConfusionMatrixDisplay

cm = confusion_matrix(y_test, y_pred, labels=[-1, 0, 1])
disp = ConfusionMatrixDisplay(
    confusion_matrix=cm,
    display_labels=["Away Win", "Draw", "Home Win"]
)
disp.plot(cmap="Blues")
plt.title("Confusion Matrix — Test Set")
plt.savefig("outputs/confusion_matrix.png", dpi=150)
```

**Per-Class Precision / Recall / F1:**

```python
print(classification_report(
    y_test, y_pred,
    target_names=["Away Win", "Draw", "Home Win"]
))
```

### Goal Prediction Metrics (Primary — Poisson Models)

```python
# MAE on predicted goals per team
mae_home = mean_absolute_error(y_test_home_goals, lambda_home_pred)
mae_away = mean_absolute_error(y_test_away_goals, lambda_away_pred)
print(f"MAE Home: {mae_home:.3f} goals | MAE Away: {mae_away:.3f} goals")

# Exact scoreline accuracy
predicted_scorelines = [(round(lh), round(la)) for lh, la in zip(lambda_home_pred, lambda_away_pred)]
actual_scorelines = list(zip(y_test_home_goals, y_test_away_goals))
exact_acc = sum(p == a for p, a in zip(predicted_scorelines, actual_scorelines)) / len(actual_scorelines)
print(f"Exact scoreline accuracy: {exact_acc:.3f}")

# Ranked Probability Score (RPS) — measures how close the predicted
# cumulative distribution is to the actual outcome
def ranked_probability_score(y_true_goals, lambda_pred, max_goals=8):
    """RPS for a single team's goal prediction."""
    from scipy.stats import poisson
    rps_list = []
    for actual, lam in zip(y_true_goals, lambda_pred):
        cum_pred = np.cumsum([poisson.pmf(g, lam) for g in range(max_goals + 1)])
        cum_true = np.cumsum([1 if g >= actual else 0 for g in range(max_goals + 1)])
        rps_list.append(np.mean((cum_pred - cum_true) ** 2))
    return np.mean(rps_list)

rps = ranked_probability_score(y_test_home_goals, lambda_home_pred)
print(f"RPS (home goals): {rps:.4f}")
```

### Calibration Curve

A well-calibrated model outputs probabilities that match actual frequencies: when it says 70% chance of a home win, a home win should occur ~70% of the time.

```python
from sklearn.calibration import calibration_curve, CalibratedClassifierCV

# Plot calibration for each class
fig, axes = plt.subplots(1, 3, figsize=(15, 5))
class_labels = ["Away Win", "Draw", "Home Win"]

for cls_idx, ax in enumerate(axes):
    prob_true, prob_pred = calibration_curve(
        (y_test == cls_idx - 1).astype(int),
        y_proba[:, cls_idx],
        n_bins=10
    )
    ax.plot(prob_pred, prob_true, marker="o", label="Model")
    ax.plot([0, 1], [0, 1], linestyle="--", color="gray", label="Perfect")
    ax.set_title(f"Calibration — {class_labels[cls_idx]}")
    ax.set_xlabel("Mean Predicted Probability")
    ax.set_ylabel("Fraction of Positives")
    ax.legend()

plt.tight_layout()
plt.savefig("outputs/calibration_curves.png", dpi=150)
```

**Post-hoc calibration** — if the model is poorly calibrated, apply Platt scaling or isotonic regression:

```python
calibrated_model = CalibratedClassifierCV(
    xgb_model, method="isotonic", cv="prefit"
)
calibrated_model.fit(X_cal, y_cal)   # use a held-out calibration set
```

### Stage-Stratified Evaluation

Performance often differs dramatically between group stage and knockout matches. Report metrics separately.

```python
for stage in ["group", "round_of_16", "quarterfinal", "semifinal", "final"]:
    mask = df_test["stage"] == stage
    if mask.sum() < 5:
        continue
    stage_acc = accuracy_score(y_test[mask], y_pred[mask])
    stage_ll  = log_loss(y_test[mask], y_proba[mask])
    print(f"{stage:15s} | n={mask.sum():3d} | Acc={stage_acc:.3f} | LogLoss={stage_ll:.3f}")
```

---

## Feature Selection

With ~50–80 engineered features and only ~600–2,000 training rows, feature selection is important to reduce overfitting and improve model interpretability.

### Method 1 — Variance Threshold (Remove Near-Constant Features)

```python
from sklearn.feature_selection import VarianceThreshold

selector = VarianceThreshold(threshold=0.01)
selector.fit(X_train)
low_var_cols = X_train.columns[~selector.get_support()].tolist()
print(f"Removing {len(low_var_cols)} near-constant features: {low_var_cols}")
X_train = X_train.drop(columns=low_var_cols)
X_test  = X_test.drop(columns=low_var_cols)
```

### Method 2 — Correlation Analysis (Remove Redundant Features)

Highly correlated features add noise without adding information. Keep one from each highly correlated pair.

```python
corr_matrix = X_train.corr().abs()
upper = corr_matrix.where(
    np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
)

# Drop features with correlation > 0.90 to any other feature
correlated_cols = [col for col in upper.columns if any(upper[col] > 0.90)]
print(f"Removing {len(correlated_cols)} highly correlated features")
X_train = X_train.drop(columns=correlated_cols)
X_test  = X_test.drop(columns=correlated_cols)

# Visualise the correlation matrix
import seaborn as sns
plt.figure(figsize=(20, 16))
sns.heatmap(corr_matrix, cmap="coolwarm", center=0, square=True,
            linewidths=0.5, annot=False)
plt.title("Feature Correlation Matrix")
plt.tight_layout()
plt.savefig("outputs/correlation_heatmap.png", dpi=150)
```

### Method 3 — Permutation Importance

Measures how much model performance drops when each feature is randomly shuffled. Unlike built-in tree importance, this works for any model and is not biased towards high-cardinality features.

```python
from sklearn.inspection import permutation_importance

perm_result = permutation_importance(
    xgb_model, X_test, y_test,
    n_repeats=30,
    random_state=42,
    scoring="neg_log_loss",
    n_jobs=-1
)

perm_df = pd.DataFrame({
    "feature":    X_test.columns,
    "importance": perm_result.importances_mean,
    "std":        perm_result.importances_std,
}).sort_values("importance", ascending=False)

# Remove features that hurt or don't help performance
useless = perm_df[perm_df["importance"] < 0]["feature"].tolist()
print(f"Features with negative permutation importance (noise): {useless}")
```

### Method 4 — Recursive Feature Elimination with CV (RFECV)

Systematically removes the weakest features and cross-validates at each step to find the optimal subset.

```python
from sklearn.feature_selection import RFECV
from sklearn.linear_model import LogisticRegression

# Use logistic regression as the estimator for RFECV (faster than XGBoost)
rfecv = RFECV(
    estimator=LogisticRegression(max_iter=1000, class_weight="balanced"),
    step=1,
    cv=tscv,
    scoring="neg_log_loss",
    min_features_to_select=10,
    n_jobs=-1
)
rfecv.fit(X_train_scaled, y_train)

optimal_n = rfecv.n_features_
selected_features = X_train.columns[rfecv.support_].tolist()
print(f"Optimal feature count: {optimal_n}")
print(f"Selected features: {selected_features}")

# Plot CV score vs number of features
plt.figure(figsize=(10, 5))
plt.plot(range(1, len(rfecv.cv_results_["mean_test_score"]) + 1),
         -rfecv.cv_results_["mean_test_score"])
plt.xlabel("Number of Features")
plt.ylabel("Log Loss (CV)")
plt.title("RFECV — Feature Count vs. Performance")
plt.axvline(optimal_n, color="red", linestyle="--", label=f"Optimal: {optimal_n}")
plt.legend()
plt.savefig("outputs/rfecv_curve.png", dpi=150)
```

### Method 5 — XGBoost Native Importance

Use XGBoost's built-in `gain`-based importance as a quick sanity check and starting point, but do not rely on it alone — it tends to overweight high-cardinality continuous features.

```python
import xgboost as xgb

xgb_importance = pd.DataFrame({
    "feature":    X_train.columns,
    "gain":       xgb_model.get_booster().get_score(importance_type="gain").values(),
    "cover":      xgb_model.get_booster().get_score(importance_type="cover").values(),
    "frequency":  xgb_model.get_booster().get_score(importance_type="weight").values(),
}).sort_values("gain", ascending=False)

# Plot top 20
plt.figure(figsize=(10, 8))
xgb_importance.head(20).plot.barh(x="feature", y="gain", legend=False)
plt.xlabel("Information Gain")
plt.title("XGBoost Feature Importance (Gain) — Top 20")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("outputs/xgb_importance.png", dpi=150)
```

### Feature Selection Decision Process

```
1. Start with all engineered features (~60–80)
2. Remove near-zero variance features          → typically removes 5–10
3. Remove highly correlated duplicates (>0.90) → typically removes 10–15
4. Run RFECV to find optimal count             → typically lands at 25–40
5. Validate final set with permutation importance
6. Re-train final model on selected features only
7. Confirm test set performance did not degrade
```

---

## Explainability with SHAP

SHAP (SHapley Additive exPlanations) attributes each prediction to individual features based on game-theory principles. It is the gold standard for model explainability on tabular data.

**Install:**

```bash
pip install shap
```

### 8a. Compute SHAP Values

```python
import shap

# TreeExplainer is optimised for gradient boosting models
explainer = shap.TreeExplainer(xgb_model)
shap_values = explainer.shap_values(X_test)

# shap_values is a list of arrays: one per class
# shap_values[0] = SHAP for "away_win", [1] = "draw", [2] = "home_win"
print(f"SHAP values shape: {shap_values[0].shape}")  # (n_samples, n_features)
```

For non-tree models, use the model-agnostic `KernelExplainer` (slower):

```python
# For logistic regression, MLP, etc.
background = shap.sample(X_train, 100)   # background dataset for integration
explainer_kernel = shap.KernelExplainer(model.predict_proba, background)
shap_values_kernel = explainer_kernel.shap_values(X_test.iloc[:50])  # subset for speed
```

---

### 8b. Global Feature Importance — Summary Plot

Shows which features drive predictions most across the entire test set, and in which direction.

```python
# For the "home win" class (index 2)
plt.figure(figsize=(10, 10))
shap.summary_plot(
    shap_values[2],
    X_test,
    feature_names=X_test.columns.tolist(),
    plot_type="dot",    # "dot" shows both direction and magnitude
    max_display=25,
    show=False
)
plt.title("SHAP Summary — Home Win Probability")
plt.tight_layout()
plt.savefig("outputs/shap_summary_home_win.png", dpi=150, bbox_inches="tight")
```

**How to read:** Each dot is one match. Colour = feature value (red = high, blue = low). Position on x-axis = SHAP value (rightward = pushes toward home win). A cluster of red dots on the right means "high feature value increases home win probability."

---

### 8c. Feature Importance Bar Plot

Aggregated mean absolute SHAP value per feature — a clean importance ranking that is bias-free (unlike tree gain importance).

```python
shap.summary_plot(
    shap_values[2],
    X_test,
    feature_names=X_test.columns.tolist(),
    plot_type="bar",
    max_display=20,
    show=False
)
plt.title("Mean |SHAP| — Home Win (Feature Importance)")
plt.tight_layout()
plt.savefig("outputs/shap_bar_home_win.png", dpi=150, bbox_inches="tight")
```

---

### 8d. SHAP Dependence Plots

Shows the relationship between one feature's value and its SHAP contribution, revealing non-linear effects and interactions.

```python
# How does squad rating differential affect the home win probability?
shap.dependence_plot(
    "squad_rating_diff",
    shap_values[2],
    X_test,
    interaction_index="rank_diff",   # colour by a second feature to reveal interactions
    show=False
)
plt.title("SHAP Dependence — Squad Rating Differential")
plt.savefig("outputs/shap_dependence_rating_diff.png", dpi=150, bbox_inches="tight")
```

Run dependence plots for your top 5–10 features. Key ones to examine:

```python
key_features = [
    "squad_rating_diff",
    "win_rate_diff",
    "rank_diff",
    "h2h_home_win_rate",
    "days_rest",
    "stage_ordinal",
    "expected_total_goals",
    "top5_ratio_diff",
]
```

---

### 8e. Waterfall Plot — Single Match Explanation

For any individual match, show exactly which features pushed the prediction toward or away from a home win.

```python
def explain_match(fixture_idx, class_idx=2):
    """
    Explain a single match prediction.
    class_idx: 0=away_win, 1=draw, 2=home_win
    """
    expected_value = explainer.expected_value[class_idx]
    shap_vals      = shap_values[class_idx][fixture_idx]
    feature_vals   = X_test.iloc[fixture_idx]

    shap.waterfall_plot(
        shap.Explanation(
            values=shap_vals,
            base_values=expected_value,
            data=feature_vals,
            feature_names=X_test.columns.tolist()
        ),
        max_display=15,
        show=False
    )
    plt.title(f"Match {fixture_idx}: Home Win Probability Breakdown")
    plt.tight_layout()
    plt.savefig(f"outputs/shap_waterfall_match_{fixture_idx}.png",
                dpi=150, bbox_inches="tight")

# Example: explain the first World Cup final in the test set
final_idx = df_test[df_test["stage"] == "final"].index[0]
explain_match(final_idx, class_idx=2)
```

---

### 8f. Force Plot — Interactive Visualisation

```python
# Single match force plot
shap.initjs()
force_plot = shap.force_plot(
    explainer.expected_value[2],
    shap_values[2][0],
    X_test.iloc[0],
    feature_names=X_test.columns.tolist()
)
shap.save_html("outputs/shap_force_plot.html", force_plot)

# Multi-match force plot (stacked, interactive)
multi_force = shap.force_plot(
    explainer.expected_value[2],
    shap_values[2][:50],
    X_test.iloc[:50],
    feature_names=X_test.columns.tolist()
)
shap.save_html("outputs/shap_force_multi.html", multi_force)
```

---

### 8g. SHAP Interaction Values

Identify which pairs of features have the strongest interaction effects (i.e. where knowing both values together is more predictive than each alone).

```python
# Computationally expensive — run on a sample
shap_interaction = explainer.shap_interaction_values(X_test.iloc[:200])

# Mean absolute interaction strength matrix
mean_interactions = np.abs(shap_interaction[2]).mean(axis=0)
interaction_df = pd.DataFrame(
    mean_interactions,
    index=X_test.columns,
    columns=X_test.columns
)

plt.figure(figsize=(14, 12))
sns.heatmap(interaction_df, cmap="YlOrRd", square=True, linewidths=0.3)
plt.title("SHAP Interaction Values — Home Win Class")
plt.tight_layout()
plt.savefig("outputs/shap_interactions.png", dpi=150, bbox_inches="tight")
```

---

### 8h. SHAP-Based Feature Selection

Use mean absolute SHAP values as a principled feature importance measure to inform final feature selection.

```python
# Compute mean |SHAP| per feature, averaged across all classes
mean_abs_shap = np.mean([
    np.abs(shap_values[c]).mean(axis=0) for c in range(3)
], axis=0)

shap_importance = pd.DataFrame({
    "feature":    X_test.columns,
    "mean_shap":  mean_abs_shap,
}).sort_values("mean_shap", ascending=False)

# Drop features with negligible SHAP contribution
SHAP_THRESHOLD = 0.005
weak_features = shap_importance[shap_importance["mean_shap"] < SHAP_THRESHOLD]["feature"].tolist()
print(f"Features to consider dropping (low SHAP): {weak_features}")
```

---

## Model Comparison & Selection

### Comparison Table

After running all models through the CV loop, produce a unified comparison:

```python
comparison = pd.DataFrame({
    "Model": model_names,
    "CV Accuracy (mean)":  [r["accuracy"] for r in cv_results],
    "CV Accuracy (std)":   [r["accuracy_std"] for r in cv_results],
    "CV Log Loss (mean)":  [r["log_loss"] for r in cv_results],
    "CV Log Loss (std)":   [r["log_loss_std"] for r in cv_results],
    "CV Brier (mean)":     [r["brier"] for r in cv_results],
    "Test Accuracy":       [r["test_acc"] for r in cv_results],
    "Test Log Loss":       [r["test_ll"] for r in cv_results],
    "Training Time (s)":   [r["train_time"] for r in cv_results],
})
comparison = comparison.sort_values("CV Log Loss (mean)")
comparison.to_csv("outputs/model_comparison.csv", index=False)
print(comparison.to_string())
```

### Selection Criteria

Select the final model based on this priority order:

1. **CV Log Loss** — primary metric; most sensitive to probability quality
2. **CV Accuracy** — secondary, ensures ranking quality
3. **Test set performance** — confirms generalisation; should be close to CV
4. **Calibration quality** — predictions must be trustworthy probabilities
5. **Training stability** — low std across folds means robust learning
6. **Inference speed** — matters if predictions are needed in real time
7. **Explainability** — prefer interpretable models when performance is equivalent

### Overfitting Diagnostics

```python
for model_name, model in models.items():
    train_ll = log_loss(y_train, model.predict_proba(X_train))
    test_ll  = log_loss(y_test,  model.predict_proba(X_test))
    gap = test_ll - train_ll
    flag = "⚠️  OVERFIT" if gap > 0.15 else "✅"
    print(f"{model_name:20s} | Train LL: {train_ll:.4f} | Test LL: {test_ll:.4f} | Gap: {gap:.4f} {flag}")
```

---

## Production Readiness Checks

Before deploying the model to predict a real tournament, run the following checklist.

### Checklist

```python
checks = {
    "No temporal leakage in train/test split":      True,   # Verified in Step 12 of feature guide
    "Model calibrated on held-out data":            calibrated_model is not None,
    "All features available at inference time":     True,   # No post-match fields
    "Missing value handling implemented":           True,   # Defaults defined
    "Scaler fitted on train only, saved to disk":   True,
    "SHAP explainer saved to disk":                 True,
    "Test performance close to CV performance":     abs(test_ll - cv_ll_mean) < 0.05,
    "Predictions output as probabilities not labels": True,
    "Model version logged":                         True,
}

for check, passed in checks.items():
    status = "✅" if passed else "❌"
    print(f"{status}  {check}")
```

### Save Final Model Artefacts

```python
import joblib

joblib.dump(final_model,        "artefacts/model_final.pkl")
joblib.dump(calibrated_model,   "artefacts/model_calibrated.pkl")
joblib.dump(scaler,             "artefacts/scaler.pkl")
joblib.dump(explainer,          "artefacts/shap_explainer.pkl")
joblib.dump(selected_features,  "artefacts/selected_features.pkl")

shap_importance.to_csv("artefacts/shap_feature_importance.csv", index=False)
comparison.to_csv("artefacts/model_comparison.csv", index=False)

print("✅  All artefacts saved to /artefacts/")
```

### Inference Template

A clean function to call the Poisson models on a single future match:

```python
def predict_match(home_team_id, away_team_id, league_id, match_date,
                  feature_store, model_home, model_away, scaler, selected_features,
                  rho=0.1, max_goals=8):
    """
    Predict scoreline probabilities for a single upcoming match.

    Returns dict with:
      - lambda_home, lambda_away: expected goals per team
      - scoreline_matrix: full probability grid
      - most_likely_score: (home_goals, away_goals)
      - home_win, draw, away_win: outcome probabilities (summing to 1)
    """
    # 1. Build feature row from pre-computed feature store
    row = feature_store.get_match_features(
        home_team_id, away_team_id, league_id, match_date
    )

    # 2. Select and order features
    X = pd.DataFrame([row])[selected_features]

    # 3. Handle missing values
    X = X.fillna(X.median())

    # 4. Scale (if using linear Poisson; skip for XGBoost Poisson)
    X_input = scaler.transform(X) if scaler else X

    # 5. Predict expected goals
    lambda_home = float(model_home.predict(X_input)[0])
    lambda_away = float(model_away.predict(X_input)[0])

    # 6. Build scoreline matrix (with bivariate correction)
    matrix = bivariate_poisson_matrix(lambda_home, lambda_away, rho, max_goals)

    # 7. Derive outputs
    p_home, p_draw, p_away = matrix_to_outcome_probs(matrix)
    best_h, best_a, best_p = most_likely_scoreline(matrix)

    return {
        "lambda_home":       round(lambda_home, 3),
        "lambda_away":       round(lambda_away, 3),
        "most_likely_score": f"{best_h}-{best_a}",
        "score_probability": round(float(best_p), 4),
        "home_win":          round(float(p_home), 4),
        "draw":              round(float(p_draw), 4),
        "away_win":          round(float(p_away), 4),
    }

# Example
result = predict_match(
    home_team_id=2,       # France
    away_team_id=26,      # Argentina
    league_id=1,          # World Cup
    match_date=pd.Timestamp("2026-07-19"),
    feature_store=feature_store,
    model_home=poisson_home,
    model_away=poisson_away,
    scaler=scaler,
    selected_features=selected_features
)
print(result)
# → {'lambda_home': 1.42, 'lambda_away': 1.18, 'most_likely_score': '1-1',
#     'score_probability': 0.1283, 'home_win': 0.420, 'draw': 0.268, 'away_win': 0.312}
```

---

## Quick Reference

### Metric Targets (Rough Benchmarks for International Football)

#### Goal Prediction (Primary — Poisson Models)

| Metric | Naive Baseline | Good Model | Excellent Model |
|---|---|---|---|
| MAE (goals/team) | ~1.2 | ~0.95 | ~0.85 |
| Exact scoreline accuracy | ~10% | ~18% | ~25% |
| Ranked Probability Score | ~0.24 | ~0.20 | ~0.17 |

#### Derived Outcome Probabilities

| Metric | Naive Baseline | Good Model | Excellent Model |
|---|---|---|---|
| Accuracy (W/D/L) | ~45% | ~52% | ~57% |
| Log Loss | ~1.05 | ~0.95 | ~0.88 |
| Brier Score | ~0.24 | ~0.21 | ~0.19 |

### Model Selection Summary

| Model | Role | Strengths |
|---|---|---|
| **XGBoost Poisson (home/away)** | **Primary production model** | Non-linear goal prediction, scoreline matrix, tournament simulation |
| Poisson Regression (linear) | Interpretable baseline | Fast, principled, good calibration check |
| XGBoost Classifier | Secondary / ensemble | Direct W/D/L, strong accuracy |
| LightGBM Classifier | Secondary / ensemble | Fast tuning, comparison |
| Logistic Regression | Sanity check | Interpretable, fast |
| Voting Ensemble | Production stability | Reduced variance across Poisson + classifier |

### Output Files

```
outputs/
├── confusion_matrix.png
├── calibration_curves.png
├── rfecv_curve.png
├── correlation_heatmap.png
├── xgb_importance.png
├── shap_summary_home_win.png
├── shap_bar_home_win.png
├── shap_dependence_*.png
├── shap_interactions.png
├── shap_force_plot.html
├── shap_force_multi.html
└── model_comparison.csv

artefacts/
├── model_final.pkl
├── model_calibrated.pkl
├── scaler.pkl
├── shap_explainer.pkl
├── selected_features.pkl
├── shap_feature_importance.csv
└── model_comparison.csv
```
