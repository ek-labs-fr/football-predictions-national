"""Batch fixture prediction core: upcoming + recent + holdout, per competition.

For each configured competition (WC 2026, Premier League, La Liga, Ligue 1):
    * upcoming  — predictions on the inference table for league_id
    * recent    — predictions for FT fixtures played in the last 30 days,
                  with actuals merged in. Predictions are frozen on
                  first write (predictions/<fid>.json) so retrains
                  don't retro-rewrite history.
    * holdout   — predictions on the training-table holdout (with actuals).
                  NOT frozen: re-runs every time, so model improvements
                  show up on the holdout accuracy summary.

Outputs go to S3 (when DATA_BUCKET is set) under:
    web/data/competitions.json
    web/data/upcoming_<competition>.json
    web/data/recent_<competition>.json
    web/data/past_<competition>.json
    predictions/<fixture_id>.json   ← immutable per-fixture frozen predictions

PUBLIC-DATA CONSTRAINT: anything written under web/data/* is served publicly
via Cloudflare Pages (ericfc.com) — the S3 bucket policy grants public-read on
that prefix only. Only include fields that are safe to expose to anyone on the
internet. Sensitive fields (raw API tokens, PII, internal-only metrics) must
go under a different prefix — predictions/<fid>.json and the rest of the bucket
remain private.

The legacy combined CSV/Parquet files (predictions_{national_wc2026,club})
are still emitted for debugging.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.features import io
from src.inference.rationale import render_rationale
from src.models.calibrate import RhoConfig, _bivariate_poisson_matrix, load_rho_config
from src.models.train import _make_holdout_masks, get_feature_columns

logger = logging.getLogger(__name__)


_RECENT_WINDOW_DAYS = 30
_FINISHED_STATUSES = {"FT", "AET", "PEN"}
_PREDICTIONS_PREFIX = "predictions"

# Bump when the inference decision rule changes. Stamped on every frozen
# prediction so we can later distinguish predictions made under different
# rules (e.g. argmax_v0 → some_future_rule_v1).
_DECISION_RULE_VERSION = "argmax_v0"

# Supported decision rules. argmax_v0 picks the joint-argmax cell of the
# bivariate Poisson matrix. outcome_conditional_v0 picks the marginal-argmax
# outcome (W/D/L) first, then the modal cell inside that outcome's region.
_DECISION_RULES = ("argmax_v0", "outcome_conditional_v0")


# ------------------------------------------------------------------
# Mode + competition registry
# ------------------------------------------------------------------


@dataclass(frozen=True)
class ModeConfig:
    training_table: str
    inference_table: str
    artefacts_prefix: str
    legacy_csv: str
    legacy_parquet: str


MODES: dict[str, ModeConfig] = {
    "national": ModeConfig(
        training_table="data/processed/training_table.csv",
        inference_table="data/processed/inference_table.parquet",
        artefacts_prefix="artefacts",
        legacy_csv="outputs/predictions_national_wc2026.csv",
        legacy_parquet="outputs/predictions_national_wc2026.parquet",
    ),
    "club": ModeConfig(
        training_table="data/processed/training_table_club.csv",
        inference_table="data/processed/inference_table_club.parquet",
        artefacts_prefix="artefacts/club",
        legacy_csv="outputs/predictions_club.csv",
        legacy_parquet="outputs/predictions_club.parquet",
    ),
}


@dataclass(frozen=True)
class Competition:
    id: str
    name: str
    mode: str
    league_id: int
    past_label: str
    # Cap on how many upcoming fixtures the UI should render. None = no cap
    # (the UI's default time-window filter applies). Set for tournaments
    # whose fixtures cluster outside the default 7-day window.
    upcoming_display_limit: int | None = None


COMPETITIONS: list[Competition] = [
    Competition(
        "wc-2026",
        "FIFA World Cup 2026",
        "national",
        1,
        "Holdout: World Cup 2022",
        upcoming_display_limit=12,
    ),
    Competition("premier-league", "Premier League", "club", 39, "Holdout: 2024–25 season"),
    Competition("la-liga", "La Liga", "club", 140, "Holdout: 2024–25 season"),
    Competition("ligue-1", "Ligue 1", "club", 61, "Holdout: 2024–25 season"),
]


# ------------------------------------------------------------------
# Artefact loading
# ------------------------------------------------------------------


def _load_pickle(key: str) -> object:
    return joblib.load(BytesIO(io.read_bytes(key)))


def _load_artefacts(
    prefix: str,
) -> tuple[object, object, object | None, RhoConfig, str | None]:
    model_home = _load_pickle(f"{prefix}/model_final_home.pkl")
    model_away = _load_pickle(f"{prefix}/model_final_away.pkl")
    scaler_key = f"{prefix}/model_final_scaler.pkl"
    scaler = _load_pickle(scaler_key) if io.exists(scaler_key) else None
    rho_config = load_rho_config(io.read_json(f"{prefix}/rho.json"))
    model_trained_at = io.last_modified(f"{prefix}/model_final_home.pkl")
    return model_home, model_away, scaler, rho_config, model_trained_at


# ------------------------------------------------------------------
# Prediction primitive
# ------------------------------------------------------------------


_OUTCOMES = ("home_win", "draw", "away_win")


def _modal_scoreline(
    mat: np.ndarray,
    p_h: float,
    p_d: float,
    p_a: float,
    rule: str,
) -> tuple[int, int]:
    """Pick a scoreline from the bivariate Poisson matrix under the named rule.

    argmax_v0: argmax over the full joint distribution.
    outcome_conditional_v0: argmax over only the cells consistent with the
    marginal-argmax outcome (lower triangle for home win, diagonal for draw,
    upper triangle for away win). Guarantees served scoreline agrees with
    served outcome.
    """
    if rule == "argmax_v0":
        target = mat
    elif rule == "outcome_conditional_v0":
        outcome_idx = int(np.argmax([p_h, p_d, p_a]))
        if outcome_idx == 0:  # home win → strictly lower triangle
            target = np.tril(mat, -1)
        elif outcome_idx == 1:  # draw → diagonal only
            target = np.diag(np.diag(mat))
        else:  # away win → strictly upper triangle
            target = np.triu(mat, 1)
    else:
        raise ValueError(f"unknown decision rule: {rule!r}")
    idx = np.unravel_index(target.argmax(), target.shape)
    return int(idx[0]), int(idx[1])


def _predict_rows(
    rows: pd.DataFrame,
    feature_cols: list[str],
    medians: pd.Series,
    model_home: object,
    model_away: object,
    scaler: object | None,
    rho_config: RhoConfig,
    decision_rule: str = _DECISION_RULE_VERSION,
) -> pd.DataFrame:
    if decision_rule not in _DECISION_RULES:
        raise ValueError(f"decision_rule must be one of {_DECISION_RULES}")
    missing = [c for c in feature_cols if c not in rows.columns]
    if missing:
        raise KeyError(f"missing feature columns: {missing[:5]}...")

    X = rows[feature_cols].fillna(medians)
    X_input = scaler.transform(X) if scaler is not None else X.values

    lh = np.clip(model_home.predict(X_input), 0.01, 10.0)
    la = np.clip(model_away.predict(X_input), 0.01, 10.0)

    league_ids = (
        rows["league_id"].astype("Int64").tolist()
        if "league_id" in rows.columns
        else [None] * len(rows)
    )

    scores: list[str] = []
    rhos: list[float] = []
    p_h: list[float] = []
    p_d: list[float] = []
    p_a: list[float] = []
    for h, a, lid in zip(lh, la, league_ids, strict=True):
        rho = rho_config.lookup(int(lid) if lid is not None and not pd.isna(lid) else None)
        rhos.append(rho)
        mat = _bivariate_poisson_matrix(h, a, rho)
        ph = float(np.tril(mat, -1).sum())
        pd_ = float(np.trace(mat))
        pa = float(np.triu(mat, 1).sum())
        sh, sa = _modal_scoreline(mat, ph, pd_, pa, decision_rule)
        scores.append(f"{sh}-{sa}")
        p_h.append(ph)
        p_d.append(pd_)
        p_a.append(pa)

    out = rows.copy()
    out["lambda_home"] = np.round(lh, 3)
    out["lambda_away"] = np.round(la, 3)
    out["predicted_score"] = scores
    out["p_home_win"] = np.round(p_h, 4)
    out["p_draw"] = np.round(p_d, 4)
    out["p_away_win"] = np.round(p_a, 4)
    out["rho_used"] = np.round(rhos, 4)
    probs = np.column_stack([p_h, p_d, p_a])
    out["predicted_outcome"] = [_OUTCOMES[i] for i in probs.argmax(axis=1)]
    return out


# ------------------------------------------------------------------
# Frozen predictions store — write-once per fixture
# ------------------------------------------------------------------


_PREDICTION_FIELDS = (
    "lambda_home",
    "lambda_away",
    "predicted_score",
    "p_home_win",
    "p_draw",
    "p_away_win",
    "predicted_outcome",
)


def _existing_prediction_fids() -> set[int]:
    fids: set[int] = set()
    for key in io.list_keys(f"{_PREDICTIONS_PREFIX}/"):
        if not key.endswith(".json"):
            continue
        try:
            fids.add(int(Path(key).stem))
        except ValueError:
            continue
    return fids


def _store_prediction(
    fid: int,
    prediction_row: pd.Series,
    backfill: bool,
    model_trained_at: str | None,
) -> dict:
    payload = {
        "fixture_id": fid,
        "prediction_made_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "backfill": bool(backfill),
        "decision_rule_version": _DECISION_RULE_VERSION,
        "model_trained_at": model_trained_at,
        **{f: _coerce_scalar(prediction_row[f]) for f in _PREDICTION_FIELDS},
    }
    io.write_json(f"{_PREDICTIONS_PREFIX}/{fid}.json", payload)
    return payload


def _load_prediction(fid: int) -> dict:
    return io.read_json(f"{_PREDICTIONS_PREFIX}/{fid}.json")


def _coerce_scalar(value: object) -> object:
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, AttributeError):
            return value
    return value


def _compute_rationales_for_rows(
    df: pd.DataFrame,
    feature_cols: list[str],
    medians: pd.Series,
    model_home: object,
    model_away: object,
    scaler: object | None,
) -> list[str]:
    """Per-row plain-English rationale based on linear-model coefficients.

    Returns one string per input row. Empty string if the model is not
    coefficient-based (e.g. a tree model) so the UI can hide the footer.
    Computed on the fly each run; not persisted in frozen prediction JSONs.
    """
    coef_home = getattr(model_home, "coef_", None)
    coef_away = getattr(model_away, "coef_", None)
    if (
        coef_home is None
        or coef_away is None
        or len(coef_home) != len(feature_cols)
        or len(coef_away) != len(feature_cols)
    ):
        return [""] * len(df)

    required = {"home_team_name", "away_team_name", "predicted_outcome"}
    if not required.issubset(df.columns):
        return [""] * len(df)

    x_raw = df[feature_cols].fillna(medians)
    x_input = scaler.transform(x_raw) if scaler is not None else x_raw.values
    raw_arr = x_raw.to_numpy()

    rationales: list[str] = []
    for i in range(len(df)):
        rationales.append(
            render_rationale(
                home_team=str(df["home_team_name"].iloc[i]),
                away_team=str(df["away_team_name"].iloc[i]),
                predicted_outcome=str(df["predicted_outcome"].iloc[i]),
                feature_cols=feature_cols,
                scaled_x=x_input[i],
                raw_x=raw_arr[i],
                coef_home=np.asarray(coef_home),
                coef_away=np.asarray(coef_away),
            )
        )
    return rationales


def _materialise_predictions(
    rows: pd.DataFrame,
    feature_cols: list[str],
    medians: pd.Series,
    model_home: object,
    model_away: object,
    scaler: object | None,
    rho_config: RhoConfig,
    backfill: bool,
    model_trained_at: str | None,
) -> pd.DataFrame:
    """For every fixture in ``rows``: load its frozen prediction or create one.

    Returns a DataFrame with the original ``rows`` columns plus the prediction
    fields, in the same order as the input.
    """
    if rows.empty:
        return rows.copy()

    existing = _existing_prediction_fids()
    fids = rows["fixture_id"].astype(int)
    needs_predict = ~fids.isin(existing)
    new_rows = rows[needs_predict.values].copy()

    if not new_rows.empty:
        predicted = _predict_rows(
            new_rows,
            feature_cols,
            medians,
            model_home,
            model_away,
            scaler,
            rho_config,
        )
        for _, p in predicted.iterrows():
            _store_prediction(
                int(p["fixture_id"]),
                p,
                backfill=backfill,
                model_trained_at=model_trained_at,
            )
        logger.info("Froze %d new predictions (backfill=%s)", len(predicted), backfill)

    payloads = [_load_prediction(int(fid)) for fid in fids]
    pred_df = pd.DataFrame(payloads)
    out = rows.reset_index(drop=True).join(
        pred_df.drop(columns=["fixture_id"]).reset_index(drop=True),
    )
    # Recompute rationale fresh each run — not persisted in the frozen store,
    # so retraining the model automatically refreshes the served text.
    out["rationale"] = _compute_rationales_for_rows(
        out,
        feature_cols,
        medians,
        model_home,
        model_away,
        scaler,
    )
    return out


# ------------------------------------------------------------------
# Mode-level: upcoming + recent + holdout DataFrames
# ------------------------------------------------------------------


def _ensure_train_df_dates(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df.sort_values("date").reset_index(drop=True)


def predict_upcoming(mode: str) -> pd.DataFrame:
    cfg = MODES[mode]
    train_df = io.read_csv(cfg.training_table)
    feature_cols = get_feature_columns(train_df, mode=mode)
    medians = train_df[feature_cols].median()

    model_home, model_away, scaler, rho_config, trained_at = _load_artefacts(
        cfg.artefacts_prefix
    )
    inf = io.read_parquet(cfg.inference_table)

    out = _materialise_predictions(
        inf,
        feature_cols,
        medians,
        model_home,
        model_away,
        scaler,
        rho_config,
        backfill=False,
        model_trained_at=trained_at,
    )
    out = out.sort_values("date").reset_index(drop=True)

    legacy = out[
        [
            c
            for c in [
                "fixture_id",
                "date",
                "league_id",
                "round",
                "home_team_name",
                "away_team_name",
                "lambda_home",
                "lambda_away",
                "predicted_score",
                "p_home_win",
                "p_draw",
                "p_away_win",
            ]
            if c in out.columns
        ]
    ].rename(columns={"predicted_score": "most_likely_score"})
    io.write_csv(cfg.legacy_csv, legacy)
    io.write_parquet(cfg.legacy_parquet, legacy)

    logger.info(
        "[%s] upcoming: %d rows (rho_default=%.4f, buckets=%s)",
        mode,
        len(out),
        rho_config.default,
        sorted(rho_config.by_bucket) if rho_config.by_bucket else "scalar",
    )
    return out


def predict_recent(mode: str, days: int = _RECENT_WINDOW_DAYS) -> pd.DataFrame:
    cfg = MODES[mode]
    train_df = _ensure_train_df_dates(io.read_csv(cfg.training_table))
    feature_cols = get_feature_columns(train_df, mode=mode)
    medians = train_df[feature_cols].median()

    cutoff = datetime.now(UTC) - timedelta(days=days)
    recent = train_df[
        (train_df["status"].isin(_FINISHED_STATUSES)) & (train_df["date"] >= pd.Timestamp(cutoff))
    ].copy()

    if recent.empty:
        logger.info("[%s] recent: no FT fixtures in last %d days", mode, days)
        return recent

    model_home, model_away, scaler, rho_config, trained_at = _load_artefacts(
        cfg.artefacts_prefix
    )
    out = _materialise_predictions(
        recent,
        feature_cols,
        medians,
        model_home,
        model_away,
        scaler,
        rho_config,
        backfill=True,
        model_trained_at=trained_at,
    )

    out["actual_home_goals"] = out["home_goals"].astype(int)
    out["actual_away_goals"] = out["away_goals"].astype(int)
    out["actual_score"] = (
        out["actual_home_goals"].astype(str) + "-" + out["actual_away_goals"].astype(str)
    )
    out["actual_outcome"] = np.where(
        out["actual_home_goals"] > out["actual_away_goals"],
        "home_win",
        np.where(
            out["actual_home_goals"] < out["actual_away_goals"],
            "away_win",
            "draw",
        ),
    )
    out["correct_outcome"] = out["predicted_outcome"] == out["actual_outcome"]
    out["correct_score"] = out["predicted_score"] == out["actual_score"]
    out = out.sort_values("date", ascending=False).reset_index(drop=True)

    logger.info("[%s] recent: %d FT fixtures in last %d days", mode, len(out), days)
    return out


def predict_holdout(mode: str, decision_rule: str = _DECISION_RULE_VERSION) -> pd.DataFrame:
    cfg = MODES[mode]
    train_df = _ensure_train_df_dates(io.read_csv(cfg.training_table))
    feature_cols = get_feature_columns(train_df, mode=mode)
    medians = train_df[feature_cols].median()

    _train_mask, test_mask = _make_holdout_masks(train_df, mode)
    holdout = train_df[test_mask].copy()

    model_home, model_away, scaler, rho_config, _trained_at = _load_artefacts(
        cfg.artefacts_prefix
    )
    out = _predict_rows(
        holdout,
        feature_cols,
        medians,
        model_home,
        model_away,
        scaler,
        rho_config,
        decision_rule=decision_rule,
    )
    out["rationale"] = _compute_rationales_for_rows(
        out,
        feature_cols,
        medians,
        model_home,
        model_away,
        scaler,
    )

    out["actual_home_goals"] = out["home_goals"].astype(int)
    out["actual_away_goals"] = out["away_goals"].astype(int)
    out["actual_score"] = (
        out["actual_home_goals"].astype(str) + "-" + out["actual_away_goals"].astype(str)
    )
    out["actual_outcome"] = np.where(
        out["actual_home_goals"] > out["actual_away_goals"],
        "home_win",
        np.where(
            out["actual_home_goals"] < out["actual_away_goals"],
            "away_win",
            "draw",
        ),
    )
    out["correct_outcome"] = out["predicted_outcome"] == out["actual_outcome"]
    out["correct_score"] = out["predicted_score"] == out["actual_score"]
    out = out.sort_values("date").reset_index(drop=True)

    logger.info("[%s] holdout: %d rows", mode, len(out))
    return out


# ------------------------------------------------------------------
# Per-competition splitting + JSON shape
# ------------------------------------------------------------------


_UPCOMING_COLS = [
    "fixture_id",
    "date",
    "round",
    "league_id",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "predicted_score",
    "lambda_home",
    "lambda_away",
    "p_home_win",
    "p_draw",
    "p_away_win",
    "predicted_outcome",
    "rationale",
    "prediction_made_at",
]

_PAST_EXTRA_COLS = [
    "actual_home_goals",
    "actual_away_goals",
    "actual_score",
    "actual_outcome",
    "correct_outcome",
    "correct_score",
]


def _to_records(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    cols = [c for c in columns if c in df.columns]
    sub = df[cols].copy()
    if "date" in sub.columns:
        sub["date"] = pd.to_datetime(sub["date"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    for c in sub.select_dtypes(include="bool").columns:
        sub[c] = sub[c].astype(bool)
    return sub.to_dict(orient="records")


def _performance(past: pd.DataFrame) -> dict[str, float | int]:
    if len(past) == 0:
        return {
            "total_matches": 0,
            "correct_outcomes": 0,
            "correct_scores": 0,
            "outcome_accuracy": 0.0,
            "score_accuracy": 0.0,
            "mae_avg": 0.0,
        }
    mae_home = float((past["actual_home_goals"] - past["lambda_home"]).abs().mean())
    mae_away = float((past["actual_away_goals"] - past["lambda_away"]).abs().mean())
    return {
        "total_matches": int(len(past)),
        "correct_outcomes": int(past["correct_outcome"].sum()),
        "correct_scores": int(past["correct_score"].sum()),
        "outcome_accuracy": round(float(past["correct_outcome"].mean()), 4),
        "score_accuracy": round(float(past["correct_score"].mean()), 4),
        "mae_avg": round((mae_home + mae_away) / 2, 4),
    }


def _filter_competition(df: pd.DataFrame, league_id: int) -> pd.DataFrame:
    if "league_id" not in df.columns:
        return df.iloc[0:0]
    return df[df["league_id"] == league_id].copy()


# ------------------------------------------------------------------
# Orchestrator
# ------------------------------------------------------------------


def publish_dashboard_json() -> dict[str, object]:
    """Run upcoming + recent + holdout for both modes, write per-competition JSON."""
    upcoming_by_mode = {m: predict_upcoming(m) for m in MODES}
    recent_by_mode = {m: predict_recent(m) for m in MODES}
    past_by_mode = {m: predict_holdout(m) for m in MODES}

    summary: dict[str, object] = {"competitions": []}

    manifest = []
    for comp in COMPETITIONS:
        upcoming_df = _filter_competition(upcoming_by_mode[comp.mode], comp.league_id)
        recent_df = _filter_competition(recent_by_mode[comp.mode], comp.league_id)
        past_df = _filter_competition(past_by_mode[comp.mode], comp.league_id)

        upcoming_payload = {
            "competition_id": comp.id,
            "competition_name": comp.name,
            "matches": _to_records(upcoming_df, _UPCOMING_COLS),
        }
        recent_payload = {
            "competition_id": comp.id,
            "competition_name": comp.name,
            "window_days": _RECENT_WINDOW_DAYS,
            "matches": _to_records(recent_df, _UPCOMING_COLS + _PAST_EXTRA_COLS),
            "performance": _performance(recent_df),
        }
        past_payload = {
            "competition_id": comp.id,
            "competition_name": comp.name,
            "label": comp.past_label,
            "matches": _to_records(past_df, _UPCOMING_COLS + _PAST_EXTRA_COLS),
            "performance": _performance(past_df),
        }

        io.write_json(f"web/data/upcoming_{comp.id}.json", upcoming_payload)
        io.write_json(f"web/data/recent_{comp.id}.json", recent_payload)
        io.write_json(f"web/data/past_{comp.id}.json", past_payload)

        manifest.append(
            {
                "id": comp.id,
                "name": comp.name,
                "mode": comp.mode,
                "league_id": comp.league_id,
                "past_label": comp.past_label,
                "recent_window_days": _RECENT_WINDOW_DAYS,
                "upcoming_count": len(upcoming_payload["matches"]),
                "upcoming_display_limit": comp.upcoming_display_limit,
                "recent_count": len(recent_payload["matches"]),
                "past_count": len(past_payload["matches"]),
            }
        )
        summary["competitions"].append(
            {
                "id": comp.id,
                "upcoming": len(upcoming_payload["matches"]),
                "recent": len(recent_payload["matches"]),
                "past": len(past_payload["matches"]),
                "recent_outcome_accuracy": recent_payload["performance"]["outcome_accuracy"],
                "holdout_outcome_accuracy": past_payload["performance"]["outcome_accuracy"],
            }
        )

    io.write_json("web/data/competitions.json", manifest)
    logger.info("Published dashboard JSON for %d competitions", len(manifest))
    return summary


# Backward-compat shim for the existing CLI script (predict_inference.py).
def predict_mode(mode: str) -> dict[str, object]:
    df = predict_upcoming(mode)
    return {"mode": mode, "rows": len(df)}


CONFIGS = MODES  # legacy alias
