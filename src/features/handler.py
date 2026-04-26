"""AWS Lambda entrypoint for the feature-engineering pipeline.

Contract
--------

The handler runs the five Phase-2 feature computations end-to-end against
an S3 bucket. It expects the following inputs to already exist in the bucket
(written by the ingestion side of the pipeline, eventually):

    processed/all_fixtures.csv
    processed/all_fixtures_club.csv      (optional — only if club pipeline requested)
    processed/players.csv
    processed/h2h_raw.csv
    processed/events.csv                 (optional — graceful degradation)
    external/fifa_rankings.csv           (optional)
    external/elo_ratings.csv             (optional)

Outputs (always written):

    processed/features_rolling.csv  + .parquet
    processed/features_squad.csv    + .parquet
    processed/features_h2h.csv      + .parquet
    processed/features_tournament.csv + .parquet
    processed/training_table.csv    + .parquet

Event shape:
    {"domain": "national" | "club" | "both"}   # default: "national"

The storage backend is selected by the ``DATA_BUCKET`` environment variable;
when unset, all I/O goes to local disk — so the same handler body runs in
pytest, in a dev shell, and in Lambda.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from src.features import io
from src.features.build import (
    build_club_inference_table,
    build_club_training_table,
    build_inference_table,
    build_training_table,
)
from src.features.h2h import compute_h2h_features
from src.features.rebuild import rebuild_fixtures_csv
from src.features.rolling import compute_rolling_features
from src.features.squad import compute_squad_features
from src.features.tournament import compute_tournament_features

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _also_parquet(csv_key: str) -> None:
    """Re-read the CSV we just wrote and emit the same dataframe as Parquet."""
    df = io.read_csv(csv_key)
    parquet_key = csv_key.replace(".csv", ".parquet")
    io.write_parquet(parquet_key, df)
    logger.info("Mirrored %s → %s (%d rows)", csv_key, parquet_key, len(df))


def _run_national() -> dict[str, int]:
    counts: dict[str, int] = {}

    logger.info("=== Step 0 — rebuild all_fixtures.csv from raw JSONs ===")
    counts["all_fixtures"] = rebuild_fixtures_csv("national")

    logger.info("=== Step 2.1 — rolling features ===")
    rolling = compute_rolling_features()
    counts["rolling"] = len(rolling)
    _also_parquet("data/processed/features_rolling.csv")

    logger.info("=== Step 2.2 — squad features ===")
    squad = compute_squad_features()
    counts["squad"] = len(squad)
    _also_parquet("data/processed/features_squad.csv")

    logger.info("=== Step 2.3 — H2H features ===")
    h2h = compute_h2h_features()
    counts["h2h"] = len(h2h)
    _also_parquet("data/processed/features_h2h.csv")

    logger.info("=== Step 2.4 — tournament features ===")
    tourn = compute_tournament_features()
    counts["tournament"] = len(tourn)
    _also_parquet("data/processed/features_tournament.csv")

    logger.info("=== Step 2.7 — training table ===")
    table = build_training_table()
    counts["training_table"] = len(table)
    _also_parquet("data/processed/training_table.csv")

    logger.info("=== Inference table — upcoming fixtures ===")
    inference = build_inference_table()
    counts["inference_table"] = len(inference)
    _also_parquet("data/processed/inference_table.csv")

    return counts


def _run_club() -> dict[str, int]:
    """Club pipeline — skips tournament/Elo/FIFA, writes *_club outputs."""
    counts: dict[str, int] = {}

    logger.info("=== Club: rebuild all_fixtures_club.csv from raw JSONs ===")
    counts["all_fixtures_club"] = rebuild_fixtures_csv("club")

    logger.info("=== Club: rolling features ===")
    rolling = compute_rolling_features(
        fixtures_path="data/processed/all_fixtures_club.csv",
        output_path="data/processed/features_rolling_club.csv",
    )
    counts["rolling_club"] = len(rolling)
    _also_parquet("data/processed/features_rolling_club.csv")

    logger.info("=== Club: squad features ===")
    squad = compute_squad_features(
        players_path="data/processed/players_club.csv",
        output_path="data/processed/features_squad_club.csv",
    )
    counts["squad_club"] = len(squad)
    _also_parquet("data/processed/features_squad_club.csv")

    logger.info("=== Club: H2H features ===")
    h2h = compute_h2h_features(
        fixtures_path="data/processed/all_fixtures_club.csv",
        h2h_path="data/processed/h2h_raw_club.csv",
        output_path="data/processed/features_h2h_club.csv",
    )
    counts["h2h_club"] = len(h2h)
    _also_parquet("data/processed/features_h2h_club.csv")

    logger.info("=== Club: training table ===")
    table = build_club_training_table()
    counts["training_table_club"] = len(table)
    _also_parquet("data/processed/training_table_club.csv")

    logger.info("=== Club: inference table ===")
    inference = build_club_inference_table()
    counts["inference_table_club"] = len(inference)
    _also_parquet("data/processed/inference_table_club.csv")

    return counts


def handler(event: dict[str, Any] | None = None, context: object | None = None) -> dict[str, Any]:
    """Lambda entrypoint. Returns a JSON-serializable summary dict."""
    started = time.time()
    domain = (event or {}).get("domain", "national")
    backend = "s3" if io.using_s3() else "local"
    logger.info("Feature pipeline start — domain=%s backend=%s", domain, backend)

    counts: dict[str, int] = {}
    if domain in ("national", "both"):
        counts.update(_run_national())
    if domain in ("club", "both"):
        counts.update(_run_club())

    elapsed = round(time.time() - started, 2)
    logger.info("Feature pipeline done — elapsed=%ss counts=%s", elapsed, counts)
    return {
        "status": "ok",
        "domain": domain,
        "backend": backend,
        "elapsed_seconds": elapsed,
        "row_counts": counts,
    }


if __name__ == "__main__":
    # CLI smoke-test: `uv run python -m src.features.handler`
    import json

    print(json.dumps(handler(), indent=2, default=str))
