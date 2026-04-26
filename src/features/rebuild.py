"""Rebuild ``processed/all_fixtures{,_club}.csv`` from raw fixture JSONs.

Runs as the first step of the daily feature pipeline so downstream
feature builders always see today's status updates (NS → FT). Without
this step, fixtures stayed pinned at whatever status they had when
they were first seen, even though the daily ingest writes fresh
snapshots.

I/O goes through ``src.features.io`` so the same code runs locally
(``DATA_BUCKET`` unset) and in Lambda. Local layout:
``data/raw/{domain}/fixtures/<run-date>/league=*-season=*.json``;
S3 layout: ``{domain}/fixtures/<run-date>/league=*-season=*.json``.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.features import io

logger = logging.getLogger(__name__)


_NATIONAL_LEAGUE_IDS = {1, 4, 5, 6, 7, 9, 10, 11}

_OUTPUT_BY_DOMAIN = {
    "national": "data/processed/all_fixtures.csv",
    "club": "data/processed/all_fixtures_club.csv",
}


def _parse_stage(round_str: str | None) -> str:
    if not round_str:
        return "unknown"
    r = round_str.lower()
    if "3rd" in r or "third" in r:
        return "third_place"
    if "semi" in r:
        return "semifinal"
    if "quarter" in r:
        return "quarterfinal"
    if "16" in r or "8th" in r:
        return "round_of_16"
    if "final" in r:
        return "final"
    if "group" in r:
        return "group"
    if "qualifying" in r or "qualif" in r:
        return "qualifying"
    return "group"


def _derive_outcome(home_goals: int | None, away_goals: int | None) -> str | None:
    if home_goals is None or away_goals is None:
        return None
    if home_goals > away_goals:
        return "home_win"
    if away_goals > home_goals:
        return "away_win"
    return "draw"


def _row_from_fixture_item(item: dict[str, Any]) -> dict[str, Any]:
    fixture = item.get("fixture", {})
    league = item.get("league", {})
    teams = item.get("teams", {})
    goals = item.get("goals", {}) or {}
    score = item.get("score", {}) or {}
    ht = score.get("halftime") or {}
    status = (fixture.get("status") or {}).get("short")
    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "league_id": league.get("id"),
        "season": league.get("season"),
        "round": league.get("round"),
        "stage": _parse_stage(league.get("round")),
        "home_team_id": (teams.get("home") or {}).get("id"),
        "home_team_name": (teams.get("home") or {}).get("name"),
        "away_team_id": (teams.get("away") or {}).get("id"),
        "away_team_name": (teams.get("away") or {}).get("name"),
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away"),
        "home_goals_ht": ht.get("home"),
        "away_goals_ht": ht.get("away"),
        "outcome": _derive_outcome(goals.get("home"), goals.get("away")),
        "status": status,
    }


def _list_fixture_keys(domain: str) -> list[str]:
    if io.using_s3():
        prefix = f"{domain}/fixtures/"
    else:
        prefix = f"data/raw/{domain}/fixtures"
    return [k for k in io.list_keys(prefix) if k.endswith(".json")]


def _run_date_from_key(key: str) -> str:
    parts = key.replace("\\", "/").split("/")
    try:
        idx = parts.index("fixtures") + 1
        return parts[idx] if idx < len(parts) else ""
    except ValueError:
        return ""


def rebuild_fixtures_csv(domain: str) -> int:
    """Rebuild ``all_fixtures{,_club}.csv`` from raw JSONs. Returns row count."""
    keys = _list_fixture_keys(domain)
    if not keys:
        logger.warning("[%s] no fixture JSONs found — skipping rebuild", domain)
        return 0

    rows: list[dict[str, Any]] = []
    for key in keys:
        run_date = _run_date_from_key(key)
        try:
            payload = io.read_json(key)
        except Exception as exc:  # noqa: BLE001
            logger.warning("  skipping unreadable %s: %s", key, exc)
            continue
        for item in payload.get("response", []) or []:
            row = _row_from_fixture_item(item)
            row["_run_date"] = run_date
            rows.append(row)

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["fixture_id"])
    if df.empty:
        logger.warning("[%s] no fixture rows extracted — skipping write", domain)
        return 0
    df["fixture_id"] = df["fixture_id"].astype(int)
    df = df.sort_values("_run_date").drop_duplicates(subset=["fixture_id"], keep="last")
    df = df.drop(columns=["_run_date"])
    df = df.sort_values("date").reset_index(drop=True)

    if domain == "national":
        df = df[df["league_id"].isin(_NATIONAL_LEAGUE_IDS)].reset_index(drop=True)

    output_key = _OUTPUT_BY_DOMAIN[domain]
    io.write_csv(output_key, df)

    statuses = df["status"].value_counts(dropna=False).to_dict()
    logger.info(
        "[%s] rebuilt %s — %d fixtures from %d source files; statuses=%s",
        domain, output_key, len(df), len(keys), statuses,
    )
    return len(df)
