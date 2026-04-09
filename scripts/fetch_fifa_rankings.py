"""Fetch and process historical FIFA world rankings.

Downloads ranking data from the cnc8/fifa-world-ranking GitHub repo,
maps country names to API-Football team IDs, and outputs
data/external/fifa_rankings.csv.

Usage:
    uv run python scripts/fetch_fifa_rankings.py
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

RANKINGS_URL = (
    "https://raw.githubusercontent.com/cnc8/fifa-world-ranking/master/fifa_ranking-2020-12-10.csv"
)
EXTERNAL_DIR = Path("data/external")
PROCESSED_DIR = Path("data/processed")

# Manual mapping of FIFA ranking country names to API-Football team names.
# Only need entries where the names differ between the two sources.
_NAME_MAP: dict[str, str] = {
    "USA": "USA",
    "Korea Republic": "South Korea",
    "Korea DPR": "North Korea",
    "IR Iran": "Iran",
    "Côte d'Ivoire": "Ivory Coast",
    "Cote d'Ivoire": "Ivory Coast",
    "China PR": "China",
    "Czech Republic": "Czech Republic",
    "Czechia": "Czech Republic",
    "Eswatini": "Swaziland",
    "Swaziland": "Swaziland",
    "Türkiye": "Turkey",
    "Cabo Verde": "Cape Verde",
    "Congo DR": "DR Congo",
    "Kyrgyz Republic": "Kyrgyzstan",
    "Timor-Leste": "Timor Leste",
    "Brunei Darussalam": "Brunei",
    "St Kitts and Nevis": "Saint Kitts And Nevis",
    "St Lucia": "Saint Lucia",
    "St Vincent and the Grenadines": "Saint Vincent And The Grenadines",
    "US Virgin Islands": "US Virgin Islands",
    "Curacao": "Curacao",
    "Curaçao": "Curacao",
    "Chinese Taipei": "Chinese Taipei",
    "Bosnia and Herzegovina": "Bosnia And Herzegovina",
    "Trinidad and Tobago": "Trinidad And Tobago",
    "Antigua and Barbuda": "Antigua And Barbuda",
    "São Tomé e Príncipe": "Sao Tome And Principe",
    "Turks and Caicos Islands": "Turks And Caicos Islands",
}


def download_rankings() -> pd.DataFrame:
    """Download FIFA ranking CSV from GitHub."""
    logger.info("Downloading FIFA rankings from %s", RANKINGS_URL)
    resp = requests.get(RANKINGS_URL, timeout=30)
    resp.raise_for_status()

    # Write raw file
    EXTERNAL_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = EXTERNAL_DIR / "fifa_ranking_raw.csv"
    raw_path.write_text(resp.text, encoding="utf-8")
    logger.info("Saved raw rankings to %s", raw_path)

    df = pd.read_csv(raw_path)
    logger.info("Loaded %d ranking rows (%d dates)", len(df), df["rank_date"].nunique())
    return df


def load_team_lookup() -> dict[str, int]:
    """Load the API-Football team name → ID mapping."""
    path = PROCESSED_DIR / "team_lookup.json"
    if not path.exists():
        logger.warning("team_lookup.json not found — run bootstrap_data.py first")
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def map_rankings(rankings: pd.DataFrame, team_lookup: dict[str, int]) -> pd.DataFrame:
    """Map FIFA ranking country names to API-Football team IDs."""

    def _resolve_team_id(country_name: str) -> int | None:
        # Try direct match first
        if country_name in team_lookup:
            return team_lookup[country_name]
        # Try the name map
        mapped = _NAME_MAP.get(country_name, country_name)
        if mapped in team_lookup:
            return team_lookup[mapped]
        # Try case-insensitive
        lower_lookup = {k.lower(): v for k, v in team_lookup.items()}
        if country_name.lower() in lower_lookup:
            return lower_lookup[country_name.lower()]
        return None

    rankings = rankings.copy()
    rankings["team_id"] = rankings["country_full"].apply(_resolve_team_id)

    matched = rankings["team_id"].notna().sum()
    total = len(rankings)
    unmatched_countries = rankings[rankings["team_id"].isna()]["country_full"].unique()
    logger.info(
        "Mapped %d/%d rows (%.1f%%), %d unmatched countries",
        matched,
        total,
        matched / total * 100,
        len(unmatched_countries),
    )
    if len(unmatched_countries) > 0 and len(unmatched_countries) <= 30:
        logger.info("Unmatched: %s", sorted(unmatched_countries))

    # Keep only matched rows
    result = rankings[rankings["team_id"].notna()].copy()
    result["team_id"] = result["team_id"].astype(int)
    result = result[["team_id", "country_full", "rank", "rank_date"]].rename(
        columns={"country_full": "team_name"}
    )
    result["rank_date"] = pd.to_datetime(result["rank_date"])
    return result.sort_values(["rank_date", "rank"]).reset_index(drop=True)


def main() -> None:
    rankings = download_rankings()
    team_lookup = load_team_lookup()

    if not team_lookup:
        logger.error("Cannot map rankings without team lookup — aborting")
        return

    mapped = map_rankings(rankings, team_lookup)

    output_path = EXTERNAL_DIR / "fifa_rankings.csv"
    mapped.to_csv(output_path, index=False)
    logger.info(
        "Saved %d mapped ranking rows to %s (date range: %s to %s)",
        len(mapped),
        output_path,
        mapped["rank_date"].min(),
        mapped["rank_date"].max(),
    )


if __name__ == "__main__":
    main()
