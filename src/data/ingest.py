"""Data ingestion functions for pulling national team data from API-Football v3."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.data.api_client import APIFootballClient  # noqa: TCH001
from src.data.schemas import (
    Fixture,
    FixtureEvent,
    FixtureStatistics,
    Injury,
    League,
    OddsResponse,
    Player,
    Team,
    TeamStatistics,
)

logger = logging.getLogger(__name__)

# Key international competition IDs
COMPETITION_IDS: dict[str, int] = {
    "World Cup": 1,
    "EURO": 4,
    "Nations League": 5,
    "AFCON": 6,
    "Gold Cup": 7,
    "Copa America": 9,
    "Asian Cup": 10,
    "Friendlies": 11,
}

# Seasons to pull per competition
COMPETITION_SEASONS: dict[int, list[int]] = {
    1: [1990, 1994, 1998, 2002, 2006, 2010, 2014, 2018, 2022],  # World Cup
    4: [2000, 2004, 2008, 2012, 2016, 2020, 2024],  # EURO
    5: [2018, 2020, 2022, 2024],  # Nations League
    6: [2002, 2004, 2006, 2008, 2010, 2012, 2013, 2015, 2017, 2019, 2021, 2023],  # AFCON
    7: [2002, 2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023],  # Gold Cup
    9: [2001, 2004, 2007, 2011, 2015, 2016, 2019, 2021, 2024],  # Copa America
    10: [2004, 2007, 2011, 2015, 2019, 2023],  # Asian Cup
    11: list(range(2010, 2026)),  # Friendlies 2010–2025
}

PROCESSED_DIR = Path("data/processed")
RAW_DIR = Path("data/raw")

# Completed fixture status codes
COMPLETED_STATUSES = {"FT", "AET", "PEN"}


# ------------------------------------------------------------------
# Step 1.4 — Reference Data Pull
# ------------------------------------------------------------------


def fetch_international_leagues(client: APIFootballClient) -> list[League]:
    """Fetch all international competitions from the API."""
    leagues: list[League] = []
    for qtype in ("Cup", "League"):
        data = client.get("/leagues", {"type": qtype, "country": "World"})
        for item in data.get("response", []):
            leagues.append(League.model_validate(item))
    logger.info("Fetched %d international leagues", len(leagues))
    return leagues


def fetch_national_teams(client: APIFootballClient) -> list[Team]:
    """Discover national teams by querying known competition fixtures."""
    seen_ids: set[int] = set()
    teams: list[Team] = []
    # Query teams from major tournaments to discover all nationals
    for league_id in (1, 4, 5, 6, 7, 9, 10):
        data = client.get("/teams", {"league": league_id, "season": 2022})
        for item in data.get("response", []):
            team = Team.model_validate(item)
            if team.team.id not in seen_ids:
                seen_ids.add(team.team.id)
                teams.append(team)
    logger.info("Fetched %d unique national teams", len(teams))
    return teams


def save_leagues(leagues: list[League], output_dir: Path = RAW_DIR) -> None:
    """Save leagues to JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "leagues.json"
    path.write_text(
        json.dumps([lg.model_dump() for lg in leagues], indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("Saved %d leagues to %s", len(leagues), path)


def save_teams(teams: list[Team], output_dir: Path = RAW_DIR) -> None:
    """Save teams to JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "teams.json"
    path.write_text(
        json.dumps([t.model_dump() for t in teams], indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("Saved %d teams to %s", len(teams), path)


def build_team_lookup(teams: list[Team], output_dir: Path = PROCESSED_DIR) -> dict[str, int]:
    """Build and save team_name -> team_id mapping."""
    output_dir.mkdir(parents=True, exist_ok=True)
    lookup = {t.team.name: t.team.id for t in teams}
    path = output_dir / "team_lookup.json"
    path.write_text(json.dumps(lookup, indent=2), encoding="utf-8")
    logger.info("Saved team lookup (%d entries) to %s", len(lookup), path)
    return lookup


# ------------------------------------------------------------------
# Step 1.5 — Historical Fixtures Pull
# ------------------------------------------------------------------


def fetch_fixtures(client: APIFootballClient, league_id: int, season: int) -> list[Fixture]:
    """Fetch all fixtures for a given competition and season."""
    data = client.get("/fixtures", {"league": league_id, "season": season})
    fixtures = [Fixture.model_validate(item) for item in data.get("response", [])]
    logger.info("Fetched %d fixtures for league=%d season=%d", len(fixtures), league_id, season)
    return fixtures


def _parse_stage(round_str: str | None) -> str:
    """Derive a normalised stage from the round string."""
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
    """Derive match outcome from goals."""
    if home_goals is None or away_goals is None:
        return None
    if home_goals > away_goals:
        return "home_win"
    if away_goals > home_goals:
        return "away_win"
    return "draw"


def fixtures_to_dataframe(fixtures: list[Fixture]) -> pd.DataFrame:
    """Convert a list of Fixture models to a flat DataFrame."""
    rows: list[dict[str, Any]] = []
    for f in fixtures:
        ht = f.score.halftime if f.score else None
        rows.append(
            {
                "fixture_id": f.fixture.id,
                "date": f.fixture.date,
                "league_id": f.league.id,
                "season": f.league.season,
                "round": f.league.round,
                "stage": _parse_stage(f.league.round),
                "home_team_id": f.teams.home.id,
                "home_team_name": f.teams.home.name,
                "away_team_id": f.teams.away.id,
                "away_team_name": f.teams.away.name,
                "home_goals": f.goals.home,
                "away_goals": f.goals.away,
                "home_goals_ht": ht.home if ht else None,
                "away_goals_ht": ht.away if ht else None,
                "outcome": _derive_outcome(f.goals.home, f.goals.away),
                "status": None,
            }
        )
    return pd.DataFrame(rows)


def merge_all_fixtures(
    client: APIFootballClient,
    competitions: dict[int, list[int]] | None = None,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pull all fixtures across competitions/seasons and merge into one DataFrame."""
    competitions = competitions or COMPETITION_SEASONS
    all_dfs: list[pd.DataFrame] = []

    for league_id, seasons in competitions.items():
        for season in seasons:
            fixtures = fetch_fixtures(client, league_id, season)
            if fixtures:
                df = fixtures_to_dataframe(fixtures)
                all_dfs.append(df)

    if not all_dfs:
        logger.warning("No fixtures pulled")
        return pd.DataFrame()

    merged = pd.concat(all_dfs, ignore_index=True)
    # Filter to completed matches (have goals)
    merged = merged.dropna(subset=["home_goals", "away_goals"])
    merged = merged.drop_duplicates(subset=["fixture_id"])

    # Filter to national teams only (exclude clubs, youth teams)
    lookup_path = output_dir / "team_lookup.json"
    if lookup_path.exists():
        national_ids = set(json.load(open(lookup_path, encoding="utf-8")).values())
        before = len(merged)
        merged = merged[
            merged["home_team_id"].isin(national_ids) & merged["away_team_id"].isin(national_ids)
        ]
        logger.info("Filtered to national teams: %d → %d fixtures", before, len(merged))

    merged = merged.sort_values("date").reset_index(drop=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "all_fixtures.csv"
    merged.to_csv(path, index=False)
    logger.info(
        "Saved %d fixtures to %s (date range: %s to %s)",
        len(merged),
        path,
        merged["date"].min(),
        merged["date"].max(),
    )
    return merged


# ------------------------------------------------------------------
# Step 1.6 — Team Statistics Pull
# ------------------------------------------------------------------


def fetch_team_statistics(
    client: APIFootballClient, league_id: int, season: int, team_id: int
) -> TeamStatistics | None:
    """Fetch aggregate team statistics for a given competition/season/team."""
    data = client.get("/teams/statistics", {"league": league_id, "season": season, "team": team_id})
    resp = data.get("response")
    if not resp:
        return None
    return TeamStatistics.model_validate(resp)


def _extract_team_stat_row(ts: TeamStatistics) -> dict[str, Any]:
    """Flatten a TeamStatistics model into a dict for CSV output."""
    fixtures = ts.fixtures
    played = fixtures.played.total if fixtures and fixtures.played else None
    wins = fixtures.wins.total if fixtures and fixtures.wins else None
    draws = fixtures.draws.total if fixtures and fixtures.draws else None
    losses = fixtures.loses.total if fixtures and fixtures.loses else None

    goals_for = ts.goals.get("for") if ts.goals else None
    goals_against = ts.goals.get("against") if ts.goals else None

    return {
        "team_id": ts.team.id if ts.team else None,
        "league_id": ts.league.id if ts.league else None,
        "season": ts.league.season if ts.league else None,
        "form": ts.form,
        "matches_played": played,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "goals_scored_total": goals_for.total.total if goals_for and goals_for.total else None,
        "goals_conceded_total": (
            goals_against.total.total if goals_against and goals_against.total else None
        ),
        "goals_scored_avg": goals_for.total.average if goals_for and goals_for.total else None,
        "goals_conceded_avg": (
            goals_against.total.average if goals_against and goals_against.total else None
        ),
        "clean_sheets": ts.clean_sheet.total if ts.clean_sheet else None,
        "failed_to_score": ts.failed_to_score.total if ts.failed_to_score else None,
    }


def pull_team_statistics(
    client: APIFootballClient,
    fixtures_df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pull team statistics for every unique (team, league, season) in the fixtures."""
    combos: set[tuple[int, int, int]] = set()
    for _, row in fixtures_df.iterrows():
        lid, season = int(row["league_id"]), int(row["season"])
        combos.add((int(row["home_team_id"]), lid, season))
        combos.add((int(row["away_team_id"]), lid, season))

    rows: list[dict[str, Any]] = []
    for team_id, league_id, season in sorted(combos):
        ts = fetch_team_statistics(client, league_id, season, team_id)
        if ts:
            rows.append(_extract_team_stat_row(ts))

    df = pd.DataFrame(rows)
    if not df.empty:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "team_statistics.csv"
        df.to_csv(path, index=False)
        logger.info("Saved %d team-season stats to %s", len(df), path)
    else:
        logger.warning("No team statistics pulled")
    return df


# ------------------------------------------------------------------
# Step 1.7 — Player & Squad Data Pull
# ------------------------------------------------------------------


def fetch_players(client: APIFootballClient, team_id: int, season: int) -> list[Player]:
    """Fetch all players for a team in a given season, handling pagination."""
    players: list[Player] = []
    page = 1
    while True:
        data = client.get("/players", {"team": team_id, "season": season, "page": page})
        for item in data.get("response", []):
            players.append(Player.model_validate(item))
        paging = data.get("paging", {})
        if page >= paging.get("total", 1):
            break
        page += 1
    return players


def _extract_player_row(player: Player, team_id: int, season: int) -> dict[str, Any]:
    """Flatten a Player model into a dict for CSV output."""
    stat = player.statistics[0] if player.statistics else None
    games = stat.games if stat else None
    goals = stat.goals if stat else None
    cards = stat.cards if stat else None
    return {
        "player_id": player.player.id,
        "player_name": player.player.name,
        "team_id": team_id,
        "season": season,
        "age": player.player.age,
        "nationality": player.player.nationality,
        "position": games.position if games else None,
        "club_league": stat.league.name if stat and stat.league else None,
        "appearances": games.appearences if games else None,
        "goals": goals.total if goals else None,
        "assists": goals.assists if goals else None,
        "yellow_cards": cards.yellow if cards else None,
        "red_cards": cards.red if cards else None,
        "rating": games.rating if games else None,
    }


def pull_players(
    client: APIFootballClient,
    fixtures_df: pd.DataFrame,
    min_year: int = 2006,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pull player data for all teams appearing in fixtures from min_year onward."""
    fixtures_df = fixtures_df.copy()
    fixtures_df["date"] = pd.to_datetime(fixtures_df["date"])
    recent = fixtures_df[fixtures_df["date"].dt.year >= min_year]

    combos: set[tuple[int, int]] = set()
    for _, row in recent.iterrows():
        season = int(row["season"])
        combos.add((int(row["home_team_id"]), season))
        combos.add((int(row["away_team_id"]), season))

    rows: list[dict[str, Any]] = []
    for team_id, season in sorted(combos):
        players = fetch_players(client, team_id, season)
        for p in players:
            rows.append(_extract_player_row(p, team_id, season))

    df = pd.DataFrame(rows)
    if not df.empty:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "players.csv"
        df.to_csv(path, index=False)
        logger.info("Saved %d player rows to %s", len(df), path)
    else:
        logger.warning("No player data pulled")
    return df


# ------------------------------------------------------------------
# Step 1.8 — Head-to-Head Data Pull
# ------------------------------------------------------------------


def fetch_head_to_head(
    client: APIFootballClient, team_a_id: int, team_b_id: int, last: int = 10
) -> list[Fixture]:
    """Fetch H2H fixtures between two teams."""
    h2h_str = f"{team_a_id}-{team_b_id}"
    data = client.get("/fixtures/headtohead", {"h2h": h2h_str, "last": last})
    return [Fixture.model_validate(item) for item in data.get("response", [])]


def pull_head_to_head(
    client: APIFootballClient,
    fixtures_df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pull H2H data for all unique team pairs in the fixture set."""
    pairs: set[tuple[int, int]] = set()
    for _, row in fixtures_df.iterrows():
        a, b = int(row["home_team_id"]), int(row["away_team_id"])
        pairs.add((min(a, b), max(a, b)))

    all_dfs: list[pd.DataFrame] = []
    for team_a, team_b in sorted(pairs):
        h2h_fixtures = fetch_head_to_head(client, team_a, team_b)
        if h2h_fixtures:
            df = fixtures_to_dataframe(h2h_fixtures)
            all_dfs.append(df)

    if not all_dfs:
        logger.warning("No H2H data pulled")
        return pd.DataFrame()

    merged = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["fixture_id"])
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "h2h_raw.csv"
    merged.to_csv(path, index=False)
    logger.info("Saved %d H2H fixture rows to %s (from %d pairs)", len(merged), path, len(pairs))
    return merged


# ------------------------------------------------------------------
# Step 1.9 — Match Events Pull
# ------------------------------------------------------------------


def fetch_events(client: APIFootballClient, fixture_id: int) -> list[FixtureEvent]:
    """Fetch in-match events for a single fixture."""
    data = client.get("/fixtures/events", {"fixture": fixture_id})
    return [FixtureEvent.model_validate(item) for item in data.get("response", [])]


def _aggregate_events(fixture_id: int, events: list[FixtureEvent]) -> list[dict[str, Any]]:
    """Aggregate events into per-team summary rows."""
    team_stats: dict[int, dict[str, int]] = {}
    for e in events:
        if not e.team or not e.team.id:
            continue
        tid = e.team.id
        if tid not in team_stats:
            team_stats[tid] = {
                "fixture_id": fixture_id,
                "team_id": tid,
                "yellow_cards": 0,
                "red_cards": 0,
                "goals": 0,
                "own_goals": 0,
                "penalties_scored": 0,
                "penalties_missed": 0,
            }
        s = team_stats[tid]
        detail = (e.detail or "").lower()
        etype = (e.type or "").lower()
        if etype == "card":
            if "yellow" in detail:
                s["yellow_cards"] += 1
            elif "red" in detail:
                s["red_cards"] += 1
        elif etype == "goal":
            if "own goal" in detail:
                s["own_goals"] += 1
            elif "penalty" in detail:
                s["penalties_scored"] += 1
            else:
                s["goals"] += 1
        elif "missed penalty" in detail or (etype == "goal" and "missed" in detail):
            s["penalties_missed"] += 1

    return list(team_stats.values())


def pull_events(
    client: APIFootballClient,
    fixtures_df: pd.DataFrame,
    min_year: int = 2006,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pull match events for all fixtures from min_year onward."""
    fixtures_df = fixtures_df.copy()
    fixtures_df["date"] = pd.to_datetime(fixtures_df["date"])
    recent = fixtures_df[fixtures_df["date"].dt.year >= min_year]

    rows: list[dict[str, Any]] = []
    fixture_ids = recent["fixture_id"].unique()
    for i, fid in enumerate(fixture_ids):
        events = fetch_events(client, int(fid))
        if events:
            rows.extend(_aggregate_events(int(fid), events))
        if (i + 1) % 100 == 0:
            logger.info("Events progress: %d/%d fixtures", i + 1, len(fixture_ids))

    df = pd.DataFrame(rows)
    if not df.empty:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "events.csv"
        df.to_csv(path, index=False)
        logger.info("Saved %d event rows to %s", len(df), path)
    else:
        logger.warning("No event data pulled")
    return df


# ------------------------------------------------------------------
# Step 1.10 — Betting Odds Pull
# ------------------------------------------------------------------


def fetch_odds(client: APIFootballClient, fixture_id: int) -> OddsResponse | None:
    """Fetch pre-match betting odds for a single fixture."""
    data = client.get("/odds", {"fixture": fixture_id})
    resp = data.get("response", [])
    if not resp:
        return None
    return OddsResponse.model_validate(resp[0])


def _extract_match_winner_odds(odds: OddsResponse) -> dict[str, Any] | None:
    """Extract average 1X2 odds across all bookmakers."""
    home_odds: list[float] = []
    draw_odds: list[float] = []
    away_odds: list[float] = []

    for bk in odds.bookmakers:
        for bet in bk.bets:
            if bet.name and "match winner" in bet.name.lower():
                for val in bet.values:
                    try:
                        odd = float(val.odd)
                    except (ValueError, TypeError):
                        continue
                    v = val.value.lower()
                    if v == "home":
                        home_odds.append(odd)
                    elif v == "draw":
                        draw_odds.append(odd)
                    elif v == "away":
                        away_odds.append(odd)

    if not (home_odds and draw_odds and away_odds):
        return None

    avg_home = sum(home_odds) / len(home_odds)
    avg_draw = sum(draw_odds) / len(draw_odds)
    avg_away = sum(away_odds) / len(away_odds)

    # Convert to implied probabilities and normalise
    raw_home = 1 / avg_home
    raw_draw = 1 / avg_draw
    raw_away = 1 / avg_away
    total = raw_home + raw_draw + raw_away

    return {
        "odds_home_avg": round(avg_home, 3),
        "odds_draw_avg": round(avg_draw, 3),
        "odds_away_avg": round(avg_away, 3),
        "odds_home_win": round(raw_home / total, 4),
        "odds_draw": round(raw_draw / total, 4),
        "odds_away_win": round(raw_away / total, 4),
    }


def pull_odds(
    client: APIFootballClient,
    fixtures_df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pull betting odds for all fixtures."""
    fixture_ids = fixtures_df["fixture_id"].unique()
    rows: list[dict[str, Any]] = []

    for i, fid in enumerate(fixture_ids):
        odds = fetch_odds(client, int(fid))
        if odds:
            extracted = _extract_match_winner_odds(odds)
            if extracted:
                extracted["fixture_id"] = int(fid)
                rows.append(extracted)
        if (i + 1) % 200 == 0:
            logger.info("Odds progress: %d/%d fixtures (%d with data)", i + 1, len(fixture_ids), len(rows))

    df = pd.DataFrame(rows)
    if not df.empty:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "odds.csv"
        df.to_csv(path, index=False)
        logger.info("Saved %d odds rows to %s", len(df), path)
    else:
        logger.warning("No odds data pulled")
    return df


# ------------------------------------------------------------------
# Step 1.11 — Match Statistics Pull
# ------------------------------------------------------------------


_STAT_KEYS = [
    "Shots on Goal",
    "Shots off Goal",
    "Total Shots",
    "Ball Possession",
    "Corner Kicks",
    "Fouls",
    "Passes %",
    "Passes accurate",
    "expected_goals",
]


def fetch_match_statistics(
    client: APIFootballClient, fixture_id: int
) -> list[FixtureStatistics]:
    """Fetch per-team match statistics for a single fixture."""
    data = client.get("/fixtures/statistics", {"fixture": fixture_id})
    return [FixtureStatistics.model_validate(item) for item in data.get("response", [])]


def _parse_stat_value(value: int | float | str | None) -> float | None:
    """Parse a stat value, stripping '%' if present."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().rstrip("%")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _extract_match_stat_row(
    fixture_id: int, stats: list[FixtureStatistics]
) -> dict[str, Any] | None:
    """Flatten per-team match statistics into a single row."""
    if len(stats) < 2:
        return None

    row: dict[str, Any] = {"fixture_id": fixture_id}

    for idx, prefix in enumerate(("home", "away")):
        team_stats = stats[idx]
        if team_stats.team and team_stats.team.id:
            row[f"{prefix}_team_id"] = team_stats.team.id

        stat_map = {s.type: s.value for s in team_stats.statistics if s.type}
        row[f"{prefix}_shots_on"] = _parse_stat_value(stat_map.get("Shots on Goal"))
        row[f"{prefix}_shots_off"] = _parse_stat_value(stat_map.get("Shots off Goal"))
        row[f"{prefix}_total_shots"] = _parse_stat_value(stat_map.get("Total Shots"))
        row[f"{prefix}_possession"] = _parse_stat_value(stat_map.get("Ball Possession"))
        row[f"{prefix}_corners"] = _parse_stat_value(stat_map.get("Corner Kicks"))
        row[f"{prefix}_fouls"] = _parse_stat_value(stat_map.get("Fouls"))
        row[f"{prefix}_passes_pct"] = _parse_stat_value(stat_map.get("Passes %"))
        row[f"{prefix}_passes_accurate"] = _parse_stat_value(stat_map.get("Passes accurate"))
        row[f"{prefix}_xg"] = _parse_stat_value(stat_map.get("expected_goals"))

    return row


def pull_match_statistics(
    client: APIFootballClient,
    fixtures_df: pd.DataFrame,
    min_year: int = 2006,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pull match statistics for all fixtures from min_year onward."""
    fixtures_df = fixtures_df.copy()
    fixtures_df["date"] = pd.to_datetime(fixtures_df["date"])
    recent = fixtures_df[fixtures_df["date"].dt.year >= min_year]
    fixture_ids = recent["fixture_id"].unique()

    rows: list[dict[str, Any]] = []
    for i, fid in enumerate(fixture_ids):
        stats = fetch_match_statistics(client, int(fid))
        if stats:
            extracted = _extract_match_stat_row(int(fid), stats)
            if extracted:
                rows.append(extracted)
        if (i + 1) % 200 == 0:
            logger.info(
                "Match stats progress: %d/%d fixtures (%d with data)",
                i + 1, len(fixture_ids), len(rows),
            )

    df = pd.DataFrame(rows)
    if not df.empty:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "match_statistics.csv"
        df.to_csv(path, index=False)
        logger.info("Saved %d match statistics rows to %s", len(df), path)
    else:
        logger.warning("No match statistics pulled")
    return df


# ------------------------------------------------------------------
# Step 1.12 — Injuries / Suspensions Pull
# ------------------------------------------------------------------


def fetch_injuries(client: APIFootballClient, fixture_id: int) -> list[Injury]:
    """Fetch injuries/suspensions for a single fixture."""
    data = client.get("/injuries", {"fixture": fixture_id})
    return [Injury.model_validate(item) for item in data.get("response", [])]


def _aggregate_injuries(
    fixture_id: int, injuries: list[Injury], player_ratings: dict[int, float] | None = None
) -> dict[int, dict[str, Any]]:
    """Aggregate injuries into per-team summary rows."""
    team_data: dict[int, dict[str, Any]] = {}
    player_ratings = player_ratings or {}

    for inj in injuries:
        if not inj.team or not inj.team.id:
            continue
        tid = inj.team.id
        if tid not in team_data:
            team_data[tid] = {
                "fixture_id": fixture_id,
                "team_id": tid,
                "injuries_count": 0,
                "suspensions_count": 0,
                "missing_total": 0,
                "missing_quality": 0.0,
            }
        td = team_data[tid]
        ptype = (inj.player.type or "").lower() if inj.player else ""
        if "missing" in ptype or "suspension" in ptype or "suspended" in ptype:
            td["suspensions_count"] += 1
        else:
            td["injuries_count"] += 1
        td["missing_total"] += 1

        # Add quality of missing player if we have their rating
        if inj.player and inj.player.id and inj.player.id in player_ratings:
            td["missing_quality"] += player_ratings[inj.player.id]

    return team_data


def pull_injuries(
    client: APIFootballClient,
    fixtures_df: pd.DataFrame,
    min_year: int = 2010,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pull injury/suspension data for all fixtures from min_year onward."""
    fixtures_df = fixtures_df.copy()
    fixtures_df["date"] = pd.to_datetime(fixtures_df["date"])
    recent = fixtures_df[fixtures_df["date"].dt.year >= min_year]
    fixture_ids = recent["fixture_id"].unique()

    # Load player ratings for quality metric
    player_ratings: dict[int, float] = {}
    players_path = output_dir / "players.csv"
    if players_path.exists():
        players_df = pd.read_csv(players_path)
        valid = players_df.dropna(subset=["player_id", "rating"])
        for _, row in valid.iterrows():
            try:
                player_ratings[int(row["player_id"])] = float(row["rating"])
            except (ValueError, TypeError):
                continue
        logger.info("Loaded %d player ratings for injury quality metric", len(player_ratings))

    rows: list[dict[str, Any]] = []
    for i, fid in enumerate(fixture_ids):
        injuries = fetch_injuries(client, int(fid))
        if injuries:
            team_data = _aggregate_injuries(int(fid), injuries, player_ratings)
            rows.extend(team_data.values())
        if (i + 1) % 200 == 0:
            logger.info(
                "Injuries progress: %d/%d fixtures (%d rows)",
                i + 1, len(fixture_ids), len(rows),
            )

    df = pd.DataFrame(rows)
    if not df.empty:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "injuries.csv"
        df.to_csv(path, index=False)
        logger.info("Saved %d injury rows to %s", len(df), path)
    else:
        logger.warning("No injury data pulled")
    return df
