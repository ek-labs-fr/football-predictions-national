"""Discover API-Football league_ids and fixture counts for tournaments
absent from training_table.csv.

Calls /leagues (cached) once, filters to the candidate tournaments, then
calls /fixtures for each (league_id, season) candidate to confirm fixture
counts. Total quota cost: ~15 requests. Caches in data/raw/national/ so
re-runs are free.

Targets:
  - WC 2026 Qualifying (UEFA, AFC, CAF, CONCACAF, CONMEBOL)
  - AFCON 2025 (postponed; played late 2025 / early 2026)
  - CONCACAF Gold Cup 2025
  - Olympics 2024 Men's Football

Output: outputs/missing_leagues_probe.txt
"""

from __future__ import annotations

import sys
from pathlib import Path

from src.data.api_client import APIFootballClient

_OUTPUT = Path("outputs/missing_leagues_probe.txt")

# Substrings to match against league names (lowercased)
_PATTERNS = {
    "WC 2026 Qual": ["world cup", "qualif"],
    "AFCON": ["africa cup", "afcon"],
    "Gold Cup": ["gold cup", "concacaf gold"],
    "Olympics Men": ["olymp"],
}


def main() -> None:
    out: list[str] = []

    def emit(s: str) -> None:
        out.append(s)
        sys.stdout.buffer.write((s + "\n").encode("utf-8"))

    client = APIFootballClient(plan="pro")
    emit(f"Daily quota remaining at start: {client.daily_limit}")
    emit("")

    # Pull all International cups (one request)
    data = client.get("/leagues", {"type": "Cup", "country": "World"})
    items = data.get("response", [])
    emit(f"/leagues type=Cup country=World — returned {len(items)} leagues")
    emit("")

    # Group leagues by family
    by_family: dict[str, list[dict]] = {k: [] for k in _PATTERNS}
    for item in items:
        league = item.get("league", {})
        name = (league.get("name") or "").lower()
        for family, patterns in _PATTERNS.items():
            if all(p in name for p in patterns):
                by_family[family].append(item)
                break

    # Print discovered leagues
    for family, group in by_family.items():
        emit(f"=== {family} ===")
        if not group:
            emit("  (none found)")
            continue
        for item in group:
            league = item.get("league", {})
            seasons = item.get("seasons", [])
            recent_seasons = [s.get("year") for s in seasons if s.get("year", 0) >= 2023]
            emit(
                f"  id={league.get('id'):>4d}  "
                f"name={league.get('name'):<60s}  "
                f"recent_seasons={sorted(recent_seasons)}"
            )
        emit("")

    # Pull fixture counts for the most relevant (id, season) combos
    candidates: list[tuple[str, int, int]] = []
    for family, group in by_family.items():
        for item in group:
            league = item.get("league", {})
            seasons = item.get("seasons", [])
            for s in seasons:
                year = s.get("year")
                if year and year >= 2023:
                    candidates.append((family, int(league["id"]), int(year)))

    emit("=" * 80)
    emit(f"FIXTURE COUNT PROBE — {len(candidates)} (id, season) combos")
    emit("=" * 80)
    for family, lid, season in candidates:
        try:
            resp = client.get("/fixtures", {"league": lid, "season": season})
            n = len(resp.get("response", []))
            emit(f"  {family:<18s}  id={lid:>4d}  season={season}  N_fixtures={n}")
        except Exception as exc:  # noqa: BLE001
            emit(f"  {family:<18s}  id={lid:>4d}  season={season}  ERROR: {exc}")

    emit("")
    emit(f"Last API quota remaining: {client.last_quota_remaining}")
    emit(f"Last API quota limit:     {client.last_quota_limit}")

    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT.write_text("\n".join(out), encoding="utf-8")


if __name__ == "__main__":
    main()
