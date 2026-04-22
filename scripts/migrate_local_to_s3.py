"""Re-key the local ``data/raw/`` cache into the S3 ingest-bucket layout and upload.

The local cache (``APIFootballClient``) stores each response as
``data/raw/{domain}/{endpoint}/{endpoint}_<hash>.json`` — hash-keyed, opaque.
The daily Lambda writes human-readable keys like ``{domain}/{endpoint}/{fixture_id}.json``
or ``{domain}/fixtures/{date}/league=X-season=Y.json``. This script bridges the
two so the bucket ends up with a single convention.

Each local JSON carries a ``parameters`` dict echoing the original request, which
we use to derive a stable S3 key. Endpoint → key mapping lives in
``_S3_KEY_RULES`` below.

Usage:
    uv run python scripts/migrate_local_to_s3.py --bucket <name> --dry-run
    uv run python scripts/migrate_local_to_s3.py --bucket <name>
    uv run python scripts/migrate_local_to_s3.py --bucket <name> --domain club
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("migrate")


RAW_ROOT = Path("data/raw")


# --------------------------------------------------------------------------- #
# Per-endpoint key derivation — returns the S3 key *suffix* under {domain}/.  #
# --------------------------------------------------------------------------- #

KeyFn = Callable[[dict[str, Any]], str]


def _fixture_key(p: dict[str, Any]) -> str:
    return f"{p['fixture']}.json"


def _team_key(p: dict[str, Any]) -> str:
    return f"team={p['team']}.json"


def _league_season_key(p: dict[str, Any]) -> str:
    return f"league={p['league']}-season={p['season']}.json"


def _league_season_team_key(p: dict[str, Any]) -> str:
    return f"league={p['league']}-season={p['season']}-team={p['team']}.json"


def _fixtures_list_key(p: dict[str, Any]) -> str:
    base = f"historical/league={p['league']}-season={p['season']}"
    if "from" in p and "to" in p:
        base += f"_from={p['from']}_to={p['to']}"
    return f"{base}.json"


def _h2h_key(p: dict[str, Any]) -> str:
    suffix = f"-last{p['last']}" if "last" in p else ""
    return f"{p['h2h']}{suffix}.json"


def _players_key(p: dict[str, Any]) -> str:
    page = p.get("page", "1")
    return f"team={p['team']}-season={p['season']}-page={page}.json"


def _venues_key(p: dict[str, Any]) -> str:
    if "country" in p:
        return f"country={p['country']}.json"
    if "id" in p:
        return f"id={p['id']}.json"
    return _fallback_key(p)


def _fallback_key(p: dict[str, Any]) -> str:
    """Generic: join all params sorted for endpoints without a dedicated rule."""
    if not p:
        return "all.json"
    parts = [f"{k}={v}" for k, v in sorted(p.items())]
    return "_".join(parts) + ".json"


# endpoint_subdir → (s3_prefix_under_domain, key_fn)
_S3_KEY_RULES: dict[str, tuple[str, KeyFn]] = {
    # Per-fixture detail endpoints
    "fixtures_events":     ("fixtures_events",     _fixture_key),
    "fixtures_statistics": ("fixtures_statistics", _fixture_key),
    "fixtures_lineups":    ("fixtures_lineups",    _fixture_key),
    "odds":                ("odds",                _fixture_key),
    "injuries":            ("injuries",            _fixture_key),
    # H2H is keyed by the team-pair, not a fixture
    "fixtures_headtohead": ("fixtures_headtohead", _h2h_key),
    # Fixture lists — historical snapshots under a separate subprefix
    "fixtures":            ("fixtures",            _fixtures_list_key),
    # Reference endpoints
    "teams":               ("teams",               _league_season_key),
    "standings":           ("standings",           _league_season_key),
    "teams_statistics":    ("teams_statistics",    _league_season_team_key),
    "players":             ("players",             _players_key),
    "players_squads":      ("players_squads",      _team_key),
    "coachs":              ("coachs",              _team_key),
    "transfers":           ("transfers",           _team_key),
    "leagues":             ("leagues",             _fallback_key),
    "venues":              ("venues",              _venues_key),
}


# --------------------------------------------------------------------------- #
# File → S3 key resolution                                                    #
# --------------------------------------------------------------------------- #


def _extract_params(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("parameters") or payload.get("get") or {}


def resolve_s3_key(local_path: Path, domain: str) -> str | None:
    """Given a local JSON cache file, return the target S3 key, or None to skip."""
    parts = local_path.relative_to(RAW_ROOT / domain).parts
    # Top-level {domain}/foo.json — e.g. leagues.json, teams.json
    if len(parts) == 1:
        return f"{domain}/{parts[0]}"

    endpoint = parts[0]
    rule = _S3_KEY_RULES.get(endpoint)
    if rule is None:
        logger.warning("No rule for endpoint %s — skipping %s", endpoint, local_path)
        return None

    try:
        payload = json.loads(local_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Unreadable %s: %s", local_path, exc)
        return None

    params = _extract_params(payload)
    if not params:
        logger.warning("No parameters in %s — skipping", local_path)
        return None

    s3_prefix, key_fn = rule
    try:
        suffix = key_fn(params)
    except KeyError as exc:
        logger.warning("Missing param %s for %s (%s)", exc, endpoint, local_path.name)
        return None

    return f"{domain}/{s3_prefix}/{suffix}"


# --------------------------------------------------------------------------- #
# Driver                                                                      #
# --------------------------------------------------------------------------- #


def _collect(domain: str) -> list[Path]:
    root = RAW_ROOT / domain
    if not root.exists():
        return []
    return [p for p in root.rglob("*.json") if p.is_file() and not p.name.endswith(".gitkeep")]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bucket", required=True, help="Target S3 bucket name")
    ap.add_argument("--region", default="eu-west-3")
    ap.add_argument(
        "--domain", choices=["club", "national", "all"], default="all",
        help="Which domain(s) to migrate",
    )
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--dry-run", action="store_true", help="Resolve keys but do not upload")
    args = ap.parse_args()

    domains = ["club", "national"] if args.domain == "all" else [args.domain]

    # Resolve every local file to its (local_path, s3_key) tuple first.
    plan: list[tuple[Path, str]] = []
    unresolved = Counter()
    for domain in domains:
        files = _collect(domain)
        logger.info("[%s] %d local files", domain, len(files))
        for p in files:
            key = resolve_s3_key(p, domain)
            if key is None:
                unresolved[p.parent.name] += 1
                continue
            plan.append((p, key))

    if unresolved:
        logger.warning("Unresolved by endpoint: %s", dict(unresolved))

    # Detect collisions (same s3 key from multiple local files) — last-write wins,
    # but report them so we know.
    by_key: dict[str, list[Path]] = defaultdict(list)
    for p, k in plan:
        by_key[k].append(p)
    collisions = {k: v for k, v in by_key.items() if len(v) > 1}
    if collisions:
        logger.info("%d S3 keys with multiple local sources (last-write wins)", len(collisions))

    per_prefix = Counter(k.split("/", 2)[1] for _, k in plan)
    logger.info("Plan: %d uploads across prefixes: %s", len(plan), dict(per_prefix))

    if args.dry_run:
        logger.info("--dry-run: not uploading. Sample keys:")
        for p, k in plan[:5]:
            logger.info("  %s  →  s3://%s/%s", p, args.bucket, k)
        return 0

    s3 = boto3.client("s3", region_name=args.region)

    def _upload(item: tuple[Path, str]) -> tuple[str, bool, str]:
        local, key = item
        try:
            s3.upload_file(
                Filename=str(local),
                Bucket=args.bucket,
                Key=key,
                ExtraArgs={"ContentType": "application/json"},
            )
            return key, True, ""
        except Exception as exc:  # noqa: BLE001
            return key, False, str(exc)

    done = 0
    errors: list[tuple[str, str]] = []
    total = len(plan)
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for key, ok, err in ex.map(_upload, plan):
            done += 1
            if not ok:
                errors.append((key, err))
            if done % 500 == 0 or done == total:
                logger.info("  uploaded %d/%d", done, total)

    if errors:
        logger.error("%d uploads failed. First 5: %s", len(errors), errors[:5])
        return 1

    logger.info("Migration complete — %d objects uploaded.", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
