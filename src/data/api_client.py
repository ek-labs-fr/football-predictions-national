"""Rate-limited, cached, retrying client for API-Football v3."""

from __future__ import annotations

import collections
import hashlib
import json
import logging
import os
import time
from datetime import date
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://v3.football.api-sports.io"
_DEFAULT_CACHE_DIR = Path("data/raw/national")
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0  # seconds

RATE_LIMITS: dict[str, dict[str, int]] = {
    "free": {"daily": 100, "per_minute": 30, "per_second": 30},
    "pro": {"daily": 7500, "per_minute": 300, "per_second": 300},
    "ultra": {"daily": 75000, "per_minute": 300, "per_second": 300},
    "mega": {"daily": 150000, "per_minute": 300, "per_second": 300},
}
_DEFAULT_PLAN = "pro"


class RateLimitError(Exception):
    """Raised when the daily request quota is exhausted."""


class APIFootballError(Exception):
    """Raised when the API returns a non-empty errors field."""


class APIFootballClient:
    """Reusable client for API-Football v3 with rate limiting, disk caching, and retries.

    Parameters
    ----------
    api_key:
        API key. Falls back to the ``API_FOOTBALL_KEY`` env var.
    base_url:
        API base URL. Defaults to ``https://v3.football.api-sports.io``.
    cache_dir:
        Root directory for cached JSON responses. Defaults to ``data/raw/national``.
        For club ingestion, pass ``cache_dir="data/raw/club"`` explicitly.
    plan:
        API-Football subscription plan name (``"free"``, ``"pro"``, ``"ultra"``,
        ``"mega"``). Determines daily, per-minute, and per-second rate limits.
        Defaults to ``"pro"``.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = _DEFAULT_BASE_URL,
        cache_dir: str | Path = _DEFAULT_CACHE_DIR,
        plan: str = _DEFAULT_PLAN,
    ) -> None:
        self.api_key = api_key or os.getenv("API_FOOTBALL_KEY", "")
        if not self.api_key:
            raise ValueError("API key required — pass api_key or set the API_FOOTBALL_KEY env var")
        plan = plan.lower()
        if plan not in RATE_LIMITS:
            raise ValueError(f"Unknown plan {plan!r} — choose from {list(RATE_LIMITS)}")
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        limits = RATE_LIMITS[plan]
        self.daily_limit = limits["daily"]
        self.per_minute_limit = limits["per_minute"]
        self.per_second_limit = limits["per_second"]

        # Daily counter keyed by date string
        self._request_date: str = ""
        self._request_count: int = 0
        # Rolling window timestamps for per-second and per-minute throttling
        self._timestamps: collections.deque[float] = collections.deque()
        # Latest API-Football quota headers (None until the first non-cached request)
        self.last_quota_remaining: int | None = None
        self.last_quota_limit: int | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a GET request with caching, rate limiting, and retries.

        Parameters
        ----------
        endpoint:
            API endpoint path, e.g. ``"/fixtures"`` or ``"fixtures"``.
        params:
            Query parameters forwarded to the API.

        Returns
        -------
        dict
            The full JSON response from API-Football.
        """
        endpoint = "/" + endpoint.lstrip("/")
        params = params or {}

        # 1. Check cache
        cache_path = self._cache_path(endpoint, params)
        cached = self._read_cache(cache_path)
        if cached is not None:
            logger.info("CACHE HIT  %s %s", endpoint, params)
            return cached

        # 2. Check rate limit
        self._enforce_rate_limit()

        # 3. Make request with retries
        data = self._request_with_retries(endpoint, params)

        # 4. Validate response
        self._validate_response(data)

        # 5. Write cache
        self._write_cache(cache_path, data)

        return data

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _enforce_rate_limit(self) -> None:
        today = date.today().isoformat()
        if self._request_date != today:
            self._request_date = today
            self._request_count = 0

        if self._request_count >= self.daily_limit:
            raise RateLimitError(f"Daily limit of {self.daily_limit} requests reached for {today}")

        now = time.monotonic()
        self._purge_old_timestamps(now)

        # Per-second throttle: wait if we've hit the per-second limit
        one_sec_ago = now - 1.0
        recent_1s = sum(1 for ts in self._timestamps if ts > one_sec_ago)
        if recent_1s >= self.per_second_limit:
            wait = self._timestamps[0] - one_sec_ago
            logger.info("Per-second limit reached — sleeping %.2fs", wait)
            time.sleep(wait)

        # Per-minute throttle: wait if we've hit the per-minute limit
        now = time.monotonic()
        one_min_ago = now - 60.0
        recent_1m = sum(1 for ts in self._timestamps if ts > one_min_ago)
        if recent_1m >= self.per_minute_limit:
            wait = self._timestamps[0] - one_min_ago
            logger.info("Per-minute limit reached — sleeping %.2fs", wait)
            time.sleep(wait)

    def _increment_counter(self) -> None:
        today = date.today().isoformat()
        if self._request_date != today:
            self._request_date = today
            self._request_count = 0
        self._request_count += 1
        self._timestamps.append(time.monotonic())

    def _purge_old_timestamps(self, now: float) -> None:
        cutoff = now - 60.0
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()

    # ------------------------------------------------------------------
    # HTTP with retries
    # ------------------------------------------------------------------

    def _capture_quota_headers(self, headers: Any) -> None:
        for header, attr in (
            ("x-ratelimit-requests-remaining", "last_quota_remaining"),
            ("x-ratelimit-requests-limit", "last_quota_limit"),
        ):
            raw = headers.get(header)
            if raw is None:
                continue
            try:
                setattr(self, attr, int(raw))
            except (TypeError, ValueError):
                continue

    def _request_with_retries(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        headers = {"x-apisports-key": self.api_key}
        last_exc: Exception | None = None

        for attempt in range(_MAX_RETRIES):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
                self._increment_counter()
                logger.info(
                    "REQUEST    %s %s → %s (attempt %d, count %d/%d)",
                    endpoint,
                    params,
                    resp.status_code,
                    attempt + 1,
                    self._request_count,
                    self.daily_limit,
                )

                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = _BACKOFF_BASE**attempt
                    logger.warning("Retryable status %d — sleeping %.1fs", resp.status_code, wait)
                    time.sleep(wait)
                    last_exc = requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                    continue

                resp.raise_for_status()
                self._capture_quota_headers(resp.headers)
                return resp.json()  # type: ignore[no-any-return]

            except requests.ConnectionError as exc:
                wait = _BACKOFF_BASE**attempt
                logger.warning("Connection error — sleeping %.1fs", wait)
                time.sleep(wait)
                last_exc = exc

        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Response validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_response(data: dict[str, Any]) -> None:
        errors = data.get("errors")
        if errors and (isinstance(errors, list) and len(errors) > 0):
            raise APIFootballError(f"API returned errors: {errors}")
        if errors and isinstance(errors, dict) and len(errors) > 0:
            raise APIFootballError(f"API returned errors: {errors}")

    # ------------------------------------------------------------------
    # Disk cache
    # ------------------------------------------------------------------

    def _cache_key(self, endpoint: str, params: dict[str, Any]) -> str:
        stable = json.dumps(params, sort_keys=True)
        h = hashlib.sha256(stable.encode()).hexdigest()[:12]
        name = endpoint.strip("/").replace("/", "_")
        return f"{name}_{h}"

    def _cache_path(self, endpoint: str, params: dict[str, Any]) -> Path:
        subdir = endpoint.strip("/").replace("/", "_")
        return self.cache_dir / subdir / f"{self._cache_key(endpoint, params)}.json"

    @staticmethod
    def _read_cache(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
        except (json.JSONDecodeError, OSError):
            logger.warning("Corrupt cache file %s — ignoring", path)
            return None

    @staticmethod
    def _write_cache(path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.debug("Cached response to %s", path)
