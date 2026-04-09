"""Unit tests for the API-Football client."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    from pathlib import Path

import pytest
import requests

from src.data.api_client import (
    APIFootballClient,
    APIFootballError,
    RateLimitError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE: dict = {
    "get": "fixtures",
    "parameters": {"league": "1", "season": "2022"},
    "errors": [],
    "results": 1,
    "paging": {"current": 1, "total": 1},
    "response": [{"fixture": {"id": 123}}],
}


def _make_client(tmp_path: Path, **kwargs) -> APIFootballClient:  # type: ignore[no-untyped-def]
    return APIFootballClient(
        api_key="test-key",
        cache_dir=tmp_path / "cache",
        plan=kwargs.pop("plan", "pro"),
        **kwargs,
    )


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or SAMPLE_RESPONSE
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status_code}", response=resp)
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCacheHit:
    """Cache hit should skip the HTTP call entirely."""

    @patch("src.data.api_client.requests.get")
    def test_cache_hit_skips_http(self, mock_get: MagicMock, tmp_path: Path) -> None:
        client = _make_client(tmp_path)

        # Pre-populate cache
        cache_path = client._cache_path("/fixtures", {"league": "1"})
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(SAMPLE_RESPONSE), encoding="utf-8")

        result = client.get("/fixtures", {"league": "1"})

        mock_get.assert_not_called()
        assert result == SAMPLE_RESPONSE

    @patch("src.data.api_client.requests.get")
    def test_miss_then_hit(self, mock_get: MagicMock, tmp_path: Path) -> None:
        mock_get.return_value = _mock_response()
        client = _make_client(tmp_path)

        # First call — cache miss, hits HTTP
        client.get("/fixtures", {"league": "1"})
        assert mock_get.call_count == 1

        # Second call — cache hit
        client.get("/fixtures", {"league": "1"})
        assert mock_get.call_count == 1  # no additional call


class TestRateLimit:
    """Rate limiting should raise when quota is exhausted."""

    @patch("src.data.api_client.requests.get")
    def test_raises_when_quota_exhausted(self, mock_get: MagicMock, tmp_path: Path) -> None:
        mock_get.return_value = _mock_response()
        client = _make_client(tmp_path)
        client.daily_limit = 2  # override for test

        # Use up quota with distinct params to avoid cache hits
        client.get("/fixtures", {"league": "1"})
        client.get("/fixtures", {"league": "2"})

        with pytest.raises(RateLimitError, match="Daily limit of 2"):
            client.get("/fixtures", {"league": "3"})

    @patch("src.data.api_client.requests.get")
    def test_cache_hits_dont_count(self, mock_get: MagicMock, tmp_path: Path) -> None:
        mock_get.return_value = _mock_response()
        client = _make_client(tmp_path)
        client.daily_limit = 1  # override for test

        # First call uses the quota
        client.get("/fixtures", {"league": "1"})

        # Same params → cache hit, should not raise
        result = client.get("/fixtures", {"league": "1"})
        assert result == SAMPLE_RESPONSE


class TestRetry:
    """Retry on 429 and 5xx status codes."""

    @patch("src.data.api_client.time.sleep")
    @patch("src.data.api_client.requests.get")
    def test_retry_on_429_then_success(
        self, mock_get: MagicMock, mock_sleep: MagicMock, tmp_path: Path
    ) -> None:
        mock_get.side_effect = [
            _mock_response(429),
            _mock_response(200),
        ]
        client = _make_client(tmp_path)

        result = client.get("/fixtures", {"league": "1"})

        assert result == SAMPLE_RESPONSE
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()

    @patch("src.data.api_client.time.sleep")
    @patch("src.data.api_client.requests.get")
    def test_retry_on_500_then_success(
        self, mock_get: MagicMock, mock_sleep: MagicMock, tmp_path: Path
    ) -> None:
        mock_get.side_effect = [
            _mock_response(500),
            _mock_response(200),
        ]
        client = _make_client(tmp_path)

        result = client.get("/fixtures", {"league": "1"})

        assert result == SAMPLE_RESPONSE
        assert mock_get.call_count == 2

    @patch("src.data.api_client.time.sleep")
    @patch("src.data.api_client.requests.get")
    def test_raises_after_max_retries(
        self, mock_get: MagicMock, mock_sleep: MagicMock, tmp_path: Path
    ) -> None:
        mock_get.return_value = _mock_response(503)
        client = _make_client(tmp_path)

        with pytest.raises(requests.HTTPError):
            client.get("/fixtures", {"league": "1"})

        assert mock_get.call_count == 3  # _MAX_RETRIES


class TestErrorDetection:
    """API error field should be detected and raised."""

    @patch("src.data.api_client.requests.get")
    def test_list_errors_raises(self, mock_get: MagicMock, tmp_path: Path) -> None:
        error_response = {**SAMPLE_RESPONSE, "errors": ["Invalid API key"]}
        mock_get.return_value = _mock_response(200, error_response)
        client = _make_client(tmp_path)

        with pytest.raises(APIFootballError, match="Invalid API key"):
            client.get("/fixtures", {"league": "1"})

    @patch("src.data.api_client.requests.get")
    def test_dict_errors_raises(self, mock_get: MagicMock, tmp_path: Path) -> None:
        error_response = {**SAMPLE_RESPONSE, "errors": {"token": "Error/Missing token"}}
        mock_get.return_value = _mock_response(200, error_response)
        client = _make_client(tmp_path)

        with pytest.raises(APIFootballError, match="Error/Missing token"):
            client.get("/fixtures", {"league": "1"})

    @patch("src.data.api_client.requests.get")
    def test_empty_errors_ok(self, mock_get: MagicMock, tmp_path: Path) -> None:
        mock_get.return_value = _mock_response(200, SAMPLE_RESPONSE)
        client = _make_client(tmp_path)

        result = client.get("/fixtures", {"league": "1"})
        assert result["results"] == 1


class TestConstructor:
    """Constructor validation."""

    def test_missing_api_key_raises(self, tmp_path: Path) -> None:
        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(ValueError, match="API key required"),
        ):
            APIFootballClient(api_key="", cache_dir=tmp_path)

    def test_creates_cache_dir(self, tmp_path: Path) -> None:
        cache = tmp_path / "sub" / "cache"
        assert not cache.exists()
        APIFootballClient(api_key="test-key", cache_dir=cache)
        assert cache.exists()
