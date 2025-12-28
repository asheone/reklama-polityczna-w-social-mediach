"""
Tests for the rate limiter module.
"""

import time
import pytest
from shared.rate_limiter import RateLimiter
from shared.exceptions import RateLimitError


class TestRateLimiter:
    """Test cases for RateLimiter class."""

    def test_init_default_values(self):
        """Test initialization with default values."""
        limiter = RateLimiter()

        assert limiter.requests_per_minute == 180
        assert limiter.burst == 5
        assert limiter.tokens == 5.0
        assert limiter.total_requests == 0

    def test_init_custom_values(self):
        """Test initialization with custom values."""
        limiter = RateLimiter(
            requests_per_minute=60,
            burst=10,
            backoff_multiplier=3.0,
            max_retries=5
        )

        assert limiter.requests_per_minute == 60
        assert limiter.burst == 10
        assert limiter.backoff_multiplier == 3.0
        assert limiter.max_retries == 5

    def test_wait_if_needed_consumes_token(self):
        """Test that wait_if_needed consumes a token."""
        limiter = RateLimiter(requests_per_minute=60, burst=5)
        initial_tokens = limiter.tokens

        limiter.wait_if_needed()

        assert limiter.tokens < initial_tokens
        assert limiter.total_requests == 1

    def test_wait_if_needed_tracks_requests(self):
        """Test that requests are tracked in history."""
        limiter = RateLimiter(requests_per_minute=600, burst=10)

        for _ in range(5):
            limiter.wait_if_needed()

        assert limiter.total_requests == 5
        assert len(limiter.request_history) == 5

    def test_wait_if_needed_waits_when_no_tokens(self):
        """Test that wait_if_needed waits when tokens exhausted."""
        # High rate to avoid waiting during consumption
        limiter = RateLimiter(requests_per_minute=6000, burst=2)

        # Consume all tokens quickly
        limiter.wait_if_needed()
        limiter.wait_if_needed()

        # Next request should wait
        start_time = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start_time

        # Should have waited some time (token refill)
        assert elapsed >= 0.001  # At least some waiting

    def test_get_stats(self):
        """Test statistics retrieval."""
        limiter = RateLimiter(requests_per_minute=600, burst=10)

        for _ in range(3):
            limiter.wait_if_needed()

        stats = limiter.get_stats()

        assert stats["total_requests"] == 3
        assert stats["requests_last_minute"] == 3
        assert stats["requests_per_minute_limit"] == 600
        assert "tokens_remaining" in stats
        assert "utilization_percent" in stats

    def test_handle_rate_limit_error_basic(self):
        """Test handling rate limit error."""
        limiter = RateLimiter(
            requests_per_minute=60,
            initial_retry_delay=0.01  # Very short for testing
        )

        start_time = time.time()
        limiter.handle_rate_limit_error(retry_after=0.01, attempt=1)
        elapsed = time.time() - start_time

        assert elapsed >= 0.01
        assert limiter.rate_limit_errors == 1

    def test_handle_rate_limit_error_max_retries(self):
        """Test that max retries raises exception."""
        limiter = RateLimiter(max_retries=3)

        with pytest.raises(RateLimitError) as exc_info:
            limiter.handle_rate_limit_error(attempt=4)

        assert "max retries" in str(exc_info.value).lower()

    def test_reset(self):
        """Test resetting the rate limiter."""
        limiter = RateLimiter(requests_per_minute=600, burst=5)

        # Use some tokens
        for _ in range(3):
            limiter.wait_if_needed()

        limiter.reset()

        assert limiter.tokens == 5.0
        assert limiter.total_requests == 0
        assert len(limiter.request_history) == 0
        assert limiter.rate_limit_errors == 0

    def test_request_history_cleanup(self):
        """Test that old request history is cleaned up."""
        limiter = RateLimiter(requests_per_minute=600, burst=10)

        # Add some requests
        for _ in range(5):
            limiter.wait_if_needed()

        # Manually add old timestamp (older than 5 minutes)
        old_time = time.time() - 400  # 400 seconds ago
        limiter.request_history.insert(0, old_time)

        # Next request should clean up old entry
        limiter.wait_if_needed()

        # Old entry should be removed
        assert all(t > time.time() - 300 for t in limiter.request_history)
