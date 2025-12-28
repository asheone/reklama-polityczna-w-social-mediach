"""
Token bucket rate limiter for API request throttling.
Works in both local and Cloud Functions environments.
"""

import time
from typing import Optional
from shared.logger import get_logger
from shared.exceptions import RateLimitError


class RateLimiter:
    """
    Token bucket rate limiter for controlling API request rates.

    Features:
    - Token bucket algorithm for smooth rate limiting
    - Burst allowance for handling short-term spikes
    - Automatic backoff on rate limit errors
    - Statistics tracking for monitoring
    """

    def __init__(
        self,
        requests_per_minute: int = 180,
        burst: int = 5,
        backoff_multiplier: float = 2.0,
        max_retries: int = 3,
        initial_retry_delay: float = 60.0
    ):
        """
        Initialize the rate limiter.

        Args:
            requests_per_minute: Maximum requests allowed per minute
            burst: Maximum burst size (token bucket capacity)
            backoff_multiplier: Multiplier for exponential backoff
            max_retries: Maximum retry attempts on rate limit errors
            initial_retry_delay: Initial delay in seconds for rate limit retries
        """
        self.requests_per_minute = requests_per_minute
        self.requests_per_second = requests_per_minute / 60.0
        self.burst = burst
        self.backoff_multiplier = backoff_multiplier
        self.max_retries = max_retries
        self.initial_retry_delay = initial_retry_delay

        # Token bucket state
        self.tokens = float(burst)
        self.last_update = time.time()

        # Statistics
        self.request_history: list[float] = []
        self.total_requests = 0
        self.total_wait_time = 0.0
        self.rate_limit_errors = 0

        self.logger = get_logger("rate_limiter")

    def wait_if_needed(self) -> float:
        """
        Wait if no tokens are available (token bucket algorithm).

        Returns:
            Time waited in seconds (0 if no wait was needed)
        """
        now = time.time()
        elapsed = now - self.last_update

        # Refill tokens based on elapsed time
        self.tokens = min(
            float(self.burst),
            self.tokens + elapsed * self.requests_per_second
        )
        self.last_update = now

        wait_time = 0.0

        # If no tokens available, wait until one is available
        if self.tokens < 1.0:
            wait_time = (1.0 - self.tokens) / self.requests_per_second
            self.logger.debug(f"Rate limit: waiting {wait_time:.2f}s for token")
            time.sleep(wait_time)
            self.tokens = 1.0
            self.total_wait_time += wait_time

        # Consume one token
        self.tokens -= 1.0
        self.total_requests += 1

        # Track request in history
        current_time = time.time()
        self.request_history.append(current_time)

        # Clean old history (keep last 5 minutes)
        cutoff = current_time - 300
        self.request_history = [t for t in self.request_history if t > cutoff]

        return wait_time

    def handle_rate_limit_error(
        self,
        retry_after: Optional[int] = None,
        attempt: int = 1
    ) -> float:
        """
        Handle a 429 rate limit response with exponential backoff.

        Args:
            retry_after: Seconds to wait (from API response header)
            attempt: Current retry attempt number

        Returns:
            Time waited in seconds

        Raises:
            RateLimitError: If max retries exceeded
        """
        if attempt > self.max_retries:
            self.logger.error(
                f"Rate limit: max retries ({self.max_retries}) exceeded"
            )
            raise RateLimitError(
                message=f"Rate limit exceeded after {self.max_retries} retries",
                retry_after=retry_after
            )

        self.rate_limit_errors += 1

        # Calculate wait time with exponential backoff
        if retry_after is not None:
            wait_time = float(retry_after)
        else:
            wait_time = self.initial_retry_delay * (
                self.backoff_multiplier ** (attempt - 1)
            )

        self.logger.warning(
            f"Rate limit hit (attempt {attempt}/{self.max_retries}). "
            f"Waiting {wait_time:.1f}s before retry"
        )

        time.sleep(wait_time)
        self.total_wait_time += wait_time

        # Reset tokens to allow retry
        self.tokens = float(self.burst)
        self.last_update = time.time()

        return wait_time

    def get_stats(self) -> dict:
        """
        Return current rate limit statistics for logging/monitoring.

        Returns:
            Dictionary with rate limiter statistics
        """
        now = time.time()
        last_minute = [t for t in self.request_history if t > now - 60]
        last_5_minutes = [t for t in self.request_history if t > now - 300]

        return {
            "tokens_remaining": round(self.tokens, 2),
            "requests_last_minute": len(last_minute),
            "requests_last_5_minutes": len(last_5_minutes),
            "requests_per_minute_limit": self.requests_per_minute,
            "total_requests": self.total_requests,
            "total_wait_time_seconds": round(self.total_wait_time, 2),
            "rate_limit_errors": self.rate_limit_errors,
            "utilization_percent": round(
                len(last_minute) / self.requests_per_minute * 100, 1
            ) if self.requests_per_minute > 0 else 0
        }

    def reset(self):
        """Reset the rate limiter to initial state."""
        self.tokens = float(self.burst)
        self.last_update = time.time()
        self.request_history.clear()
        self.total_requests = 0
        self.total_wait_time = 0.0
        self.rate_limit_errors = 0
        self.logger.info("Rate limiter reset")
