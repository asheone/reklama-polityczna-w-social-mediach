"""
Custom exceptions for the political ad collector framework.
Provides structured error handling across all collectors.
"""


class CollectorError(Exception):
    """Base exception for all collector errors."""

    def __init__(self, message: str, platform: str = None, details: dict = None):
        self.message = message
        self.platform = platform
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self):
        if self.platform:
            return f"[{self.platform}] {self.message}"
        return self.message

    def to_dict(self) -> dict:
        """Convert exception to dictionary for logging."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "platform": self.platform,
            "details": self.details
        }


class AuthenticationError(CollectorError):
    """Raised when API authentication fails."""

    def __init__(self, message: str, platform: str = None, details: dict = None):
        super().__init__(
            message=message or "Authentication failed",
            platform=platform,
            details=details
        )


class RateLimitError(CollectorError):
    """Raised when API rate limit is exceeded."""

    def __init__(
        self,
        message: str = None,
        platform: str = None,
        retry_after: int = None,
        details: dict = None
    ):
        self.retry_after = retry_after
        details = details or {}
        if retry_after:
            details["retry_after_seconds"] = retry_after
        super().__init__(
            message=message or "Rate limit exceeded",
            platform=platform,
            details=details
        )


class CheckpointError(CollectorError):
    """Raised when checkpoint operations fail."""

    def __init__(
        self,
        message: str = None,
        platform: str = None,
        operation: str = None,
        details: dict = None
    ):
        self.operation = operation
        details = details or {}
        if operation:
            details["operation"] = operation
        super().__init__(
            message=message or "Checkpoint operation failed",
            platform=platform,
            details=details
        )


class ValidationError(CollectorError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str = None,
        platform: str = None,
        field: str = None,
        value: any = None,
        details: dict = None
    ):
        self.field = field
        self.value = value
        details = details or {}
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = str(value)[:100]  # Truncate long values
        super().__init__(
            message=message or "Validation failed",
            platform=platform,
            details=details
        )


class OutputError(CollectorError):
    """Raised when output operations fail."""

    def __init__(
        self,
        message: str = None,
        platform: str = None,
        output_path: str = None,
        details: dict = None
    ):
        self.output_path = output_path
        details = details or {}
        if output_path:
            details["output_path"] = output_path
        super().__init__(
            message=message or "Output operation failed",
            platform=platform,
            details=details
        )


class APIError(CollectorError):
    """Raised when API calls fail."""

    def __init__(
        self,
        message: str = None,
        platform: str = None,
        status_code: int = None,
        response_body: str = None,
        details: dict = None
    ):
        self.status_code = status_code
        self.response_body = response_body
        details = details or {}
        if status_code:
            details["status_code"] = status_code
        if response_body:
            details["response_body"] = response_body[:500]  # Truncate
        super().__init__(
            message=message or f"API error (status {status_code})",
            platform=platform,
            details=details
        )


class ConfigurationError(CollectorError):
    """Raised when configuration is invalid or missing."""

    def __init__(
        self,
        message: str = None,
        config_key: str = None,
        details: dict = None
    ):
        self.config_key = config_key
        details = details or {}
        if config_key:
            details["config_key"] = config_key
        super().__init__(
            message=message or "Configuration error",
            platform=None,
            details=details
        )
