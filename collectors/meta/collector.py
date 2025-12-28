"""
Meta Ad Library API collector for Polish political ads.

This module implements the BaseAdCollector interface for the Meta (Facebook)
Ad Library API, fetching political and issue ads targeted at Poland.

API Documentation:
https://www.facebook.com/ads/library/api/
"""

import os
import re
import time
from datetime import date, datetime
from typing import Any, Dict, Iterator, Optional, Tuple

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential
)

from collectors.base import BaseAdCollector
from shared.checkpoint_manager import CheckpointManager
from shared.output_writer import OutputWriter
from shared.rate_limiter import RateLimiter
from shared.logger import get_logger
from shared.exceptions import (
    AuthenticationError,
    RateLimitError,
    APIError,
    ConfigurationError
)


class MetaAdCollector(BaseAdCollector):
    """
    Meta Ad Library API collector for Polish political ads.

    Features:
    - Fetches political and issue ads from Meta platforms
    - Handles pagination with cursor-based navigation
    - Supports checkpointing for resumable extraction
    - Implements rate limiting to avoid API throttling
    - Transforms data to standard schema
    """

    # API endpoints
    BASE_URL = "https://graph.facebook.com"
    DEFAULT_API_VERSION = "v21.0"  # Latest stable version
    ADS_ARCHIVE_ENDPOINT = "ads_archive"

    # Default fields to fetch
    DEFAULT_FIELDS = [
        "id",
        "ad_creation_time",
        "ad_creative_bodies",
        "ad_creative_link_captions",
        "ad_creative_link_descriptions",
        "ad_creative_link_titles",
        "ad_delivery_start_time",
        "ad_delivery_stop_time",
        "ad_snapshot_url",
        "bylines",
        "currency",
        "delivery_by_region",
        "demographic_distribution",
        "estimated_audience_size",
        "eu_total_reach",
        "impressions",
        "languages",
        "page_id",
        "page_name",
        "publisher_platforms",
        "spend",
        "target_ages",
        "target_gender",
        "target_locations"
    ]

    def __init__(
        self,
        config: Dict[str, Any],
        checkpoint_manager: CheckpointManager,
        output_writer: OutputWriter,
        access_token: Optional[str] = None
    ):
        """
        Initialize the Meta Ad Library collector.

        Args:
            config: Configuration dictionary
            checkpoint_manager: Checkpoint manager instance
            output_writer: Output writer instance
            access_token: Meta API access token (or set META_ACCESS_TOKEN env var)
        """
        super().__init__(config, checkpoint_manager, output_writer)

        # Get access token
        self.access_token = access_token or os.getenv("META_ACCESS_TOKEN")
        if not self.access_token:
            raise ConfigurationError(
                message="META_ACCESS_TOKEN not found in environment or config",
                config_key="META_ACCESS_TOKEN"
            )

        # API configuration
        api_config = config.get("api", {})
        self.api_version = api_config.get("version", self.DEFAULT_API_VERSION)
        self.base_url = f"{self.BASE_URL}/{self.api_version}"
        self.fields = api_config.get("fields", ",".join(self.DEFAULT_FIELDS))

        # Query defaults
        query_config = config.get("query", {})
        self.default_limit = query_config.get("default_limit", 500)
        self.ad_type = query_config.get("ad_type", "POLITICAL_AND_ISSUE_ADS")

        # Rate limiting
        rate_config = config.get("rate_limiting", {})
        self.rate_limiter = RateLimiter(
            requests_per_minute=rate_config.get("requests_per_minute", 180),
            burst=rate_config.get("burst_allowance", 5),
            backoff_multiplier=rate_config.get("backoff_multiplier", 2.0),
            max_retries=rate_config.get("max_retries", 3),
            initial_retry_delay=rate_config.get("retry_delay_seconds", 60.0)
        )

        # Checkpointing
        checkpoint_config = config.get("checkpoint", {})
        self.checkpoint_interval = checkpoint_config.get("save_every_n_records", 1000)

        # Statistics
        self._request_count = 0
        self._records_since_checkpoint = 0

        self.logger = get_logger("meta_collector")

    @property
    def platform_name(self) -> str:
        return "meta"

    def authenticate(self) -> bool:
        """
        Test API credentials with a minimal query.

        Returns:
            True if credentials are valid
        """
        try:
            self.logger.info("Testing Meta API credentials...")

            # Log token info (masked) for debugging
            token_preview = self.access_token[:10] + "..." if len(self.access_token) > 10 else "***"
            self.logger.debug(f"Using token: {token_preview} (length: {len(self.access_token)})")

            url = f"{self.base_url}/{self.ADS_ARCHIVE_ENDPOINT}"
            params = {
                "access_token": self.access_token,
                "ad_reached_countries": "PL",
                "ad_type": self.ad_type,
                "search_terms": "",  # Required parameter - empty string returns all ads
                "fields": "id",
                "limit": 1
            }

            self.logger.debug(f"Request URL: {url}")
            response = requests.get(url, params=params, timeout=30)

            self.logger.debug(f"Response status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                ad_count = len(data.get("data", []))
                self.logger.info(f"Meta API credentials validated successfully (found {ad_count} test ads)")
                return True

            # Try to parse error response
            try:
                response_json = response.json()
                self.logger.debug(f"Error response: {response_json}")
            except ValueError:
                self.logger.error(f"Non-JSON error response: {response.text[:500]}")
                raise AuthenticationError(
                    message=f"API returned non-JSON response (HTTP {response.status_code}): {response.text[:200]}",
                    platform="meta"
                )

            # Handle specific error cases
            error_data = response_json.get("error", {})
            error_code = error_data.get("code")
            error_subcode = error_data.get("error_subcode")
            error_message = error_data.get("message", "Unknown error")
            error_type = error_data.get("type", "Unknown")

            self.logger.error(f"API Error - Code: {error_code}, Subcode: {error_subcode}, Type: {error_type}")
            self.logger.error(f"Error message: {error_message}")

            # Provide helpful messages for common errors
            if error_code == 190:
                if error_subcode == 463:
                    raise AuthenticationError(
                        message="Access token has expired. Generate a new token at https://developers.facebook.com/tools/explorer/",
                        platform="meta",
                        details={"error_code": error_code, "error_subcode": error_subcode}
                    )
                elif error_subcode == 460:
                    raise AuthenticationError(
                        message="Password changed. Generate a new token at https://developers.facebook.com/tools/explorer/",
                        platform="meta",
                        details={"error_code": error_code, "error_subcode": error_subcode}
                    )
                else:
                    raise AuthenticationError(
                        message=f"Invalid access token: {error_message}",
                        platform="meta",
                        details={"error_code": error_code, "error_subcode": error_subcode}
                    )
            elif error_code == 4:
                raise RateLimitError(
                    message="Rate limit hit during authentication test. Wait a few minutes and try again.",
                    platform="meta"
                )
            elif error_code == 10:
                raise AuthenticationError(
                    message="Permission denied. Ensure your app has 'ads_read' permission and is approved for Marketing API.",
                    platform="meta",
                    details={"error_code": error_code}
                )
            elif error_code == 100:
                raise AuthenticationError(
                    message=f"Invalid parameter: {error_message}. Check API version and field names.",
                    platform="meta",
                    details={"error_code": error_code}
                )
            elif error_code == 200:
                raise AuthenticationError(
                    message="Permission error. Your app may not have access to the Ad Library API.",
                    platform="meta",
                    details={"error_code": error_code}
                )
            elif error_code == 1:
                # Error code 1 is a generic "unknown error" - often related to:
                # - Temporary API issues
                # - Missing required parameters
                # - Token format issues
                raise AuthenticationError(
                    message=(
                        "Meta API returned 'unknown error' (code 1). This usually means:\n"
                        "  1. Temporary API issue - try again in a few minutes\n"
                        "  2. Token needs 'ads_read' permission - regenerate at Graph API Explorer\n"
                        "  3. App needs Marketing API access - check app settings\n"
                        "\n"
                        "To get a working token:\n"
                        "  1. Go to https://developers.facebook.com/tools/explorer/\n"
                        "  2. Select your app (or 'Meta App' for testing)\n"
                        "  3. Click 'Add Permission' → 'Ads Management' → select 'ads_read'\n"
                        "  4. Click 'Generate Access Token'\n"
                        "  5. Copy the token to your .env file"
                    ),
                    platform="meta",
                    details={"error_code": error_code, "error_type": error_type}
                )
            else:
                raise AuthenticationError(
                    message=f"Authentication failed (code {error_code}): {error_message}",
                    platform="meta",
                    details={
                        "error_code": error_code,
                        "error_subcode": error_subcode,
                        "error_type": error_type,
                        "full_error": error_data
                    }
                )

        except requests.RequestException as e:
            self.logger.error(f"Network error during authentication: {e}")
            raise AuthenticationError(
                message=f"Network error: {e}",
                platform="meta"
            )
        except ValueError as e:
            self.logger.error(f"JSON parsing error: {e}")
            raise AuthenticationError(
                message=f"Failed to parse API response: {e}",
                platform="meta"
            )

    def fetch_ads(
        self,
        start_date: date,
        end_date: date,
        country_code: str = "PL"
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch all political ads in the specified date range.

        Handles pagination, rate limiting, and checkpointing automatically.

        Args:
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)
            country_code: ISO country code

        Yields:
            Raw API response dictionaries for each ad
        """
        self.logger.info(
            f"Fetching ads from {start_date} to {end_date} for {country_code}"
        )

        # Build initial parameters
        params = {
            "access_token": self.access_token,
            "ad_reached_countries": country_code,
            "ad_type": self.ad_type,
            "ad_active_status": "ALL",
            "ad_delivery_date_min": start_date.strftime("%Y-%m-%d"),
            "ad_delivery_date_max": end_date.strftime("%Y-%m-%d"),
            "search_terms": "",  # Required - empty string returns all political ads
            "fields": self.fields,
            "limit": self.default_limit
        }

        # Check for existing checkpoint
        cursor = self.checkpoint_manager.get_cursor()
        if cursor:
            params["after"] = cursor
            self.logger.info(f"Resuming from checkpoint cursor: {cursor[:20]}...")

        url = f"{self.base_url}/{self.ADS_ARCHIVE_ENDPOINT}"
        page_count = 0
        total_ads = 0

        while True:
            # Rate limiting
            self.rate_limiter.wait_if_needed()

            # Make request with retry logic
            try:
                response_data = self._make_request(url, params)
            except RateLimitError:
                # Rate limit handled by retry logic in _make_request
                continue
            except APIError as e:
                self.logger.error(f"API error: {e}")
                raise

            page_count += 1
            ads = response_data.get("data", [])
            ads_in_page = len(ads)
            total_ads += ads_in_page

            self.logger.debug(
                f"Page {page_count}: {ads_in_page} ads (total: {total_ads})"
            )

            # Yield each ad
            for ad in ads:
                yield ad

                # Update checkpoint periodically
                self._records_since_checkpoint += 1
                if self._records_since_checkpoint >= self.checkpoint_interval:
                    cursor = response_data.get("paging", {}).get("cursors", {}).get("after")
                    if cursor:
                        self.checkpoint_manager.save_cursor(
                            cursor,
                            records_in_batch=self._records_since_checkpoint,
                            additional_data={
                                "total_fetched": total_ads,
                                "pages_processed": page_count,
                                "date_range": f"{start_date} to {end_date}"
                            }
                        )
                        self._records_since_checkpoint = 0

            # Check for next page
            paging = response_data.get("paging", {})
            if "next" not in paging:
                self.logger.info(
                    f"Reached end of results. Total: {total_ads} ads in {page_count} pages"
                )
                break

            # Update params for next page
            next_cursor = paging.get("cursors", {}).get("after")
            if next_cursor:
                params["after"] = next_cursor
            else:
                break

            # Log progress periodically
            if page_count % 10 == 0:
                stats = self.rate_limiter.get_stats()
                self.logger.info(
                    f"Progress: {total_ads} ads fetched, "
                    f"rate limit: {stats['requests_last_minute']}/{stats['requests_per_minute_limit']} rpm"
                )

        # Final checkpoint
        self.checkpoint_manager.update_progress(
            records_processed=total_ads,
            additional_data={
                "status": "completed",
                "pages_processed": page_count,
                "date_range": f"{start_date} to {end_date}"
            }
        )

    @retry(
        retry=retry_if_exception_type((requests.RequestException, RateLimitError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    def _make_request(
        self,
        url: str,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Make an API request with retry logic.

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            Response JSON data

        Raises:
            APIError: For non-retryable API errors
            RateLimitError: For rate limit errors (will be retried)
        """
        self._request_count += 1

        try:
            response = requests.get(url, params=params, timeout=60)

            if response.status_code == 200:
                return response.json()

            # Parse error response
            try:
                error_data = response.json().get("error", {})
            except ValueError:
                error_data = {}

            error_code = error_data.get("code")
            error_message = error_data.get("message", f"HTTP {response.status_code}")

            # Handle rate limiting (code 4 or 17)
            if error_code in (4, 17) or response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                self.rate_limiter.handle_rate_limit_error(retry_after=retry_after)
                raise RateLimitError(
                    message=error_message,
                    platform="meta",
                    retry_after=retry_after
                )

            # Handle other errors
            raise APIError(
                message=error_message,
                platform="meta",
                status_code=response.status_code,
                response_body=response.text[:500],
                details={"error_code": error_code}
            )

        except requests.Timeout:
            self.logger.warning("Request timed out, will retry")
            raise
        except requests.RequestException as e:
            self.logger.warning(f"Request failed: {e}, will retry")
            raise

    def transform_ad(self, raw_ad: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Meta API response to standard schema.

        Args:
            raw_ad: Raw API response dictionary

        Returns:
            Transformed dictionary following standard schema
        """
        # Parse spend range: "100 - 500" or "<100" or ">5000"
        spend_data = self._parse_range(
            raw_ad.get("spend", {}),
            value_type="spend"
        )

        # Parse impressions range
        impressions_data = self._parse_range(
            raw_ad.get("impressions", {}),
            value_type="impressions"
        )

        # Extract ad text content
        ad_content = self._extract_ad_content(raw_ad)

        # Build targeting data
        targeting_data = self._build_targeting_data(raw_ad)

        return {
            # Required fields
            "ad_id": raw_ad.get("id"),
            "platform": "meta",
            "advertiser_name": raw_ad.get("page_name"),

            # Page/advertiser info
            "page_id": raw_ad.get("page_id"),
            "funding_entity": self._extract_funding_entity(raw_ad),

            # Dates
            "ad_creation_time": raw_ad.get("ad_creation_time"),
            "start_date": raw_ad.get("ad_delivery_start_time"),
            "end_date": raw_ad.get("ad_delivery_stop_time"),

            # Spend
            "spend_min": spend_data["min"],
            "spend_max": spend_data["max"],
            "spend_currency": raw_ad.get("currency"),

            # Impressions/Reach
            "impressions_min": impressions_data["min"],
            "impressions_max": impressions_data["max"],
            "eu_total_reach": raw_ad.get("eu_total_reach"),
            "estimated_audience_size": self._parse_audience_size(
                raw_ad.get("estimated_audience_size")
            ),

            # Content
            "ad_content": ad_content,
            "ad_snapshot_url": raw_ad.get("ad_snapshot_url"),
            "languages": raw_ad.get("languages"),

            # Targeting
            "targeting_data": targeting_data,

            # Platform info
            "publisher_platforms": raw_ad.get("publisher_platforms"),

            # Raw response for reference
            "raw_response": raw_ad,

            # Metadata
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "api_version": self.api_version
        }

    def validate_record(
        self,
        record: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a transformed record.

        Args:
            record: Transformed record dictionary

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check required fields
        required_fields = ["ad_id", "platform", "advertiser_name"]
        is_valid, error = self._validate_required_fields(record, required_fields)
        if not is_valid:
            return False, error

        # Validate ad_id format (should be numeric string)
        ad_id = record.get("ad_id", "")
        if not ad_id or not str(ad_id).strip():
            return False, "ad_id is empty"

        # Validate platform
        if record.get("platform") != "meta":
            return False, f"Invalid platform: {record.get('platform')}"

        # Validate dates if present
        for date_field in ["start_date", "end_date", "ad_creation_time"]:
            date_value = record.get(date_field)
            if date_value:
                try:
                    # Meta uses ISO format
                    datetime.fromisoformat(date_value.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    return False, f"Invalid date format for {date_field}: {date_value}"

        # Validate spend values if present
        spend_min = record.get("spend_min")
        spend_max = record.get("spend_max")
        if spend_min is not None and spend_max is not None:
            if spend_min > spend_max:
                return False, f"spend_min ({spend_min}) > spend_max ({spend_max})"

        return True, None

    def _parse_range(
        self,
        range_data: Any,
        value_type: str = "generic"
    ) -> Dict[str, Optional[float]]:
        """
        Parse range data from Meta API.

        Meta returns ranges like:
        - {"lower_bound": "100", "upper_bound": "500"}
        - {"lower_bound": "5000"} (no upper bound)

        Args:
            range_data: Range dictionary or None
            value_type: Type of value for logging

        Returns:
            Dictionary with 'min' and 'max' keys
        """
        result = {"min": None, "max": None}

        if not range_data or not isinstance(range_data, dict):
            return result

        try:
            lower = range_data.get("lower_bound")
            upper = range_data.get("upper_bound")

            if lower is not None:
                result["min"] = float(str(lower).replace(",", ""))

            if upper is not None:
                result["max"] = float(str(upper).replace(",", ""))
            elif lower is not None:
                # If no upper bound, use lower as both
                result["max"] = result["min"]

        except (ValueError, TypeError) as e:
            self.logger.debug(f"Failed to parse {value_type} range: {range_data} - {e}")

        return result

    def _extract_ad_content(self, raw_ad: Dict[str, Any]) -> Optional[str]:
        """
        Extract text content from ad creative fields.

        Args:
            raw_ad: Raw API response

        Returns:
            Combined ad text content or None
        """
        content_parts = []

        # Ad creative bodies (main text)
        bodies = raw_ad.get("ad_creative_bodies", [])
        if bodies:
            content_parts.extend(bodies)

        # Link titles
        titles = raw_ad.get("ad_creative_link_titles", [])
        if titles:
            content_parts.extend(titles)

        # Link descriptions
        descriptions = raw_ad.get("ad_creative_link_descriptions", [])
        if descriptions:
            content_parts.extend(descriptions)

        # Link captions
        captions = raw_ad.get("ad_creative_link_captions", [])
        if captions:
            content_parts.extend(captions)

        if content_parts:
            # Join with newlines, remove empty strings
            return "\n".join(part for part in content_parts if part and part.strip())

        return None

    def _extract_funding_entity(self, raw_ad: Dict[str, Any]) -> Optional[str]:
        """
        Extract funding entity from bylines.

        Args:
            raw_ad: Raw API response

        Returns:
            Funding entity name or None
        """
        bylines = raw_ad.get("bylines")
        if bylines and isinstance(bylines, str):
            # Format: "Paid for by [Entity Name]"
            match = re.search(r"Paid for by (.+)", bylines, re.IGNORECASE)
            if match:
                return match.group(1).strip()
            return bylines
        return None

    def _build_targeting_data(self, raw_ad: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build targeting data object from raw ad.

        Args:
            raw_ad: Raw API response

        Returns:
            Targeting data dictionary
        """
        targeting = {}

        # Demographics
        demo = raw_ad.get("demographic_distribution")
        if demo:
            targeting["demographic_distribution"] = demo

        # Regional distribution
        region = raw_ad.get("delivery_by_region")
        if region:
            targeting["region_distribution"] = region

        # Target ages
        ages = raw_ad.get("target_ages")
        if ages:
            targeting["target_ages"] = ages

        # Target gender
        gender = raw_ad.get("target_gender")
        if gender:
            targeting["target_gender"] = gender

        # Target locations
        locations = raw_ad.get("target_locations")
        if locations:
            targeting["target_locations"] = locations

        return targeting if targeting else None

    def _parse_audience_size(
        self,
        audience_data: Any
    ) -> Optional[Dict[str, int]]:
        """
        Parse estimated audience size.

        Args:
            audience_data: Audience size data from API

        Returns:
            Dictionary with min/max or None
        """
        if not audience_data or not isinstance(audience_data, dict):
            return None

        try:
            return {
                "min": int(audience_data.get("lower_bound", 0)),
                "max": int(audience_data.get("upper_bound", 0))
            }
        except (ValueError, TypeError):
            return None

    def get_stats(self) -> Dict[str, Any]:
        """
        Get collector statistics.

        Returns:
            Statistics dictionary
        """
        rate_stats = self.rate_limiter.get_stats()
        checkpoint_progress = self.checkpoint_manager.get_progress()

        return {
            "platform": self.platform_name,
            "api_version": self.api_version,
            "total_requests": self._request_count,
            "rate_limiter": rate_stats,
            "checkpoint": checkpoint_progress
        }
