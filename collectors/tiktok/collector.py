"""
TikTok political ads collector (stub).

This module will implement the BaseAdCollector interface for TikTok political ads.

NOTE: TikTok does not provide a public API for political ads transparency.
This collector is designed for manual data collection workflows:
- CSV import of manually collected data
- Web scraping (where legally permitted)
- TikTok Commercial Content Library (when/if available)

This is a stub implementation. Full implementation TBD based on data source.
"""

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

from collectors.base import BaseAdCollector
from shared.checkpoint_manager import CheckpointManager
from shared.output_writer import OutputWriter
from shared.logger import get_logger


class TikTokManualCollector(BaseAdCollector):
    """
    TikTok manual data collector.

    Processes CSV uploads of manually collected TikTok political ad data.
    Can be extended for other data sources as they become available.

    Expected CSV columns:
    - date: Date observed (YYYY-MM-DD)
    - account_handle: TikTok account username
    - account_name: Display name
    - video_url: URL to the video
    - video_id: TikTok video ID
    - views: Number of views
    - likes: Number of likes
    - comments: Number of comments
    - shares: Number of shares
    - caption: Video caption/description
    - hashtags: Comma-separated hashtags
    - is_sponsored: Whether marked as sponsored content
    - notes: Additional notes
    """

    def __init__(
        self,
        config: Dict[str, Any],
        checkpoint_manager: CheckpointManager,
        output_writer: OutputWriter,
        csv_path: Optional[str] = None
    ):
        """
        Initialize TikTok manual collector.

        Args:
            config: Configuration dictionary
            checkpoint_manager: Checkpoint manager
            output_writer: Output writer
            csv_path: Path to CSV file with collected data
        """
        super().__init__(config, checkpoint_manager, output_writer)
        self.csv_path = csv_path
        self.logger = get_logger("tiktok_collector")

    @property
    def platform_name(self) -> str:
        return "tiktok"

    def authenticate(self) -> bool:
        """
        Validate CSV file exists and has correct schema.

        Returns:
            True if CSV is valid
        """
        if not self.csv_path:
            self.logger.warning("No CSV path provided. Use set_csv_path() first.")
            return True  # No authentication needed for manual input

        csv_file = Path(self.csv_path)
        if not csv_file.exists():
            self.logger.error(f"CSV file not found: {self.csv_path}")
            return False

        # Validate CSV headers
        required_columns = {"date", "account_handle", "video_url"}

        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = set(reader.fieldnames or [])

                missing = required_columns - headers
                if missing:
                    self.logger.error(f"CSV missing required columns: {missing}")
                    return False

            self.logger.info(f"CSV validated: {self.csv_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to validate CSV: {e}")
            return False

    def set_csv_path(self, csv_path: str) -> None:
        """Set the CSV file path."""
        self.csv_path = csv_path

    def fetch_ads(
        self,
        start_date: date,
        end_date: date,
        country_code: str = "PL"
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch ads from CSV file.

        Args:
            start_date: Start of date range
            end_date: End of date range
            country_code: Country code (not used for manual data)

        Yields:
            CSV rows as dictionaries
        """
        if not self.csv_path:
            self.logger.error("No CSV path set. Use set_csv_path() first.")
            return

        self.logger.info(f"Reading TikTok data from: {self.csv_path}")

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                # Filter by date if provided
                row_date_str = row.get("date", "")
                if row_date_str:
                    try:
                        row_date = datetime.strptime(row_date_str, "%Y-%m-%d").date()
                        if row_date < start_date or row_date > end_date:
                            continue
                    except ValueError:
                        self.logger.warning(f"Invalid date format: {row_date_str}")

                yield row

    def transform_ad(self, raw_ad: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform CSV row to standard schema.

        Args:
            raw_ad: CSV row as dictionary

        Returns:
            Transformed dictionary following standard schema
        """
        # Parse numeric fields
        views = self._parse_int(raw_ad.get("views"))
        likes = self._parse_int(raw_ad.get("likes"))
        comments = self._parse_int(raw_ad.get("comments"))
        shares = self._parse_int(raw_ad.get("shares"))

        # Extract video ID from URL if not provided
        video_id = raw_ad.get("video_id") or self._extract_video_id(
            raw_ad.get("video_url", "")
        )

        return {
            # Required fields
            "ad_id": video_id or raw_ad.get("video_url"),
            "platform": "tiktok",
            "advertiser_name": raw_ad.get("account_handle"),

            # Account info
            "account_handle": raw_ad.get("account_handle"),
            "account_name": raw_ad.get("account_name"),

            # Dates
            "start_date": raw_ad.get("date"),
            "end_date": raw_ad.get("date"),

            # Spend (not available for organic content)
            "spend_min": None,
            "spend_max": None,
            "spend_currency": None,

            # Engagement as proxy for impressions
            "impressions_min": views,
            "impressions_max": views,

            # Engagement metrics
            "engagement": {
                "views": views,
                "likes": likes,
                "comments": comments,
                "shares": shares
            },

            # Content
            "ad_content": raw_ad.get("caption"),
            "video_url": raw_ad.get("video_url"),
            "hashtags": self._parse_hashtags(raw_ad.get("hashtags")),

            # Metadata
            "is_sponsored": self._parse_bool(raw_ad.get("is_sponsored")),
            "notes": raw_ad.get("notes"),

            # Targeting (not available for TikTok)
            "targeting_data": None,

            # Raw response
            "raw_response": raw_ad,

            # Metadata
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "collection_method": "manual"
        }

    def validate_record(
        self,
        record: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate TikTok record.

        Args:
            record: Transformed record

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Required: ad_id (video URL or ID) and account handle
        if not record.get("ad_id"):
            return False, "Missing ad_id (video_url or video_id required)"

        if not record.get("advertiser_name"):
            return False, "Missing advertiser_name (account_handle required)"

        return True, None

    def _parse_int(self, value: Any) -> Optional[int]:
        """Parse integer from string, handling various formats."""
        if value is None or value == "":
            return None

        try:
            # Handle comma-separated numbers
            if isinstance(value, str):
                value = value.replace(",", "").strip()

            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _parse_bool(self, value: Any) -> Optional[bool]:
        """Parse boolean from string."""
        if value is None or value == "":
            return None

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            return value.lower() in ("true", "yes", "1", "y")

        return bool(value)

    def _parse_hashtags(self, value: Optional[str]) -> Optional[list]:
        """Parse comma-separated hashtags."""
        if not value:
            return None

        hashtags = [tag.strip() for tag in value.split(",") if tag.strip()]
        return hashtags if hashtags else None

    def _extract_video_id(self, url: str) -> Optional[str]:
        """Extract video ID from TikTok URL."""
        import re

        if not url:
            return None

        # Pattern: https://www.tiktok.com/@username/video/1234567890
        match = re.search(r"/video/(\d+)", url)
        if match:
            return match.group(1)

        return None


class TikTokAPICollector(BaseAdCollector):
    """
    TikTok API collector (placeholder).

    This is a placeholder for when TikTok provides an official API
    for political ad transparency data.

    See: https://www.tiktok.com/transparency/
    """

    def __init__(
        self,
        config: Dict[str, Any],
        checkpoint_manager: CheckpointManager,
        output_writer: OutputWriter
    ):
        super().__init__(config, checkpoint_manager, output_writer)
        self.logger = get_logger("tiktok_api_collector")

    @property
    def platform_name(self) -> str:
        return "tiktok"

    def authenticate(self) -> bool:
        raise NotImplementedError(
            "TikTok API collector not yet implemented. "
            "Use TikTokManualCollector for CSV imports."
        )

    def fetch_ads(
        self,
        start_date: date,
        end_date: date,
        country_code: str = "PL"
    ) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError("TikTok API collector not yet implemented")

    def transform_ad(self, raw_ad: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("TikTok API collector not yet implemented")

    def validate_record(
        self,
        record: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        raise NotImplementedError("TikTok API collector not yet implemented")
