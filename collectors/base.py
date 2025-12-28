"""
Abstract base class for political ad collectors.
All data sources (Meta, Google, TikTok) must implement this interface.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict, Iterator, Optional, Tuple

from shared.checkpoint_manager import CheckpointManager
from shared.output_writer import OutputWriter
from shared.logger import get_logger


class BaseAdCollector(ABC):
    """
    Abstract base class for political ad collectors.

    This interface defines the contract that all platform-specific collectors
    must implement. It ensures consistent behavior across data sources and
    enables the framework to treat all collectors uniformly.

    Implementations must handle:
    - API authentication
    - Data fetching with pagination
    - Transformation to standard schema
    - Validation of required fields

    Standard Schema Fields:
        - ad_id (str, required): Unique identifier for the ad
        - platform (str, required): Platform name ('meta', 'google', 'tiktok')
        - advertiser_name (str, required): Name of the advertiser/page
        - start_date (str, nullable): ISO format start date
        - end_date (str, nullable): ISO format end date
        - spend_min (float, nullable): Minimum spend amount
        - spend_max (float, nullable): Maximum spend amount
        - spend_currency (str, nullable): Currency code (e.g., 'PLN')
        - impressions_min (int, nullable): Minimum impressions
        - impressions_max (int, nullable): Maximum impressions
        - ad_content (str, nullable): Ad text/caption
        - targeting_data (dict, nullable): Targeting information
        - raw_response (dict): Full API response for reference
        - extracted_at (str): ISO timestamp of extraction
    """

    def __init__(
        self,
        config: Dict[str, Any],
        checkpoint_manager: CheckpointManager,
        output_writer: OutputWriter
    ):
        """
        Initialize the collector.

        Args:
            config: Platform-specific configuration dictionary
            checkpoint_manager: Manager for extraction checkpoints
            output_writer: Writer for output data
        """
        self.config = config
        self.checkpoint_manager = checkpoint_manager
        self.output_writer = output_writer
        self.logger = get_logger(self.platform_name)

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """
        Return platform identifier.

        Returns:
            One of: 'meta', 'google', 'tiktok', etc.
        """
        pass

    @abstractmethod
    def authenticate(self) -> bool:
        """
        Verify API credentials are valid.

        Should make a minimal API call to verify credentials work.

        Returns:
            True if authentication successful, False otherwise
        """
        pass

    @abstractmethod
    def fetch_ads(
        self,
        start_date: date,
        end_date: date,
        country_code: str = "PL"
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch ads for the specified date range.

        This method should:
        - Handle pagination internally
        - Respect rate limits
        - Save checkpoints periodically
        - Yield raw API responses

        Args:
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)
            country_code: ISO country code (default: 'PL' for Poland)

        Yields:
            Raw API response dictionaries for each ad
        """
        pass

    @abstractmethod
    def transform_ad(self, raw_ad: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw API response to standard schema.

        The returned dictionary must include these required fields:
        - ad_id (str): Unique identifier
        - platform (str): Platform name
        - advertiser_name (str): Advertiser/page name

        And should include these optional fields when available:
        - funding_entity (str): Who paid for the ad
        - start_date (str): ISO format
        - end_date (str): ISO format
        - spend_min, spend_max (float): Spend range
        - spend_currency (str): Currency code
        - impressions_min, impressions_max (int): Impression range
        - ad_content (str): Ad text/caption
        - targeting_data (dict): Targeting information
        - raw_response (dict): Full API response

        Args:
            raw_ad: Raw API response dictionary

        Returns:
            Transformed dictionary following standard schema
        """
        pass

    @abstractmethod
    def validate_record(
        self,
        record: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a transformed record.

        Check that required fields are present and have valid values.

        Args:
            record: Transformed record dictionary

        Returns:
            Tuple of (is_valid, error_message).
            If valid: (True, None)
            If invalid: (False, "error description")
        """
        pass

    def run(
        self,
        start_date: date,
        end_date: date,
        country_code: str = "PL",
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Run the complete extraction pipeline.

        This method orchestrates the full extraction:
        1. Authenticate with the API
        2. Fetch ads for date range
        3. Transform each ad to standard schema
        4. Validate each record
        5. Write valid records to output
        6. Return summary statistics

        Args:
            start_date: Start of date range
            end_date: End of date range
            country_code: ISO country code
            dry_run: If True, don't write output

        Returns:
            Summary dictionary with statistics
        """
        self.logger.info(f"Starting extraction for {start_date} to {end_date}")

        # Track statistics
        stats = {
            "platform": self.platform_name,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "country_code": country_code,
            "records_fetched": 0,
            "records_valid": 0,
            "records_invalid": 0,
            "validation_errors": []
        }

        try:
            for raw_ad in self.fetch_ads(start_date, end_date, country_code):
                stats["records_fetched"] += 1

                # Transform
                transformed = self.transform_ad(raw_ad)

                # Validate
                is_valid, error = self.validate_record(transformed)

                if not is_valid:
                    stats["records_invalid"] += 1
                    if len(stats["validation_errors"]) < 100:  # Limit stored errors
                        stats["validation_errors"].append({
                            "ad_id": transformed.get("ad_id", "unknown"),
                            "error": error
                        })
                    self.logger.warning(
                        f"Validation error for ad {transformed.get('ad_id')}: {error}"
                    )
                    continue

                stats["records_valid"] += 1

                # Write (unless dry run)
                if not dry_run:
                    self.output_writer.write_record(transformed)

                # Progress logging
                if stats["records_fetched"] % 1000 == 0:
                    self.logger.info(
                        f"Progress: {stats['records_fetched']} fetched, "
                        f"{stats['records_valid']} valid"
                    )

        except KeyboardInterrupt:
            self.logger.warning("Interrupted by user")
            stats["interrupted"] = True
        except Exception as e:
            self.logger.error(f"Extraction failed: {e}", exc_info=True)
            stats["error"] = str(e)
            raise
        finally:
            # Finalize output
            if not dry_run:
                manifest = self.output_writer.finalize()
                stats["manifest"] = manifest

            self.logger.info(
                f"Extraction complete: {stats['records_valid']} valid records "
                f"({stats['records_invalid']} invalid)"
            )

        return stats

    def _validate_required_fields(
        self,
        record: Dict[str, Any],
        required_fields: list
    ) -> Tuple[bool, Optional[str]]:
        """
        Helper to validate required fields are present and non-empty.

        Args:
            record: Record to validate
            required_fields: List of field names that must be present

        Returns:
            Tuple of (is_valid, error_message)
        """
        for field in required_fields:
            value = record.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                return False, f"Missing or empty required field: {field}"
        return True, None
