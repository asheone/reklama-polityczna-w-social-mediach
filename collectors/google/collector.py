"""
Google Political Ads Transparency Report collector (stub).

This module will implement the BaseAdCollector interface for Google's
Political Ads Transparency Report, accessed via BigQuery public datasets.

Dataset: bigquery-public-data.google_political_ads
Documentation: https://transparencyreport.google.com/political-ads/

NOTE: This is a stub implementation. Full implementation TBD.
"""

from datetime import date
from typing import Any, Dict, Iterator, Optional, Tuple

from collectors.base import BaseAdCollector
from shared.checkpoint_manager import CheckpointManager
from shared.output_writer import OutputWriter
from shared.logger import get_logger


class GoogleAdsCollector(BaseAdCollector):
    """
    Google Political Ads Transparency Report collector.

    Data source: BigQuery public dataset
    - bigquery-public-data.google_political_ads.creative_stats
    - bigquery-public-data.google_political_ads.advertiser_stats
    - bigquery-public-data.google_political_ads.geo_spend

    Features (planned):
    - Query BigQuery for Polish political ads
    - Join creative and advertiser data
    - Transform to standard schema
    """

    def __init__(
        self,
        config: Dict[str, Any],
        checkpoint_manager: CheckpointManager,
        output_writer: OutputWriter
    ):
        """Initialize Google Ads collector."""
        super().__init__(config, checkpoint_manager, output_writer)
        self.logger = get_logger("google_collector")

        # BigQuery client will be initialized lazily
        self._bq_client = None

    @property
    def platform_name(self) -> str:
        return "google"

    @property
    def bq_client(self):
        """Lazy-load BigQuery client."""
        if self._bq_client is None:
            try:
                from google.cloud import bigquery
                self._bq_client = bigquery.Client()
            except ImportError:
                raise RuntimeError(
                    "google-cloud-bigquery required. "
                    "Install with: pip install google-cloud-bigquery"
                )
        return self._bq_client

    def authenticate(self) -> bool:
        """
        Test BigQuery access.

        Returns:
            True if BigQuery is accessible
        """
        self.logger.info("Testing BigQuery access...")

        try:
            # Simple query to test access
            query = """
                SELECT COUNT(*) as cnt
                FROM `bigquery-public-data.google_political_ads.creative_stats`
                WHERE 'PL' IN UNNEST(regions)
                LIMIT 1
            """
            result = self.bq_client.query(query).result()
            row = next(iter(result))
            self.logger.info(f"BigQuery accessible. Found {row.cnt} Polish ads")
            return True

        except Exception as e:
            self.logger.error(f"BigQuery authentication failed: {e}")
            return False

    def fetch_ads(
        self,
        start_date: date,
        end_date: date,
        country_code: str = "PL"
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch ads from Google Political Ads BigQuery dataset.

        Args:
            start_date: Start of date range
            end_date: End of date range
            country_code: ISO country code

        Yields:
            Raw query result rows as dictionaries
        """
        self.logger.info(
            f"Fetching Google ads from {start_date} to {end_date} for {country_code}"
        )

        query = f"""
            SELECT
                cs.ad_id,
                cs.ad_url,
                cs.ad_type,
                cs.regions,
                cs.advertiser_id,
                cs.advertiser_name,
                cs.date_range_start,
                cs.date_range_end,
                cs.num_of_days,
                cs.impressions,
                cs.spend_usd,
                cs.first_served_timestamp,
                cs.last_served_timestamp,
                cs.age_targeting,
                cs.gender_targeting,
                cs.geo_targeting_included,
                cs.geo_targeting_excluded
            FROM `bigquery-public-data.google_political_ads.creative_stats` cs
            WHERE '{country_code}' IN UNNEST(cs.regions)
              AND cs.date_range_start >= '{start_date.strftime('%Y-%m-%d')}'
              AND cs.date_range_end <= '{end_date.strftime('%Y-%m-%d')}'
            ORDER BY cs.date_range_start
        """

        query_job = self.bq_client.query(query)
        results = query_job.result()

        for row in results:
            yield dict(row)

    def transform_ad(self, raw_ad: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Google BigQuery row to standard schema.

        Args:
            raw_ad: Raw BigQuery row as dictionary

        Returns:
            Transformed dictionary following standard schema
        """
        from datetime import datetime

        # Parse impressions (comes as string range like "≤ 10k" or "10k-100k")
        impressions = self._parse_impressions(raw_ad.get("impressions"))

        return {
            # Required fields
            "ad_id": raw_ad.get("ad_id"),
            "platform": "google",
            "advertiser_name": raw_ad.get("advertiser_name"),

            # Advertiser info
            "advertiser_id": raw_ad.get("advertiser_id"),

            # Dates
            "start_date": str(raw_ad.get("date_range_start")),
            "end_date": str(raw_ad.get("date_range_end")),
            "first_served": str(raw_ad.get("first_served_timestamp")),
            "last_served": str(raw_ad.get("last_served_timestamp")),

            # Spend (Google provides USD)
            "spend_min": raw_ad.get("spend_usd"),
            "spend_max": raw_ad.get("spend_usd"),
            "spend_currency": "USD",

            # Impressions
            "impressions_min": impressions.get("min"),
            "impressions_max": impressions.get("max"),

            # Content
            "ad_content": None,  # Not available in BigQuery
            "ad_url": raw_ad.get("ad_url"),
            "ad_type": raw_ad.get("ad_type"),

            # Targeting
            "targeting_data": {
                "regions": raw_ad.get("regions"),
                "age_targeting": raw_ad.get("age_targeting"),
                "gender_targeting": raw_ad.get("gender_targeting"),
                "geo_targeting_included": raw_ad.get("geo_targeting_included"),
                "geo_targeting_excluded": raw_ad.get("geo_targeting_excluded")
            },

            # Raw response
            "raw_response": raw_ad,

            # Metadata
            "extracted_at": datetime.utcnow().isoformat() + "Z"
        }

    def validate_record(
        self,
        record: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate Google ad record.

        Args:
            record: Transformed record

        Returns:
            Tuple of (is_valid, error_message)
        """
        required_fields = ["ad_id", "platform", "advertiser_name"]
        return self._validate_required_fields(record, required_fields)

    def _parse_impressions(
        self,
        impressions_str: Optional[str]
    ) -> Dict[str, Optional[int]]:
        """
        Parse Google's impression range format.

        Examples:
        - "≤ 10k" -> {"min": 0, "max": 10000}
        - "10k-100k" -> {"min": 10000, "max": 100000}
        - "100k-1M" -> {"min": 100000, "max": 1000000}
        """
        result = {"min": None, "max": None}

        if not impressions_str:
            return result

        # Handle "≤ 10k" format
        if impressions_str.startswith("≤"):
            max_val = self._parse_number(impressions_str[1:].strip())
            result["min"] = 0
            result["max"] = max_val
        # Handle "10k-100k" format
        elif "-" in impressions_str:
            parts = impressions_str.split("-")
            if len(parts) == 2:
                result["min"] = self._parse_number(parts[0])
                result["max"] = self._parse_number(parts[1])
        # Handle "> 10M" format
        elif impressions_str.startswith(">"):
            result["min"] = self._parse_number(impressions_str[1:].strip())

        return result

    def _parse_number(self, num_str: str) -> Optional[int]:
        """Parse number with k/M suffix."""
        if not num_str:
            return None

        num_str = num_str.strip().upper()

        multipliers = {"K": 1000, "M": 1000000, "B": 1000000000}

        for suffix, multiplier in multipliers.items():
            if num_str.endswith(suffix):
                try:
                    return int(float(num_str[:-1]) * multiplier)
                except ValueError:
                    return None

        try:
            return int(float(num_str))
        except ValueError:
            return None
