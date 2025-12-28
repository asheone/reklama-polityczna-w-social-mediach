"""
Tests for the Meta Ad Library collector.
"""

import os
import json
import tempfile
from datetime import date
from unittest.mock import Mock, patch, MagicMock
import pytest
import responses

from collectors.meta.collector import MetaAdCollector
from shared.checkpoint_manager import CheckpointManager
from shared.output_writer import OutputWriter
from shared.exceptions import AuthenticationError, RateLimitError, ConfigurationError


class TestMetaAdCollector:
    """Test cases for MetaAdCollector class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def config(self):
        """Default test configuration."""
        return {
            "api": {
                "version": "v19.0",
                "fields": "id,page_name,spend"
            },
            "query": {
                "default_limit": 100,
                "ad_type": "POLITICAL_AND_ISSUE_ADS"
            },
            "rate_limiting": {
                "requests_per_minute": 600,
                "burst_allowance": 10,
                "max_retries": 3
            },
            "checkpoint": {
                "save_every_n_records": 100
            }
        }

    @pytest.fixture
    def collector(self, config, temp_dir):
        """Create a collector instance for testing."""
        checkpoint_manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )
        output_writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        with patch.dict(os.environ, {"META_ACCESS_TOKEN": "test_token"}):
            return MetaAdCollector(
                config=config,
                checkpoint_manager=checkpoint_manager,
                output_writer=output_writer
            )

    def test_init_requires_access_token(self, config, temp_dir):
        """Test that initialization requires access token."""
        checkpoint_manager = CheckpointManager("meta", "local", base_path=temp_dir)
        output_writer = OutputWriter("meta", "local", base_path=temp_dir)

        with patch.dict(os.environ, {}, clear=True):
            # Remove META_ACCESS_TOKEN if exists
            os.environ.pop("META_ACCESS_TOKEN", None)

            with pytest.raises(ConfigurationError) as exc_info:
                MetaAdCollector(
                    config=config,
                    checkpoint_manager=checkpoint_manager,
                    output_writer=output_writer
                )

            assert "META_ACCESS_TOKEN" in str(exc_info.value)

    def test_platform_name(self, collector):
        """Test platform name property."""
        assert collector.platform_name == "meta"

    @responses.activate
    def test_authenticate_success(self, collector):
        """Test successful authentication."""
        responses.add(
            responses.GET,
            "https://graph.facebook.com/v19.0/ads_archive",
            json={"data": []},
            status=200
        )

        assert collector.authenticate() is True

    @responses.activate
    def test_authenticate_invalid_token(self, collector):
        """Test authentication with invalid token."""
        responses.add(
            responses.GET,
            "https://graph.facebook.com/v19.0/ads_archive",
            json={
                "error": {
                    "code": 190,
                    "message": "Invalid OAuth access token"
                }
            },
            status=400
        )

        with pytest.raises(AuthenticationError):
            collector.authenticate()

    def test_transform_ad_basic(self, collector):
        """Test basic ad transformation."""
        raw_ad = {
            "id": "12345",
            "page_name": "Test Page",
            "page_id": "67890",
            "ad_delivery_start_time": "2024-01-15T00:00:00+0000",
            "ad_delivery_stop_time": "2024-01-20T00:00:00+0000",
            "spend": {"lower_bound": "100", "upper_bound": "500"},
            "currency": "PLN",
            "impressions": {"lower_bound": "1000", "upper_bound": "5000"},
            "ad_creative_bodies": ["Test ad content"],
            "bylines": "Paid for by Test Org"
        }

        transformed = collector.transform_ad(raw_ad)

        assert transformed["ad_id"] == "12345"
        assert transformed["platform"] == "meta"
        assert transformed["advertiser_name"] == "Test Page"
        assert transformed["page_id"] == "67890"
        assert transformed["spend_min"] == 100.0
        assert transformed["spend_max"] == 500.0
        assert transformed["spend_currency"] == "PLN"
        assert transformed["impressions_min"] == 1000.0
        assert transformed["impressions_max"] == 5000.0
        assert "Test ad content" in transformed["ad_content"]
        assert transformed["funding_entity"] == "Test Org"
        assert "extracted_at" in transformed
        assert "raw_response" in transformed

    def test_transform_ad_missing_fields(self, collector):
        """Test transformation with missing optional fields."""
        raw_ad = {
            "id": "12345",
            "page_name": "Test Page"
        }

        transformed = collector.transform_ad(raw_ad)

        assert transformed["ad_id"] == "12345"
        assert transformed["advertiser_name"] == "Test Page"
        assert transformed["spend_min"] is None
        assert transformed["spend_max"] is None
        assert transformed["ad_content"] is None

    def test_validate_record_valid(self, collector):
        """Test validation of valid record."""
        record = {
            "ad_id": "12345",
            "platform": "meta",
            "advertiser_name": "Test Page"
        }

        is_valid, error = collector.validate_record(record)
        assert is_valid is True
        assert error is None

    def test_validate_record_missing_ad_id(self, collector):
        """Test validation with missing ad_id."""
        record = {
            "platform": "meta",
            "advertiser_name": "Test Page"
        }

        is_valid, error = collector.validate_record(record)
        assert is_valid is False
        assert "ad_id" in error.lower()

    def test_validate_record_empty_ad_id(self, collector):
        """Test validation with empty ad_id."""
        record = {
            "ad_id": "",
            "platform": "meta",
            "advertiser_name": "Test Page"
        }

        is_valid, error = collector.validate_record(record)
        assert is_valid is False
        assert "empty" in error.lower()

    def test_validate_record_invalid_platform(self, collector):
        """Test validation with wrong platform."""
        record = {
            "ad_id": "12345",
            "platform": "google",
            "advertiser_name": "Test Page"
        }

        is_valid, error = collector.validate_record(record)
        assert is_valid is False
        assert "platform" in error.lower()

    def test_validate_record_invalid_spend(self, collector):
        """Test validation with invalid spend values."""
        record = {
            "ad_id": "12345",
            "platform": "meta",
            "advertiser_name": "Test Page",
            "spend_min": 500,
            "spend_max": 100  # min > max is invalid
        }

        is_valid, error = collector.validate_record(record)
        assert is_valid is False
        assert "spend" in error.lower()

    def test_parse_range_with_bounds(self, collector):
        """Test parsing range with both bounds."""
        result = collector._parse_range({
            "lower_bound": "100",
            "upper_bound": "500"
        })

        assert result["min"] == 100.0
        assert result["max"] == 500.0

    def test_parse_range_lower_only(self, collector):
        """Test parsing range with only lower bound."""
        result = collector._parse_range({
            "lower_bound": "1000"
        })

        assert result["min"] == 1000.0
        assert result["max"] == 1000.0  # Uses lower as max when upper missing

    def test_parse_range_empty(self, collector):
        """Test parsing empty range."""
        result = collector._parse_range(None)

        assert result["min"] is None
        assert result["max"] is None

    def test_extract_ad_content(self, collector):
        """Test ad content extraction."""
        raw_ad = {
            "ad_creative_bodies": ["Main text"],
            "ad_creative_link_titles": ["Title"],
            "ad_creative_link_descriptions": ["Description"]
        }

        content = collector._extract_ad_content(raw_ad)

        assert "Main text" in content
        assert "Title" in content
        assert "Description" in content

    def test_extract_funding_entity(self, collector):
        """Test funding entity extraction."""
        # Standard format
        raw_ad = {"bylines": "Paid for by Test Organization"}
        assert collector._extract_funding_entity(raw_ad) == "Test Organization"

        # Just the entity name
        raw_ad = {"bylines": "Some Entity"}
        assert collector._extract_funding_entity(raw_ad) == "Some Entity"

        # Empty
        raw_ad = {}
        assert collector._extract_funding_entity(raw_ad) is None

    def test_get_stats(self, collector):
        """Test statistics retrieval."""
        stats = collector.get_stats()

        assert stats["platform"] == "meta"
        assert stats["api_version"] == "v19.0"
        assert "rate_limiter" in stats
        assert "checkpoint" in stats

    @responses.activate
    def test_fetch_ads_pagination(self, config, temp_dir):
        """Test fetching ads with pagination."""
        checkpoint_manager = CheckpointManager("meta", "local", base_path=temp_dir)
        output_writer = OutputWriter("meta", "local", base_path=temp_dir)

        with patch.dict(os.environ, {"META_ACCESS_TOKEN": "test_token"}):
            collector = MetaAdCollector(
                config=config,
                checkpoint_manager=checkpoint_manager,
                output_writer=output_writer
            )

        # First page
        responses.add(
            responses.GET,
            "https://graph.facebook.com/v19.0/ads_archive",
            json={
                "data": [{"id": "1"}, {"id": "2"}],
                "paging": {
                    "cursors": {"after": "cursor1"},
                    "next": "https://graph.facebook.com/v19.0/ads_archive?after=cursor1"
                }
            },
            status=200
        )

        # Second page (last)
        responses.add(
            responses.GET,
            "https://graph.facebook.com/v19.0/ads_archive",
            json={
                "data": [{"id": "3"}],
                "paging": {"cursors": {"after": "cursor2"}}
            },
            status=200
        )

        ads = list(collector.fetch_ads(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        ))

        assert len(ads) == 3
        assert ads[0]["id"] == "1"
        assert ads[2]["id"] == "3"
