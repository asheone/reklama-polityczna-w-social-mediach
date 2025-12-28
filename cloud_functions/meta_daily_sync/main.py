"""
GCP Cloud Function for daily Meta ad sync.

Triggered by Cloud Scheduler daily at 02:00 UTC.
Fetches Meta ads for the previous day and writes to GCS.

Environment Variables Required:
- GCS_BUCKET_NAME: Target bucket for output
- GCP_PROJECT_ID: GCP project ID

Secrets (via Secret Manager):
- META_ACCESS_TOKEN: Meta API access token
"""

import os
import json
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple

import functions_framework
from flask import Request

# Import collectors (when deployed, these are in the same package)
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from collectors.meta.collector import MetaAdCollector
from shared.checkpoint_manager import CheckpointManager
from shared.output_writer import OutputWriter
from shared.logger import setup_logger
import yaml


def get_secret(secret_id: str) -> str:
    """
    Fetch secret from GCP Secret Manager.

    Args:
        secret_id: Secret identifier

    Returns:
        Secret value as string
    """
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    project_id = os.environ.get("GCP_PROJECT_ID")

    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID environment variable not set")

    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def load_config() -> Dict[str, Any]:
    """Load configuration for Meta collector."""
    # Try to load from file first
    config_paths = [
        os.path.join(os.path.dirname(__file__), "config.yaml"),
        os.path.join(os.path.dirname(__file__), "..", "..", "collectors", "meta", "config.yaml")
    ]

    for config_path in config_paths:
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                return yaml.safe_load(f)

    # Return default config
    return {
        "api": {"version": "v19.0"},
        "query": {
            "default_limit": 500,
            "ad_type": "POLITICAL_AND_ISSUE_ADS"
        },
        "rate_limiting": {
            "requests_per_minute": 180,
            "burst_allowance": 5,
            "max_retries": 3,
            "retry_delay_seconds": 60
        },
        "checkpoint": {
            "save_every_n_records": 1000
        }
    }


@functions_framework.http
def meta_daily_sync(request: Request) -> Tuple[Dict[str, Any], int]:
    """
    Cloud Function entry point for daily Meta ad sync.

    Fetches Meta ads for yesterday and writes to GCS.

    Args:
        request: HTTP request object

    Returns:
        Tuple of (response dict, status code)
    """
    logger = setup_logger(level="INFO", format_type="json")

    try:
        # Parse request for optional parameters
        request_data = request.get_json(silent=True) or {}
        target_date_str = request_data.get("date")

        # Determine target date
        if target_date_str:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
            logger.info(f"Using requested date: {target_date}")
        else:
            target_date = datetime.utcnow().date() - timedelta(days=1)
            logger.info(f"Using yesterday: {target_date}")

        # Get configuration
        bucket_name = os.environ.get("GCS_BUCKET_NAME", "polish-political-ads")

        # Get access token from Secret Manager
        access_token = get_secret("META_ACCESS_TOKEN")

        # Load config
        config = load_config()

        # Initialize managers (GCS mode for Cloud Functions)
        checkpoint_manager = CheckpointManager(
            platform="meta",
            mode="gcs",
            bucket_name=bucket_name
        )

        output_writer = OutputWriter(
            platform="meta",
            mode="gcs",
            bucket_name=bucket_name,
            batch_size=config.get("output", {}).get("batch_size", 10000)
        )

        # Create collector
        collector = MetaAdCollector(
            config=config,
            checkpoint_manager=checkpoint_manager,
            output_writer=output_writer,
            access_token=access_token
        )

        # Authenticate
        logger.info("Authenticating with Meta API...")
        if not collector.authenticate():
            logger.error("Authentication failed")
            return {
                "status": "error",
                "error": "Authentication failed",
                "date": str(target_date)
            }, 401

        # Run extraction for target date
        logger.info(f"Starting daily sync for {target_date}")

        stats = collector.run(
            start_date=target_date,
            end_date=target_date,
            country_code="PL",
            dry_run=False
        )

        logger.info(
            f"Daily sync complete: {stats['records_valid']} records "
            f"({stats['records_invalid']} invalid)"
        )

        return {
            "status": "success",
            "date": str(target_date),
            "records_processed": stats["records_valid"],
            "records_invalid": stats["records_invalid"],
            "manifest": stats.get("manifest")
        }, 200

    except Exception as e:
        logger.error(f"Daily sync failed: {e}")
        logger.error(traceback.format_exc())

        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }, 500


@functions_framework.http
def meta_backfill(request: Request) -> Tuple[Dict[str, Any], int]:
    """
    Cloud Function for backfilling historical Meta ad data.

    Accepts start_date and end_date parameters for historical collection.

    Args:
        request: HTTP request object with date range

    Returns:
        Tuple of (response dict, status code)
    """
    logger = setup_logger(level="INFO", format_type="json")

    try:
        request_data = request.get_json(silent=True) or {}

        start_date_str = request_data.get("start_date")
        end_date_str = request_data.get("end_date")

        if not start_date_str or not end_date_str:
            return {
                "status": "error",
                "error": "start_date and end_date required"
            }, 400

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

        logger.info(f"Starting backfill from {start_date} to {end_date}")

        # Get configuration
        bucket_name = os.environ.get("GCS_BUCKET_NAME", "polish-political-ads")
        access_token = get_secret("META_ACCESS_TOKEN")
        config = load_config()

        # Initialize managers
        checkpoint_manager = CheckpointManager(
            platform="meta",
            mode="gcs",
            bucket_name=bucket_name
        )

        output_writer = OutputWriter(
            platform="meta",
            mode="gcs",
            bucket_name=bucket_name
        )

        # Create and run collector
        collector = MetaAdCollector(
            config=config,
            checkpoint_manager=checkpoint_manager,
            output_writer=output_writer,
            access_token=access_token
        )

        if not collector.authenticate():
            return {"status": "error", "error": "Authentication failed"}, 401

        stats = collector.run(
            start_date=start_date,
            end_date=end_date,
            country_code="PL"
        )

        return {
            "status": "success",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "records_processed": stats["records_valid"],
            "manifest": stats.get("manifest")
        }, 200

    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        return {
            "status": "error",
            "error": str(e)
        }, 500
