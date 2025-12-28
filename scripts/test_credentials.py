#!/usr/bin/env python3
"""
Test API credentials for all configured collectors.

Usage:
    python scripts/test_credentials.py
    python scripts/test_credentials.py --platform meta
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
import yaml

from shared.logger import setup_logger
from shared.checkpoint_manager import CheckpointManager
from shared.output_writer import OutputWriter


def test_meta_credentials(logger) -> bool:
    """Test Meta Ad Library API credentials."""
    logger.info("-" * 40)
    logger.info("Testing Meta Ad Library API credentials")
    logger.info("-" * 40)

    access_token = os.getenv("META_ACCESS_TOKEN")
    if not access_token:
        logger.error("META_ACCESS_TOKEN not found in environment")
        logger.error("Set it in .env file or as environment variable")
        return False

    # Mask token for logging
    masked_token = access_token[:10] + "..." + access_token[-5:] if len(access_token) > 20 else "***"
    logger.info(f"Token found: {masked_token}")

    try:
        from collectors.meta.collector import MetaAdCollector

        # Load config
        config_path = project_root / "collectors" / "meta" / "config.yaml"
        if config_path.exists():
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
        else:
            config = {
                "api": {"version": "v19.0"},
                "query": {"default_limit": 1},
                "rate_limiting": {"requests_per_minute": 180}
            }

        # Create minimal checkpoint and output managers
        checkpoint_manager = CheckpointManager(platform="meta", mode="local")
        output_writer = OutputWriter(platform="meta", mode="local")

        # Create collector and test
        collector = MetaAdCollector(
            config=config,
            checkpoint_manager=checkpoint_manager,
            output_writer=output_writer
        )

        if collector.authenticate():
            logger.info("Meta API credentials: VALID")
            return True
        else:
            logger.error("Meta API credentials: INVALID")
            return False

    except Exception as e:
        logger.error(f"Meta API credentials: FAILED - {e}")
        return False


def test_gcp_credentials(logger) -> bool:
    """Test Google Cloud Platform credentials."""
    logger.info("-" * 40)
    logger.info("Testing Google Cloud Platform credentials")
    logger.info("-" * 40)

    # Check for credentials file or default credentials
    creds_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_file:
        if os.path.exists(creds_file):
            logger.info(f"Credentials file found: {creds_file}")
        else:
            logger.warning(f"Credentials file not found: {creds_file}")

    try:
        from google.cloud import storage
        client = storage.Client()
        # Try to list buckets (just to verify credentials work)
        _ = list(client.list_buckets(max_results=1))
        logger.info("GCP credentials: VALID")
        return True
    except ImportError:
        logger.warning("google-cloud-storage not installed")
        logger.info("Run: pip install google-cloud-storage")
        return False
    except Exception as e:
        logger.warning(f"GCP credentials: NOT CONFIGURED - {e}")
        logger.info("This is OK for local development mode")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Test API credentials")
    parser.add_argument(
        "--platform",
        choices=["meta", "gcp", "all"],
        default="all",
        help="Platform to test (default: all)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level"
    )
    args = parser.parse_args()

    # Load .env file
    load_dotenv()

    # Setup logging
    logger = setup_logger(level=args.log_level)

    logger.info("=" * 50)
    logger.info("Credential Testing Tool")
    logger.info("=" * 50)

    results = {}

    if args.platform in ("meta", "all"):
        results["meta"] = test_meta_credentials(logger)

    if args.platform in ("gcp", "all"):
        results["gcp"] = test_gcp_credentials(logger)

    # Summary
    logger.info("=" * 50)
    logger.info("SUMMARY")
    logger.info("=" * 50)

    all_passed = True
    for platform, passed in results.items():
        status = "PASS" if passed else "FAIL"
        logger.info(f"  {platform}: {status}")
        if not passed and platform == "meta":
            all_passed = False

    if all_passed:
        logger.info("\nAll required credentials are valid!")
        return 0
    else:
        logger.error("\nSome credentials are missing or invalid.")
        logger.info("\nSee README.md for setup instructions.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
