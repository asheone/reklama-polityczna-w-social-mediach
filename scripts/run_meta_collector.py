#!/usr/bin/env python3
"""
Local prototype runner for Meta Ad Library collector.

Usage:
    python scripts/run_meta_collector.py --start-date 2024-01-01 --end-date 2024-12-31
    python scripts/run_meta_collector.py --start-date 2024-12-01 --end-date 2024-12-07 --resume
    python scripts/run_meta_collector.py --start-date 2024-01-01 --end-date 2024-01-31 --dry-run

Environment Variables:
    META_ACCESS_TOKEN: Meta API access token (required)
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
import yaml

from collectors.meta.collector import MetaAdCollector
from shared.checkpoint_manager import CheckpointManager
from shared.output_writer import OutputWriter
from shared.logger import setup_logger, get_logger
from shared.exceptions import AuthenticationError, ConfigurationError


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Meta Ad Library collector (prototype)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Fetch one week of data
    python scripts/run_meta_collector.py --start-date 2024-12-01 --end-date 2024-12-07

    # Fetch full year with resume capability
    python scripts/run_meta_collector.py --start-date 2024-01-01 --end-date 2024-12-31 --resume

    # Test without writing output
    python scripts/run_meta_collector.py --start-date 2024-12-01 --end-date 2024-12-07 --dry-run
        """
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--country",
        default="PL",
        help="Country code (default: PL)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint"
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear existing checkpoint before starting"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test mode (fetch and transform, but don't write output)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config file (default: collectors/meta/config.yaml)"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory (default: output)"
    )

    return parser.parse_args()


def load_config(config_path: str = None) -> dict:
    """Load configuration from YAML file."""
    if config_path is None:
        config_path = project_root / "collectors" / "meta" / "config.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        # Return default configuration if file doesn't exist
        return {
            "api": {
                "version": "v19.0",
                "fields": ",".join([
                    "id", "ad_creation_time", "ad_creative_bodies",
                    "ad_creative_link_captions", "ad_creative_link_descriptions",
                    "ad_creative_link_titles", "ad_delivery_start_time",
                    "ad_delivery_stop_time", "ad_snapshot_url", "bylines",
                    "currency", "delivery_by_region", "demographic_distribution",
                    "estimated_audience_size", "eu_total_reach", "impressions",
                    "languages", "page_id", "page_name", "publisher_platforms",
                    "spend", "target_ages", "target_gender", "target_locations"
                ])
            },
            "query": {
                "default_limit": 500,
                "ad_type": "POLITICAL_AND_ISSUE_ADS"
            },
            "rate_limiting": {
                "requests_per_minute": 180,
                "burst_allowance": 5,
                "backoff_multiplier": 2,
                "max_retries": 3,
                "retry_delay_seconds": 60
            },
            "checkpoint": {
                "save_every_n_records": 1000
            }
        }

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    """Main entry point."""
    args = parse_args()

    # Load environment variables from .env file
    load_dotenv()

    # Setup logging
    logger = setup_logger(level=args.log_level)
    logger.info("=" * 60)
    logger.info("Meta Ad Library Collector - Local Prototype")
    logger.info("=" * 60)

    # Load configuration
    try:
        config = load_config(args.config)
        logger.debug(f"Configuration loaded: {config}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1

    # Check for access token
    if not os.getenv("META_ACCESS_TOKEN"):
        logger.error(
            "META_ACCESS_TOKEN not found. Set it in .env file or environment."
        )
        logger.error("See .env.example for reference.")
        return 1

    # Initialize checkpoint manager
    checkpoint_manager = CheckpointManager(
        platform="meta",
        mode="local",
        base_path=".checkpoints"
    )

    # Clear checkpoint if requested
    if args.clear_checkpoint:
        logger.info("Clearing existing checkpoint...")
        checkpoint_manager.clear()

    # Check existing progress
    if not args.resume:
        progress = checkpoint_manager.get_progress()
        if progress["has_checkpoint"]:
            logger.warning(
                f"Existing checkpoint found with {progress['records_processed']} records. "
                "Use --resume to continue or --clear-checkpoint to start fresh."
            )
            # Don't return error, just clear and start fresh
            checkpoint_manager.clear()

    # Initialize output writer
    output_writer = OutputWriter(
        platform="meta",
        mode="local",
        base_path=args.output_dir,
        batch_size=config.get("output", {}).get("batch_size", 10000)
    )

    # Create collector
    try:
        collector = MetaAdCollector(
            config=config,
            checkpoint_manager=checkpoint_manager,
            output_writer=output_writer
        )
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1

    # Authenticate
    logger.info("Testing API credentials...")
    try:
        if not collector.authenticate():
            logger.error("Authentication failed. Check META_ACCESS_TOKEN")
            return 1
        logger.info("Authentication successful")
    except AuthenticationError as e:
        logger.error(f"Authentication failed: {e}")
        return 1

    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        logger.error("Use YYYY-MM-DD format (e.g., 2024-01-15)")
        return 1

    if start_date > end_date:
        logger.error("Start date must be before or equal to end date")
        return 1

    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Country: {args.country}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("-" * 60)

    # Run extraction
    try:
        stats = collector.run(
            start_date=start_date,
            end_date=end_date,
            country_code=args.country,
            dry_run=args.dry_run
        )

        # Print summary
        logger.info("=" * 60)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Records fetched: {stats['records_fetched']}")
        logger.info(f"Records valid: {stats['records_valid']}")
        logger.info(f"Records invalid: {stats['records_invalid']}")

        if stats['records_fetched'] > 0:
            valid_pct = stats['records_valid'] / stats['records_fetched'] * 100
            logger.info(f"Validation rate: {valid_pct:.1f}%")

        if not args.dry_run and "manifest" in stats:
            manifest = stats["manifest"]
            logger.info(f"Output batches: {manifest['total_batches']}")
            logger.info(f"Extraction ID: {manifest['extraction_id']}")

        # Print any validation errors
        if stats.get("validation_errors"):
            logger.warning(
                f"First {len(stats['validation_errors'])} validation errors:"
            )
            for err in stats["validation_errors"][:5]:
                logger.warning(f"  - {err['ad_id']}: {err['error']}")

        return 0

    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user. Checkpoint saved.")
        logger.info("Use --resume to continue from last checkpoint.")
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
