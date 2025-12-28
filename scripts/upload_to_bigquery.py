#!/usr/bin/env python3
"""
Upload NDJSON output files to Google BigQuery.

Usage:
    python scripts/upload_to_bigquery.py --input-dir output/meta --dataset political_ads
    python scripts/upload_to_bigquery.py --input-dir output/meta --dataset political_ads --table meta_ads
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

from shared.logger import setup_logger


def find_ndjson_files(input_dir: Path) -> List[Path]:
    """Find all NDJSON files in directory."""
    files = []
    for pattern in ["*.ndjson", "*.ndjson.gz"]:
        files.extend(input_dir.glob(pattern))
    return sorted(files)


def load_ndjson_file(file_path: Path) -> List[dict]:
    """Load records from NDJSON file."""
    records = []

    if str(file_path).endswith(".gz"):
        import gzip
        open_func = gzip.open
    else:
        open_func = open

    with open_func(file_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    return records


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Upload NDJSON to BigQuery")
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing NDJSON files"
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="BigQuery dataset name"
    )
    parser.add_argument(
        "--table",
        default=None,
        help="BigQuery table name (default: derived from platform)"
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project ID (default: from GOOGLE_CLOUD_PROJECT env)"
    )
    parser.add_argument(
        "--location",
        default="EU",
        help="BigQuery location (default: EU)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without actually uploading"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level"
    )
    args = parser.parse_args()

    # Load .env
    load_dotenv()

    # Setup logging
    logger = setup_logger(level=args.log_level)

    logger.info("=" * 50)
    logger.info("BigQuery Upload Tool")
    logger.info("=" * 50)

    # Validate input directory
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        return 1

    # Find NDJSON files
    files = find_ndjson_files(input_dir)
    if not files:
        logger.error(f"No NDJSON files found in {input_dir}")
        return 1

    logger.info(f"Found {len(files)} NDJSON files")

    # Derive table name from directory if not specified
    table_name = args.table or f"{input_dir.name}_ads"

    # Get project ID
    project_id = args.project or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT_ID")
    if not project_id:
        logger.error("GCP project ID not found. Set --project or GOOGLE_CLOUD_PROJECT env var.")
        return 1

    if args.dry_run:
        logger.info("DRY RUN - No data will be uploaded")
        logger.info(f"Would upload to: {project_id}.{args.dataset}.{table_name}")

        total_records = 0
        for file_path in files:
            records = load_ndjson_file(file_path)
            total_records += len(records)
            logger.info(f"  {file_path.name}: {len(records)} records")

        logger.info(f"Total records: {total_records}")
        return 0

    # Import BigQuery client
    try:
        from google.cloud import bigquery
    except ImportError:
        logger.error("google-cloud-bigquery not installed")
        logger.error("Run: pip install google-cloud-bigquery")
        return 1

    # Initialize client
    client = bigquery.Client(project=project_id)

    # Create dataset if needed
    dataset_ref = f"{project_id}.{args.dataset}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset exists: {dataset_ref}")
    except Exception:
        logger.info(f"Creating dataset: {dataset_ref}")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = args.location
        client.create_dataset(dataset)

    # Full table reference
    table_ref = f"{dataset_ref}.{table_name}"
    logger.info(f"Target table: {table_ref}")

    # Load all records
    all_records = []
    for file_path in files:
        records = load_ndjson_file(file_path)
        all_records.extend(records)
        logger.info(f"Loaded {len(records)} records from {file_path.name}")

    logger.info(f"Total records to upload: {len(all_records)}")

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Write to temp file and load
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        for record in all_records:
            # Remove raw_response to avoid BigQuery size limits
            record_copy = {k: v for k, v in record.items() if k != "raw_response"}
            f.write(json.dumps(record_copy) + "\n")
        temp_path = f.name

    try:
        with open(temp_path, "rb") as f:
            job = client.load_table_from_file(f, table_ref, job_config=job_config)

        job.result()  # Wait for completion

        logger.info(f"Loaded {job.output_rows} rows to {table_ref}")

    finally:
        os.unlink(temp_path)

    logger.info("Upload complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
