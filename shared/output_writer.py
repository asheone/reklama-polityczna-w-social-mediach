"""
Output writer for NDJSON data files.
Supports both local filesystem and Google Cloud Storage.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from shared.logger import get_logger
from shared.exceptions import OutputError


class OutputWriter:
    """
    Write NDJSON (newline-delimited JSON) output to local filesystem or GCS.

    Features:
    - Automatic batching for memory efficiency
    - Supports local files and GCS
    - Creates extraction manifests
    - Handles compression (optional)
    """

    def __init__(
        self,
        platform: str,
        mode: str = "local",
        bucket_name: Optional[str] = None,
        batch_size: int = 10000,
        base_path: str = "output",
        compress: bool = False
    ):
        """
        Initialize the output writer.

        Args:
            platform: Platform identifier ('meta', 'google', 'tiktok')
            mode: Storage mode ('local' or 'gcs')
            bucket_name: GCS bucket name (required if mode='gcs')
            batch_size: Number of records per batch file
            base_path: Base path for local output
            compress: Whether to compress output files
        """
        self.platform = platform
        self.mode = mode
        self.bucket_name = bucket_name
        self.batch_size = batch_size
        self.base_path = base_path
        self.compress = compress
        self.logger = get_logger("output_writer")

        self._gcs_client = None

        # State
        self.current_batch: List[Dict[str, Any]] = []
        self.batch_count = 0
        self.total_records = 0
        self.extraction_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.batch_files: List[str] = []

        if mode == "local":
            self.output_dir = os.path.join(base_path, platform)
            os.makedirs(self.output_dir, exist_ok=True)
        elif mode == "gcs":
            if not bucket_name:
                raise OutputError(
                    message="bucket_name required for GCS mode",
                    platform=platform
                )
            self.output_dir = platform
        else:
            raise OutputError(
                message=f"Invalid mode: {mode}. Use 'local' or 'gcs'",
                platform=platform
            )

    @property
    def gcs_client(self):
        """Lazy-load GCS client."""
        if self._gcs_client is None and self.mode == "gcs":
            try:
                from google.cloud import storage
                self._gcs_client = storage.Client()
            except ImportError:
                raise OutputError(
                    message="google-cloud-storage package required for GCS mode",
                    platform=self.platform
                )
        return self._gcs_client

    def write_record(self, record: Dict[str, Any]) -> None:
        """
        Add a record to the current batch.

        Args:
            record: Data record to write
        """
        self.current_batch.append(record)
        self.total_records += 1

        # Flush batch if full
        if len(self.current_batch) >= self.batch_size:
            self.flush_batch()

    def write_records(self, records: List[Dict[str, Any]]) -> None:
        """
        Add multiple records to the current batch.

        Args:
            records: List of data records to write
        """
        for record in records:
            self.write_record(record)

    def flush_batch(self) -> Optional[str]:
        """
        Write current batch to storage.

        Returns:
            Filename of written batch or None if batch was empty
        """
        if not self.current_batch:
            return None

        self.batch_count += 1
        filename = (
            f"{self.platform}_ads_{self.extraction_id}_"
            f"batch_{self.batch_count:04d}.ndjson"
        )

        if self.compress:
            filename += ".gz"

        # Convert to NDJSON (newline-delimited JSON)
        ndjson_content = "\n".join(
            json.dumps(record, ensure_ascii=False) for record in self.current_batch
        )

        try:
            if self.mode == "local":
                filepath = os.path.join(self.output_dir, filename)

                if self.compress:
                    import gzip
                    with gzip.open(filepath, "wt", encoding="utf-8") as f:
                        f.write(ndjson_content)
                else:
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(ndjson_content)

                self.logger.info(
                    f"Batch {self.batch_count} written: {filepath} "
                    f"({len(self.current_batch)} records)"
                )

            elif self.mode == "gcs":
                bucket = self.gcs_client.bucket(self.bucket_name)
                blob_path = f"{self.output_dir}/{filename}"
                blob = bucket.blob(blob_path)

                if self.compress:
                    import gzip
                    compressed = gzip.compress(ndjson_content.encode("utf-8"))
                    blob.upload_from_string(
                        compressed,
                        content_type="application/gzip"
                    )
                else:
                    blob.upload_from_string(
                        ndjson_content,
                        content_type="application/x-ndjson"
                    )

                self.logger.info(
                    f"Batch {self.batch_count} uploaded to GCS: {blob_path} "
                    f"({len(self.current_batch)} records)"
                )

            self.batch_files.append(filename)

        except Exception as e:
            self.logger.error(f"Failed to write batch: {e}")
            raise OutputError(
                message=f"Failed to write batch: {e}",
                platform=self.platform,
                output_path=filename
            )

        # Clear batch
        records_in_batch = len(self.current_batch)
        self.current_batch = []

        return filename

    def finalize(self) -> Dict[str, Any]:
        """
        Flush final batch and create extraction manifest.

        Returns:
            Manifest dictionary with extraction metadata
        """
        # Flush any remaining records
        self.flush_batch()

        # Create manifest
        manifest = {
            "extraction_id": self.extraction_id,
            "platform": self.platform,
            "total_records": self.total_records,
            "total_batches": self.batch_count,
            "batch_files": self.batch_files,
            "extraction_started": self.extraction_id,
            "extraction_completed": datetime.utcnow().isoformat() + "Z",
            "mode": self.mode,
            "compressed": self.compress
        }

        manifest_filename = (
            f"{self.platform}_ads_{self.extraction_id}_manifest.json"
        )

        try:
            if self.mode == "local":
                manifest_path = os.path.join(self.output_dir, manifest_filename)
                with open(manifest_path, "w", encoding="utf-8") as f:
                    json.dump(manifest, f, indent=2)
                self.logger.info(f"Manifest created: {manifest_path}")

            elif self.mode == "gcs":
                bucket = self.gcs_client.bucket(self.bucket_name)
                blob_path = f"{self.output_dir}/{manifest_filename}"
                blob = bucket.blob(blob_path)
                blob.upload_from_string(
                    json.dumps(manifest, indent=2),
                    content_type="application/json"
                )
                self.logger.info(f"Manifest uploaded to GCS: {blob_path}")

        except Exception as e:
            self.logger.error(f"Failed to write manifest: {e}")
            raise OutputError(
                message=f"Failed to write manifest: {e}",
                platform=self.platform,
                output_path=manifest_filename
            )

        return manifest

    def get_stats(self) -> Dict[str, Any]:
        """
        Get current output statistics.

        Returns:
            Statistics dictionary
        """
        return {
            "extraction_id": self.extraction_id,
            "platform": self.platform,
            "total_records": self.total_records,
            "batches_written": self.batch_count,
            "records_in_current_batch": len(self.current_batch),
            "batch_size": self.batch_size,
            "mode": self.mode
        }
