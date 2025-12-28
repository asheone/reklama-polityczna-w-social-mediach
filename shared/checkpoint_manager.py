"""
Checkpoint manager for tracking extraction progress.
Supports both local filesystem and Google Cloud Storage.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

from shared.logger import get_logger
from shared.exceptions import CheckpointError


class CheckpointManager:
    """
    Manage extraction checkpoints for resumable data collection.

    Features:
    - Supports local filesystem (development) and GCS (production)
    - Tracks pagination cursors for API resumption
    - Records progress statistics
    - Handles failures gracefully
    """

    def __init__(
        self,
        platform: str,
        mode: str = "local",
        bucket_name: Optional[str] = None,
        base_path: str = ".checkpoints"
    ):
        """
        Initialize the checkpoint manager.

        Args:
            platform: Platform identifier ('meta', 'google', 'tiktok')
            mode: Storage mode ('local' or 'gcs')
            bucket_name: GCS bucket name (required if mode='gcs')
            base_path: Base path for local checkpoints
        """
        self.platform = platform
        self.mode = mode
        self.bucket_name = bucket_name
        self.base_path = base_path
        self.logger = get_logger("checkpoint_manager")

        self._gcs_client = None

        if mode == "local":
            self.checkpoint_path = os.path.join(
                base_path, f"{platform}_checkpoint.json"
            )
            os.makedirs(base_path, exist_ok=True)
        elif mode == "gcs":
            if not bucket_name:
                raise CheckpointError(
                    message="bucket_name required for GCS mode",
                    platform=platform,
                    operation="init"
                )
            self.checkpoint_path = f"checkpoints/{platform}_checkpoint.json"
        else:
            raise CheckpointError(
                message=f"Invalid mode: {mode}. Use 'local' or 'gcs'",
                platform=platform,
                operation="init"
            )

    @property
    def gcs_client(self):
        """Lazy-load GCS client."""
        if self._gcs_client is None and self.mode == "gcs":
            try:
                from google.cloud import storage
                self._gcs_client = storage.Client()
            except ImportError:
                raise CheckpointError(
                    message="google-cloud-storage package required for GCS mode",
                    platform=self.platform,
                    operation="init"
                )
        return self._gcs_client

    def load(self) -> Optional[Dict[str, Any]]:
        """
        Load checkpoint from storage.

        Returns:
            Checkpoint data dictionary or None if not found
        """
        try:
            if self.mode == "local":
                if os.path.exists(self.checkpoint_path):
                    with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        self.logger.debug(
                            f"Checkpoint loaded: {data.get('records_processed', 0)} records"
                        )
                        return data
            elif self.mode == "gcs":
                bucket = self.gcs_client.bucket(self.bucket_name)
                blob = bucket.blob(self.checkpoint_path)
                if blob.exists():
                    data = json.loads(blob.download_as_text())
                    self.logger.debug(
                        f"Checkpoint loaded from GCS: {data.get('records_processed', 0)} records"
                    )
                    return data
        except json.JSONDecodeError as e:
            self.logger.warning(f"Corrupted checkpoint file, starting fresh: {e}")
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            raise CheckpointError(
                message=f"Failed to load checkpoint: {e}",
                platform=self.platform,
                operation="load"
            )

        return None

    def save(self, data: Dict[str, Any]) -> None:
        """
        Save checkpoint to storage.

        Args:
            data: Checkpoint data to save
        """
        data = data.copy()
        data["last_update"] = datetime.utcnow().isoformat() + "Z"
        data["platform"] = self.platform

        try:
            if self.mode == "local":
                # Write to temp file first, then rename for atomicity
                temp_path = f"{self.checkpoint_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                os.replace(temp_path, self.checkpoint_path)

            elif self.mode == "gcs":
                bucket = self.gcs_client.bucket(self.bucket_name)
                blob = bucket.blob(self.checkpoint_path)
                blob.upload_from_string(
                    json.dumps(data, indent=2),
                    content_type="application/json"
                )

            self.logger.info(
                f"Checkpoint saved: {data.get('records_processed', 0)} records, "
                f"cursor: {data.get('cursor', 'none')[:20] if data.get('cursor') else 'none'}..."
            )

        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
            raise CheckpointError(
                message=f"Failed to save checkpoint: {e}",
                platform=self.platform,
                operation="save"
            )

    def get_cursor(self) -> Optional[str]:
        """
        Get pagination cursor from checkpoint.

        Returns:
            Cursor string or None if not found
        """
        checkpoint = self.load()
        cursor = checkpoint.get("cursor") if checkpoint else None
        if cursor:
            self.logger.info(f"Resuming from cursor: {cursor[:20]}...")
        return cursor

    def save_cursor(
        self,
        cursor: str,
        records_in_batch: int = 1000,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Save pagination cursor and update progress.

        Args:
            cursor: Pagination cursor from API
            records_in_batch: Number of records in the current batch
            additional_data: Optional additional data to store
        """
        checkpoint = self.load() or {}
        checkpoint["cursor"] = cursor
        checkpoint["records_processed"] = (
            checkpoint.get("records_processed", 0) + records_in_batch
        )

        if additional_data:
            checkpoint.update(additional_data)

        self.save(checkpoint)

    def update_progress(
        self,
        records_processed: int,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Update progress without changing cursor.

        Args:
            records_processed: Total number of records processed
            additional_data: Optional additional data to store
        """
        checkpoint = self.load() or {}
        checkpoint["records_processed"] = records_processed

        if additional_data:
            checkpoint.update(additional_data)

        self.save(checkpoint)

    def get_progress(self) -> Dict[str, Any]:
        """
        Get current progress information.

        Returns:
            Progress dictionary with records_processed, cursor, etc.
        """
        checkpoint = self.load()
        if checkpoint:
            return {
                "records_processed": checkpoint.get("records_processed", 0),
                "cursor": checkpoint.get("cursor"),
                "last_update": checkpoint.get("last_update"),
                "has_checkpoint": True
            }
        return {
            "records_processed": 0,
            "cursor": None,
            "last_update": None,
            "has_checkpoint": False
        }

    def clear(self) -> None:
        """Delete checkpoint (start fresh)."""
        try:
            if self.mode == "local":
                if os.path.exists(self.checkpoint_path):
                    os.remove(self.checkpoint_path)
                    self.logger.info("Local checkpoint cleared")
            elif self.mode == "gcs":
                bucket = self.gcs_client.bucket(self.bucket_name)
                blob = bucket.blob(self.checkpoint_path)
                if blob.exists():
                    blob.delete()
                    self.logger.info("GCS checkpoint cleared")
        except Exception as e:
            self.logger.error(f"Failed to clear checkpoint: {e}")
            raise CheckpointError(
                message=f"Failed to clear checkpoint: {e}",
                platform=self.platform,
                operation="clear"
            )
