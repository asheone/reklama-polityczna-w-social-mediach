"""
Tests for the checkpoint manager module.
"""

import json
import os
import tempfile
import pytest
from shared.checkpoint_manager import CheckpointManager
from shared.exceptions import CheckpointError


class TestCheckpointManager:
    """Test cases for CheckpointManager class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_init_local_mode(self, temp_dir):
        """Test initialization in local mode."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        assert manager.platform == "meta"
        assert manager.mode == "local"
        assert temp_dir in manager.checkpoint_path

    def test_init_gcs_mode_requires_bucket(self):
        """Test that GCS mode requires bucket name."""
        with pytest.raises(CheckpointError) as exc_info:
            CheckpointManager(
                platform="meta",
                mode="gcs",
                bucket_name=None
            )

        assert "bucket_name required" in str(exc_info.value)

    def test_init_invalid_mode(self):
        """Test that invalid mode raises error."""
        with pytest.raises(CheckpointError) as exc_info:
            CheckpointManager(
                platform="meta",
                mode="invalid"
            )

        assert "Invalid mode" in str(exc_info.value)

    def test_save_and_load(self, temp_dir):
        """Test saving and loading checkpoint."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        test_data = {
            "cursor": "abc123",
            "records_processed": 1000
        }

        manager.save(test_data)
        loaded = manager.load()

        assert loaded["cursor"] == "abc123"
        assert loaded["records_processed"] == 1000
        assert "last_update" in loaded
        assert loaded["platform"] == "meta"

    def test_load_nonexistent(self, temp_dir):
        """Test loading when no checkpoint exists."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        result = manager.load()
        assert result is None

    def test_get_cursor(self, temp_dir):
        """Test getting cursor from checkpoint."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        # No checkpoint
        assert manager.get_cursor() is None

        # With checkpoint
        manager.save({"cursor": "test_cursor"})
        assert manager.get_cursor() == "test_cursor"

    def test_save_cursor(self, temp_dir):
        """Test saving cursor."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        manager.save_cursor("cursor1", records_in_batch=500)
        loaded = manager.load()

        assert loaded["cursor"] == "cursor1"
        assert loaded["records_processed"] == 500

        # Save another cursor (should accumulate)
        manager.save_cursor("cursor2", records_in_batch=500)
        loaded = manager.load()

        assert loaded["cursor"] == "cursor2"
        assert loaded["records_processed"] == 1000

    def test_save_cursor_with_additional_data(self, temp_dir):
        """Test saving cursor with additional data."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        manager.save_cursor(
            "cursor1",
            records_in_batch=500,
            additional_data={"date_range": "2024-01-01 to 2024-01-31"}
        )

        loaded = manager.load()
        assert loaded["date_range"] == "2024-01-01 to 2024-01-31"

    def test_update_progress(self, temp_dir):
        """Test updating progress without cursor."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        manager.update_progress(2500, additional_data={"status": "running"})

        loaded = manager.load()
        assert loaded["records_processed"] == 2500
        assert loaded["status"] == "running"

    def test_get_progress(self, temp_dir):
        """Test getting progress information."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        # No checkpoint
        progress = manager.get_progress()
        assert progress["has_checkpoint"] is False
        assert progress["records_processed"] == 0

        # With checkpoint
        manager.save({
            "cursor": "test",
            "records_processed": 5000
        })

        progress = manager.get_progress()
        assert progress["has_checkpoint"] is True
        assert progress["records_processed"] == 5000
        assert progress["cursor"] == "test"

    def test_clear(self, temp_dir):
        """Test clearing checkpoint."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        # Create checkpoint
        manager.save({"cursor": "test"})
        assert manager.load() is not None

        # Clear it
        manager.clear()
        assert manager.load() is None

    def test_clear_nonexistent(self, temp_dir):
        """Test clearing when no checkpoint exists."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        # Should not raise
        manager.clear()

    def test_corrupted_checkpoint_handled(self, temp_dir):
        """Test that corrupted checkpoint is handled gracefully."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        # Write invalid JSON
        with open(manager.checkpoint_path, "w") as f:
            f.write("not valid json{{{")

        # Should return None, not raise
        result = manager.load()
        assert result is None

    def test_atomic_write(self, temp_dir):
        """Test that writes are atomic (temp file then rename)."""
        manager = CheckpointManager(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        manager.save({"cursor": "test"})

        # Verify temp file doesn't exist (was renamed)
        temp_path = f"{manager.checkpoint_path}.tmp"
        assert not os.path.exists(temp_path)

        # Verify actual file exists
        assert os.path.exists(manager.checkpoint_path)
