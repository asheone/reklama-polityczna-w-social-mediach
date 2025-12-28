"""
Tests for the output writer module.
"""

import json
import os
import tempfile
import pytest
from shared.output_writer import OutputWriter
from shared.exceptions import OutputError


class TestOutputWriter:
    """Test cases for OutputWriter class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_init_local_mode(self, temp_dir):
        """Test initialization in local mode."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        assert writer.platform == "meta"
        assert writer.mode == "local"
        assert writer.total_records == 0
        assert writer.batch_count == 0
        assert os.path.exists(writer.output_dir)

    def test_init_gcs_mode_requires_bucket(self):
        """Test that GCS mode requires bucket name."""
        with pytest.raises(OutputError) as exc_info:
            OutputWriter(
                platform="meta",
                mode="gcs",
                bucket_name=None
            )

        assert "bucket_name required" in str(exc_info.value)

    def test_write_record(self, temp_dir):
        """Test writing a single record."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=10
        )

        record = {"ad_id": "123", "content": "test"}
        writer.write_record(record)

        assert writer.total_records == 1
        assert len(writer.current_batch) == 1

    def test_write_records(self, temp_dir):
        """Test writing multiple records."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=100
        )

        records = [{"ad_id": str(i)} for i in range(5)]
        writer.write_records(records)

        assert writer.total_records == 5
        assert len(writer.current_batch) == 5

    def test_auto_flush_batch(self, temp_dir):
        """Test automatic batch flushing when size reached."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=3
        )

        for i in range(5):
            writer.write_record({"ad_id": str(i)})

        # Should have flushed once (3 records) with 2 remaining
        assert writer.batch_count == 1
        assert len(writer.current_batch) == 2

    def test_flush_batch_creates_file(self, temp_dir):
        """Test that flush creates NDJSON file."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=100
        )

        writer.write_record({"ad_id": "123", "content": "test"})
        filename = writer.flush_batch()

        assert filename is not None
        assert filename.endswith(".ndjson")
        assert writer.batch_count == 1

        # Verify file exists and content
        filepath = os.path.join(writer.output_dir, filename)
        assert os.path.exists(filepath)

        with open(filepath, "r") as f:
            content = f.read()
            data = json.loads(content.strip())
            assert data["ad_id"] == "123"

    def test_flush_empty_batch(self, temp_dir):
        """Test flushing empty batch returns None."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir
        )

        result = writer.flush_batch()
        assert result is None
        assert writer.batch_count == 0

    def test_finalize_creates_manifest(self, temp_dir):
        """Test finalize creates manifest file."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=10
        )

        for i in range(5):
            writer.write_record({"ad_id": str(i)})

        manifest = writer.finalize()

        assert manifest["platform"] == "meta"
        assert manifest["total_records"] == 5
        assert manifest["total_batches"] == 1
        assert len(manifest["batch_files"]) == 1

        # Verify manifest file exists
        manifest_files = [f for f in os.listdir(writer.output_dir) if "manifest" in f]
        assert len(manifest_files) == 1

    def test_ndjson_format(self, temp_dir):
        """Test NDJSON format (one JSON per line)."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=100
        )

        records = [
            {"ad_id": "1", "content": "first"},
            {"ad_id": "2", "content": "second"},
            {"ad_id": "3", "content": "third"}
        ]

        for record in records:
            writer.write_record(record)

        filename = writer.flush_batch()
        filepath = os.path.join(writer.output_dir, filename)

        with open(filepath, "r") as f:
            lines = f.read().strip().split("\n")

        assert len(lines) == 3

        for i, line in enumerate(lines):
            data = json.loads(line)
            assert data["ad_id"] == str(i + 1)

    def test_unicode_handling(self, temp_dir):
        """Test handling of Unicode characters."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=100
        )

        record = {
            "ad_id": "123",
            "content": "Polski tekst z polskimi znakami: ąęóśżźćń ĄĘÓŚŻŹĆŃ"
        }

        writer.write_record(record)
        filename = writer.flush_batch()
        filepath = os.path.join(writer.output_dir, filename)

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.loads(f.read().strip())

        assert "ąęóśżźćń" in data["content"]

    def test_get_stats(self, temp_dir):
        """Test statistics retrieval."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=10
        )

        for i in range(5):
            writer.write_record({"ad_id": str(i)})

        stats = writer.get_stats()

        assert stats["platform"] == "meta"
        assert stats["total_records"] == 5
        assert stats["batches_written"] == 0
        assert stats["records_in_current_batch"] == 5
        assert stats["batch_size"] == 10

    def test_compression(self, temp_dir):
        """Test compressed output."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=100,
            compress=True
        )

        writer.write_record({"ad_id": "123"})
        filename = writer.flush_batch()

        assert filename.endswith(".ndjson.gz")

        filepath = os.path.join(writer.output_dir, filename)
        assert os.path.exists(filepath)

        # Verify can decompress
        import gzip
        with gzip.open(filepath, "rt") as f:
            data = json.loads(f.read().strip())
            assert data["ad_id"] == "123"

    def test_batch_numbering(self, temp_dir):
        """Test batch file numbering."""
        writer = OutputWriter(
            platform="meta",
            mode="local",
            base_path=temp_dir,
            batch_size=2
        )

        for i in range(6):
            writer.write_record({"ad_id": str(i)})

        manifest = writer.finalize()

        assert len(manifest["batch_files"]) == 3
        assert "batch_0001" in manifest["batch_files"][0]
        assert "batch_0002" in manifest["batch_files"][1]
        assert "batch_0003" in manifest["batch_files"][2]
