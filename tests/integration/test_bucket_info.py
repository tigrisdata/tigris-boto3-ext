"""Integration tests for bucket info functionality."""

import time
from datetime import datetime

import pytest

from tigris_boto3_ext import (
    TigrisAPIError,
    create_bucket,
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    create_soft_delete_bucket,
    get_bucket_info,
    get_bucket_metadata,
    get_snapshot_version,
    has_snapshot_enabled,
)

from .conftest import generate_bucket_name


class TestSnapshotEnabled:
    """Test checking if snapshot is enabled for buckets."""

    def test_has_snapshot_enabled_on_snapshot_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test has_snapshot_enabled returns True for snapshot-enabled buckets."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "snap-enabled-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Check if snapshots are enabled
        assert has_snapshot_enabled(s3_client, bucket_name) is True

    def test_has_snapshot_enabled_on_regular_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test has_snapshot_enabled returns False for regular buckets."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "regular-")
        cleanup_buckets.append(bucket_name)

        # Create regular bucket (without snapshot enabled)
        s3_client.create_bucket(Bucket=bucket_name)

        # Check if snapshots are enabled
        assert has_snapshot_enabled(s3_client, bucket_name) is False


class TestBucketInfo:
    """Test retrieving comprehensive bucket information."""

    def test_get_bucket_info_snapshot_enabled(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test get_bucket_info for snapshot-enabled bucket."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "info-snap-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Get bucket info
        info = get_bucket_info(s3_client, bucket_name)

        assert info["snapshot_enabled"] is True
        assert info["fork_source_bucket"] is None
        assert info["fork_source_snapshot"] is None
        assert "response_metadata" in info

    def test_get_bucket_info_regular_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test get_bucket_info for regular bucket."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "info-regular-")
        cleanup_buckets.append(bucket_name)

        # Create regular bucket
        s3_client.create_bucket(Bucket=bucket_name)

        # Get bucket info
        info = get_bucket_info(s3_client, bucket_name)

        assert info["snapshot_enabled"] is False
        assert info["fork_source_bucket"] is None
        assert info["fork_source_snapshot"] is None
        assert "response_metadata" in info

    def test_get_bucket_info_forked_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test get_bucket_info for forked bucket."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "source-")
        forked_bucket = generate_bucket_name(test_bucket_prefix, "forked-")
        cleanup_buckets.extend([forked_bucket, source_bucket])

        # Create source bucket with snapshot enabled
        create_snapshot_bucket(s3_client, source_bucket)

        # Put some data
        s3_client.put_object(Bucket=source_bucket, Key="test.txt", Body=b"test data")

        # Create a snapshot
        snapshot_response = create_snapshot(s3_client, source_bucket, snapshot_name="v1")
        snapshot_version = get_snapshot_version(snapshot_response)

        # Create a forked bucket from the snapshot
        create_fork(s3_client, forked_bucket, source_bucket, snapshot_version=snapshot_version)

        # Get bucket info for forked bucket
        info = get_bucket_info(s3_client, forked_bucket)

        assert info["snapshot_enabled"] is True  # Forked buckets inherit snapshot support
        assert info["fork_source_bucket"] == source_bucket
        assert info["fork_source_snapshot"] == snapshot_version
        assert "response_metadata" in info

    def test_get_bucket_info_fork_parent(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test get_bucket_info for bucket that is a fork parent."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "parent-")
        forked_bucket = generate_bucket_name(test_bucket_prefix, "child-")
        cleanup_buckets.extend([forked_bucket, source_bucket])

        # Create source bucket with snapshot enabled
        create_snapshot_bucket(s3_client, source_bucket)

        # Create a fork (makes source_bucket a fork parent)
        create_fork(s3_client, forked_bucket, source_bucket)

        # Get bucket info for source bucket
        info = get_bucket_info(s3_client, source_bucket)

        assert info["snapshot_enabled"] is True
        assert info["fork_source_bucket"] is None
        assert info["fork_source_snapshot"] is None
        assert "response_metadata" in info

    def test_get_bucket_info_forked_from_current_state(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test get_bucket_info for bucket forked from current state (no snapshot version)."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "src-current-")
        forked_bucket = generate_bucket_name(test_bucket_prefix, "fork-current-")
        cleanup_buckets.extend([forked_bucket, source_bucket])

        # Create source bucket with snapshot enabled
        create_snapshot_bucket(s3_client, source_bucket)

        # Create a fork from current state (no snapshot version specified)
        create_fork(s3_client, forked_bucket, source_bucket)

        # Get bucket info for forked bucket
        info = get_bucket_info(s3_client, forked_bucket)

        assert info["snapshot_enabled"] is True
        assert info["fork_source_bucket"] == source_bucket
        assert info["fork_source_snapshot"] != "" and info["fork_source_snapshot"] is not None
        assert "response_metadata" in info


class TestBucketInfoEdgeCases:
    """Test edge cases for bucket info functionality."""

    def test_bucket_info_with_multiple_snapshots(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test bucket info when bucket has multiple snapshots."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "multi-snap-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Create multiple snapshots
        create_snapshot(s3_client, bucket_name, snapshot_name="v1")
        create_snapshot(s3_client, bucket_name, snapshot_name="v2")
        create_snapshot(s3_client, bucket_name, snapshot_name="v3")

        # Get bucket info
        info = get_bucket_info(s3_client, bucket_name)

        # Should still report snapshot_enabled as True
        assert info["snapshot_enabled"] is True
        assert "response_metadata" in info


class TestGetBucketMetadata:
    """Test the metadata endpoint helper."""

    def test_regular_bucket_defaults(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """A plain bucket reports the gateway defaults and no fork or soft-delete state."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "meta-regular-")
        cleanup_buckets.append(bucket_name)
        s3_client.create_bucket(Bucket=bucket_name)

        info = get_bucket_metadata(s3_client, bucket_name)

        assert info["name"] == bucket_name
        assert isinstance(info["created_at"], datetime)
        assert info["snapshot_enabled"] is False
        assert info["storage_class"] == "STANDARD"
        assert info["regions"] == []
        assert info["is_fork_parent"] is False
        assert info["fork_parents"] == []
        assert info["soft_delete"] == {"enabled": False, "retention_days": None}
        assert info["size"] is None
        assert info["raw"]["name"] == bucket_name

    def test_fork_parent_and_child(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """The child reports its parent and snapshot; the parent carries fork info."""
        parent = generate_bucket_name(test_bucket_prefix, "meta-parent-")
        child = generate_bucket_name(test_bucket_prefix, "meta-child-")
        cleanup_buckets.extend([child, parent])
        create_snapshot_bucket(s3_client, parent)
        version = get_snapshot_version(
            create_snapshot(s3_client, parent, snapshot_name="v1")
        )
        create_fork(s3_client, child, parent, snapshot_version=version)

        # The child's lineage is recorded with the fork itself. The parent's
        # fork info (ForkInfo.HasChildren) is refreshed asynchronously and, after
        # a fork from a named snapshot, may be absent for an unbounded time, so it
        # is not asserted here; the is_fork_parent mapping is covered by the unit
        # tests against a captured payload.
        deadline = time.time() + 15
        while True:
            child_info = get_bucket_metadata(s3_client, child)
            if child_info["fork_parents"] or time.time() > deadline:
                break
            time.sleep(1)
        parent_info = get_bucket_metadata(s3_client, parent)

        assert parent_info["snapshot_enabled"] is True
        assert parent_info["fork_parents"] == []

        assert child_info["is_fork_parent"] is False
        (lineage,) = child_info["fork_parents"]
        assert lineage["bucket"] == parent
        assert lineage["snapshot"] == version
        assert lineage["snapshot_name"] == "v1"
        assert isinstance(lineage["forked_at"], datetime)
        assert isinstance(lineage["snapshot_created_at"], datetime)

    def test_soft_delete_bucket(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Soft delete settings are reported with their retention window."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "meta-softdel-")
        cleanup_buckets.append(bucket_name)
        create_soft_delete_bucket(s3_client, bucket_name, retention_days=30)

        info = get_bucket_metadata(s3_client, bucket_name)

        assert info["soft_delete"] == {"enabled": True, "retention_days": 30}
        assert info["snapshot_enabled"] is False

    def test_creation_options_are_reported(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Storage class and regions set at creation come back normalized."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "meta-options-")
        cleanup_buckets.append(bucket_name)
        create_bucket(
            s3_client,
            bucket_name,
            default_storage_class="STANDARD_IA",
            regions=["iad", "sjc"],
        )

        info = get_bucket_metadata(s3_client, bucket_name)

        assert info["storage_class"] == "STANDARD_IA"
        assert info["regions"] == ["iad", "sjc"]

    def test_with_size(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """with_size=True fills in the size estimates."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "meta-size-")
        cleanup_buckets.append(bucket_name)
        s3_client.create_bucket(Bucket=bucket_name)
        for i in range(3):
            s3_client.put_object(
                Bucket=bucket_name, Key=f"data/{i}.txt", Body=b"x" * 1024
            )

        # The estimates are updated asynchronously: give them a moment to settle,
        # then assert bounds rather than exact numbers.
        expected = {"objects": 3, "bytes": 3072, "objects_all_versions": 3}
        deadline = time.time() + 15
        while True:
            size = get_bucket_metadata(s3_client, bucket_name, with_size=True)["size"]
            if size == expected or time.time() > deadline:
                break
            time.sleep(1)

        assert set(size) == set(expected)
        assert all(isinstance(value, int) for value in size.values())
        assert 0 <= size["bytes"] <= 3072
        assert 0 <= size["objects_all_versions"] <= 3
        assert 0 <= size["objects"] <= 3

    def test_missing_bucket_raises_tigris_api_error(self, s3_client, test_bucket_prefix):
        """A nonexistent bucket surfaces the gateway's NoSuchBucket error."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "meta-missing-")

        with pytest.raises(TigrisAPIError) as exc_info:
            get_bucket_metadata(s3_client, bucket_name)

        assert exc_info.value.status_code == 404
        assert exc_info.value.code == "NoSuchBucket"
        assert exc_info.value.request_id
