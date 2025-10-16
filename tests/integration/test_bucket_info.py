"""Integration tests for bucket info functionality."""

from tigris_boto3_ext import (
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    get_bucket_info,
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
        cleanup_buckets.extend([source_bucket, forked_bucket])

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
        cleanup_buckets.extend([source_bucket, forked_bucket])

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
        cleanup_buckets.extend([source_bucket, forked_bucket])

        # Create source bucket with snapshot enabled
        create_snapshot_bucket(s3_client, source_bucket)

        # Create a fork from current state (no snapshot version specified)
        create_fork(s3_client, forked_bucket, source_bucket)

        # Get bucket info for forked bucket
        info = get_bucket_info(s3_client, forked_bucket)

        assert info["snapshot_enabled"] is True
        assert info["fork_source_bucket"] == source_bucket
        assert info["fork_source_snapshot"] != ""
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
