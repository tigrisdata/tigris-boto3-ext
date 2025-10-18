"""Integration tests for snapshot_name parameter functionality."""

import pytest

from tests.integration.conftest import generate_bucket_name
from tigris_boto3_ext import (
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    forked_from,
    get_snapshot_version,
    get_snapshot_version_by_name,
)


class TestGetSnapshotVersionByName:
    """Test get_snapshot_version_by_name helper function."""

    def test_get_snapshot_version_by_name_found(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test getting snapshot version by name when snapshot exists."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "version-by-name-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshots enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Create snapshots with names
        snapshot_response = create_snapshot(
            s3_client, bucket_name, snapshot_name="test-snapshot"
        )
        expected_version = get_snapshot_version(snapshot_response)

        # Get version by name
        version = get_snapshot_version_by_name(
            s3_client, bucket_name, "test-snapshot"
        )

        assert version == expected_version
        assert version is not None

    def test_get_snapshot_version_by_name_not_found(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test getting snapshot version by name when snapshot doesn't exist."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "version-not-found-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshots enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Try to get version for non-existent snapshot
        version = get_snapshot_version_by_name(
            s3_client, bucket_name, "non-existent-snapshot"
        )

        assert version is None

    def test_get_snapshot_version_by_name_multiple_snapshots(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test getting specific snapshot version when multiple exist."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "multi-snapshots-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshots enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Create multiple snapshots
        create_snapshot(s3_client, bucket_name, snapshot_name="snapshot-1")
        snapshot_response = create_snapshot(
            s3_client, bucket_name, snapshot_name="snapshot-2"
        )
        expected_version = get_snapshot_version(snapshot_response)
        create_snapshot(s3_client, bucket_name, snapshot_name="snapshot-3")

        # Get version for specific snapshot
        version = get_snapshot_version_by_name(s3_client, bucket_name, "snapshot-2")

        assert version == expected_version


class TestCreateForkWithSnapshotName:
    """Test create_fork with snapshot_name parameter."""

    def test_create_fork_with_snapshot_name(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test forking from a named snapshot."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "fork-source-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "fork-dest-")
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source bucket and add data
        create_snapshot_bucket(s3_client, source_bucket)
        s3_client.put_object(Bucket=source_bucket, Key="test.txt", Body=b"v1")

        # Create named snapshot
        create_snapshot(s3_client, source_bucket, snapshot_name="backup")

        # Modify data after snapshot
        s3_client.put_object(Bucket=source_bucket, Key="test.txt", Body=b"v2")

        # Fork from named snapshot
        result = create_fork(
            s3_client, fork_bucket, source_bucket, snapshot_name="backup"
        )

        assert "Location" in result

        # Verify forked data is from snapshot (v1), not current state (v2)
        obj = s3_client.get_object(Bucket=fork_bucket, Key="test.txt")
        content = obj["Body"].read()
        assert content == b"v1"

    def test_create_fork_with_both_params_raises_error(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that providing both snapshot_version and snapshot_name raises ValueError."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "both-params-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "fork-")
        cleanup_buckets.append(source_bucket)

        # Create source bucket
        create_snapshot_bucket(s3_client, source_bucket)

        # Try to fork with both parameters
        with pytest.raises(ValueError, match="Cannot specify both"):
            create_fork(
                s3_client,
                fork_bucket,
                source_bucket,
                snapshot_version="12345",
                snapshot_name="backup",
            )

    def test_create_fork_with_nonexistent_snapshot_name_raises_error(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that providing non-existent snapshot_name raises ValueError."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "no-snapshot-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "fork-")
        cleanup_buckets.append(source_bucket)

        # Create source bucket
        create_snapshot_bucket(s3_client, source_bucket)

        # Try to fork with non-existent snapshot name
        with pytest.raises(ValueError, match="not found"):
            create_fork(
                s3_client,
                fork_bucket,
                source_bucket,
                snapshot_name="non-existent",
            )


class TestForkedFromDecoratorWithSnapshotName:
    """Test forked_from decorator with snapshot_name parameter."""

    def test_forked_from_decorator_with_snapshot_name(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test using @forked_from decorator with snapshot_name."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "decorator-source-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "decorator-fork-")
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source bucket and snapshot
        create_snapshot_bucket(s3_client, source_bucket)
        s3_client.put_object(Bucket=source_bucket, Key="data.txt", Body=b"original")
        create_snapshot(s3_client, source_bucket, snapshot_name="named-backup")
        s3_client.put_object(Bucket=source_bucket, Key="data.txt", Body=b"modified")

        # Use decorator with snapshot_name
        @forked_from(source_bucket, snapshot_name="named-backup")
        def create_my_fork(s3, bucket_name):
            return s3.create_bucket(Bucket=bucket_name)

        result = create_my_fork(s3_client, fork_bucket)

        assert "Location" in result

        # Verify data is from snapshot
        obj = s3_client.get_object(Bucket=fork_bucket, Key="data.txt")
        content = obj["Body"].read()
        assert content == b"original"

    def test_forked_from_decorator_both_params_raises_error(self):
        """Test that decorator raises error when both params provided."""
        with pytest.raises(ValueError, match="Cannot specify both"):

            @forked_from(
                "source-bucket", snapshot_version="12345", snapshot_name="backup"
            )
            def should_fail(s3, bucket_name):
                return s3.create_bucket(Bucket=bucket_name)

    def test_forked_from_decorator_nonexistent_snapshot_raises_error(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that decorator raises error when snapshot doesn't exist."""
        source_bucket = generate_bucket_name(
            test_bucket_prefix, "decorator-no-snap-"
        )
        fork_bucket = generate_bucket_name(test_bucket_prefix, "decorator-fork-")
        cleanup_buckets.append(source_bucket)

        # Create source bucket
        create_snapshot_bucket(s3_client, source_bucket)

        # Define decorated function with non-existent snapshot
        @forked_from(source_bucket, snapshot_name="non-existent-snapshot")
        def create_fork_from_missing(s3, bucket_name):
            return s3.create_bucket(Bucket=bucket_name)

        # Call the decorated function - should raise error
        with pytest.raises(ValueError, match="not found"):
            create_fork_from_missing(s3_client, fork_bucket)
