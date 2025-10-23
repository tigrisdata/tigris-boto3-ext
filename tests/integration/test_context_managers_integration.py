"""Integration tests for context managers."""

import pytest

from .conftest import generate_bucket_name
from tigris_boto3_ext import TigrisFork, TigrisSnapshot, TigrisSnapshotEnabled


class TestSnapshotEnabledContext:
    """Test TigrisSnapshotEnabled context manager."""

    def test_snapshot_enabled_creates_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating bucket with snapshot enabled."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "ctx-snap-")
        cleanup_buckets.append(bucket_name)

        with TigrisSnapshotEnabled(s3_client):
            result = s3_client.create_bucket(Bucket=bucket_name)

        assert "Location" in result and result["Location"] == f'/{bucket_name}'

    def test_snapshot_enabled_reusable(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that context manager can be reused."""
        bucket1 = generate_bucket_name(test_bucket_prefix, "reuse1-")
        bucket2 = generate_bucket_name(test_bucket_prefix, "reuse2-")
        cleanup_buckets.extend([bucket1, bucket2])

        ctx = TigrisSnapshotEnabled(s3_client)

        with ctx:
            s3_client.create_bucket(Bucket=bucket1)

        with ctx:
            s3_client.create_bucket(Bucket=bucket2)

        # Verify both buckets exist
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket1 in bucket_names
        assert bucket2 in bucket_names

    def test_snapshot_enabled_with_exception(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test context manager cleanup with exception."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "exc-test-")
        cleanup_buckets.append(bucket_name)

        # Create bucket first
        s3_client.create_bucket(Bucket=bucket_name)

        # Try to create again (should fail) but context should cleanup
        with pytest.raises(Exception):
            with TigrisSnapshotEnabled(s3_client):
                # This should raise an error (bucket already exists)
                s3_client.create_bucket(Bucket=bucket_name)

        # Context should have cleaned up properly


class TestSnapshotContext:
    """Test TigrisSnapshot context manager."""

    def test_snapshot_context_lists_buckets(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing buckets in snapshot context."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "snap-list-")
        cleanup_buckets.append(bucket_name)

        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)

        # List in snapshot context
        with TigrisSnapshot(s3_client, bucket_name):
            result = s3_client.list_buckets()

        assert "Buckets" in result

    def test_snapshot_context_with_operations(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test various operations in snapshot context."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "snap-ops-")
        cleanup_buckets.append(bucket_name)

        # Create bucket and add data
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"Test data")

        # Perform operations in snapshot context
        with TigrisSnapshot(s3_client, bucket_name):
            # List objects
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            assert "Contents" in response

            # Get object
            obj_response = s3_client.get_object(Bucket=bucket_name, Key="file.txt")
            data = obj_response["Body"].read()
            assert data == b"Test data"


class TestForkContext:
    """Test TigrisFork context manager."""

    def test_fork_context_creates_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating bucket in fork context."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "fork-src-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "fork-dst-")
        cleanup_buckets.extend([fork_bucket, source_bucket])

        # Create source bucket with snapshot enabled
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=source_bucket)

        # Create fork in context
        with TigrisFork(s3_client, source_bucket):
            result = s3_client.create_bucket(Bucket=fork_bucket)

        assert "Location" in result

    def test_fork_context_reusable(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that fork context can be reused."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "fork-reuse-src-")
        fork1 = generate_bucket_name(test_bucket_prefix, "fork-reuse1-")
        fork2 = generate_bucket_name(test_bucket_prefix, "fork-reuse2-")
        cleanup_buckets.extend([fork1, fork2, source_bucket])

        # Create source bucket with snapshot enabled
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=source_bucket)

        ctx = TigrisFork(s3_client, source_bucket)

        with ctx:
            s3_client.create_bucket(Bucket=fork1)

        with ctx:
            s3_client.create_bucket(Bucket=fork2)

        # Verify both forks exist
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert fork1 in bucket_names
        assert fork2 in bucket_names


class TestNestedContexts:
    """Test nesting different context managers."""

    def test_nested_snapshot_contexts(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test nesting snapshot contexts."""
        bucket1 = generate_bucket_name(test_bucket_prefix, "nested1-")
        bucket2 = generate_bucket_name(test_bucket_prefix, "nested2-")
        cleanup_buckets.extend([bucket1, bucket2])

        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=bucket1)

            with TigrisSnapshotEnabled(s3_client):
                s3_client.create_bucket(Bucket=bucket2)

        # Verify both buckets exist
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket1 in bucket_names
        assert bucket2 in bucket_names

    def test_mixed_context_nesting(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test nesting different types of contexts."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "mixed-src-")
        snap_bucket = generate_bucket_name(test_bucket_prefix, "mixed-snap-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "mixed-fork-")
        cleanup_buckets.extend([snap_bucket, fork_bucket, source_bucket])

        # Create source bucket with snapshot enabled
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=source_bucket)

        # Use nested contexts
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=snap_bucket)

            with TigrisFork(s3_client, source_bucket):
                s3_client.create_bucket(Bucket=fork_bucket)

        # Verify all buckets exist
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert snap_bucket in bucket_names
        assert fork_bucket in bucket_names


class TestContextWithDataOperations:
    """Test contexts with actual data operations."""

    def test_put_get_in_snapshot_context(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test putting and getting objects in snapshot context."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "data-ctx-")
        cleanup_buckets.append(bucket_name)

        # Create bucket and put data
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=bucket_name)

        s3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"Context data")

        # Read in snapshot context
        with TigrisSnapshot(s3_client, bucket_name):
            response = s3_client.get_object(Bucket=bucket_name, Key="test.txt")
            data = response["Body"].read()
            assert data == b"Context data"

    def test_multiple_operations_in_context(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test multiple operations within a context."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "multi-ops-")
        cleanup_buckets.append(bucket_name)

        with TigrisSnapshotEnabled(s3_client):
            # Create bucket
            s3_client.create_bucket(Bucket=bucket_name)

            # Put multiple objects
            s3_client.put_object(Bucket=bucket_name, Key="file1.txt", Body=b"Data 1")
            s3_client.put_object(Bucket=bucket_name, Key="file2.txt", Body=b"Data 2")
            s3_client.put_object(Bucket=bucket_name, Key="file3.txt", Body=b"Data 3")

        # Verify all objects exist
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 3
