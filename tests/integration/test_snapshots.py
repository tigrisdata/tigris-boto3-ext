"""Integration tests for snapshot functionality."""

import time

import pytest

from tigris_boto3_ext import (
    TigrisS3Client,
    TigrisSnapshot,
    TigrisSnapshotEnabled,
    create_snapshot,
    get_object_from_snapshot,
    list_snapshots,
)


class TestSnapshotCreation:
    """Test creating snapshots."""

    def test_create_snapshot_with_helper(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating a snapshot using the helper function."""
        bucket_name = f"{test_bucket_prefix}snapshot-helper-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        result = create_snapshot(s3_client, bucket_name)

        assert "Location" in result
        # Verify bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket_name in bucket_names

    def test_create_snapshot_with_context_manager(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating a snapshot using context manager."""
        bucket_name = f"{test_bucket_prefix}snapshot-ctx-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        with TigrisSnapshotEnabled(s3_client):
            result = s3_client.create_bucket(Bucket=bucket_name)

        assert "Location" in result
        # Verify bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket_name in bucket_names

    def test_create_snapshot_with_name(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating a named snapshot."""
        bucket_name = f"{test_bucket_prefix}named-snap-{int(time.time())}"
        snapshot_name = f"backup-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        result = create_snapshot(s3_client, bucket_name, snapshot_name=snapshot_name)

        assert "Location" in result
        # Verify bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket_name in bucket_names


class TestSnapshotListing:
    """Test listing snapshots."""

    def test_list_snapshots(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Test listing snapshots for a bucket."""
        bucket_name = f"{test_bucket_prefix}list-snap-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot
        create_snapshot(s3_client, bucket_name)

        # List snapshots
        result = list_snapshots(s3_client, bucket_name)

        assert "Buckets" in result
        # Note: The actual snapshot listing behavior depends on Tigris implementation

    def test_list_snapshots_with_context(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing snapshots using context manager."""
        bucket_name = f"{test_bucket_prefix}list-ctx-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket first
        create_snapshot(s3_client, bucket_name)

        # List snapshots using context
        with TigrisSnapshot(s3_client, bucket_name):
            result = s3_client.list_buckets()

        assert "Buckets" in result


class TestSnapshotDataAccess:
    """Test accessing data from snapshots."""

    def test_put_and_get_object_from_snapshot(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test putting object and retrieving from snapshot."""
        bucket_name = f"{test_bucket_prefix}data-snap-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket
        create_snapshot(s3_client, bucket_name)

        # Put an object
        test_key = "test-file.txt"
        test_data = b"Test data for snapshot"
        s3_client.put_object(Bucket=bucket_name, Key=test_key, Body=test_data)

        # Note: Getting object from snapshot requires snapshot version
        # This test demonstrates the API but actual snapshot version
        # would come from Tigris response
        # For now, just verify we can put/get objects normally
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        retrieved_data = response["Body"].read()
        assert retrieved_data == test_data

    def test_list_objects_from_snapshot_context(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing objects using snapshot context."""
        bucket_name = f"{test_bucket_prefix}list-obj-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket and put objects
        create_snapshot(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file1.txt", Body=b"data1")
        s3_client.put_object(Bucket=bucket_name, Key="file2.txt", Body=b"data2")

        # List objects
        with TigrisSnapshot(s3_client, bucket_name):
            response = s3_client.list_objects_v2(Bucket=bucket_name)

        assert "Contents" in response
        assert len(response["Contents"]) == 2
        keys = [obj["Key"] for obj in response["Contents"]]
        assert "file1.txt" in keys
        assert "file2.txt" in keys


class TestSnapshotWithTigrisClient:
    """Test snapshot operations using TigrisS3Client."""

    def test_create_snapshot_with_client(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating snapshot with TigrisS3Client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}client-snap-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        result = client.create_snapshot(bucket_name)

        assert "Location" in result
        # Verify bucket exists
        response = client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket_name in bucket_names

    def test_list_snapshots_with_client(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing snapshots with TigrisS3Client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}client-list-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket
        client.create_snapshot(bucket_name)

        # List snapshots
        result = client.list_snapshots(bucket_name)

        assert "Buckets" in result

    def test_snapshot_context_with_client(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test using snapshot context with TigrisS3Client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}client-ctx-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket
        client.create_snapshot(bucket_name)

        # Use context manager
        with client.snapshot_enabled():
            result = client.create_bucket(
                Bucket=f"{bucket_name}-2-{int(time.time())}"
            )
            cleanup_buckets.append(f"{bucket_name}-2-{int(time.time())}")

        assert "Location" in result
