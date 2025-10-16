"""Integration tests for snapshot functionality."""

import time

import pytest
from .conftest import generate_bucket_name

from tigris_boto3_ext import (
    TigrisSnapshot,
    TigrisSnapshotEnabled,
    create_snapshot,
    create_snapshot_bucket,
    get_object_from_snapshot,
    get_snapshot_version,
    head_object_from_snapshot,
    list_objects_from_snapshot,
    list_snapshots,
)


class TestSnapshotCreation:
    """Test creating snapshots."""

    def test_create_snapshot_enabled_bucket_with_helper(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating a snapshot using the helper function."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "snapshot-helper-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        result = create_snapshot_bucket(s3_client, bucket_name)

        assert "Location" in result
        # Verify bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket_name in bucket_names

    def test_create_snapshot_enabled_bucket_with_context_manager(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating a snapshot using context manager."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "snapshot-ctx-")
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
        """Test creating a named snapshot and extracting version."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "named-snap-")
        snapshot_name = f"backup-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled first
        create_snapshot_bucket(s3_client, bucket_name)

        # Create a named snapshot
        result = create_snapshot(s3_client, bucket_name, snapshot_name=snapshot_name)

        assert "ResponseMetadata" in result

        # Test get_snapshot_version helper
        snapshot_version = get_snapshot_version(result)
        assert snapshot_version is not None
        assert isinstance(snapshot_version, str)

        # Verify bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket_name in bucket_names


class TestSnapshotListing:
    """Test listing snapshots."""

    def test_list_snapshots(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Test listing snapshots for a bucket."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "list-snap-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Create 3 snapshots
        create_snapshot(s3_client, bucket_name, snapshot_name="v1")
        create_snapshot(s3_client, bucket_name, snapshot_name="v2")
        create_snapshot(s3_client, bucket_name, snapshot_name="v3")

        # List snapshots
        result = list_snapshots(s3_client, bucket_name)

        assert "Buckets" in result
        assert len(result["Buckets"]) == 3
        assert all(bucket["CreationDate"] is not None for bucket in result["Buckets"])

    def test_list_snapshots_with_context(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing snapshots using context manager."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "list-ctx-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled first
        create_snapshot_bucket(s3_client, bucket_name)

        # List snapshots using context
        with TigrisSnapshot(s3_client, bucket_name):
            result = s3_client.list_buckets()

        assert "Buckets" in result


class TestSnapshotDataAccess:
    """Test accessing data from snapshots."""

    def test_get_object_from_snapshot_helper(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test getting object from snapshot using helper function."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "get-snap-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Put an object
        test_key = "test-file.txt"
        test_data = b"Test data for snapshot"
        s3_client.put_object(Bucket=bucket_name, Key=test_key, Body=test_data)

        # Create a snapshot and get version
        snapshot_response = create_snapshot(s3_client, bucket_name, snapshot_name="v1")
        snapshot_version = get_snapshot_version(snapshot_response)

        # Get object from snapshot using helper
        response = get_object_from_snapshot(
            s3_client, bucket_name, test_key, snapshot_version
        )
        retrieved_data = response["Body"].read()
        assert retrieved_data == test_data

    def test_list_objects_from_snapshot_helper(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing objects from snapshot using helper function."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "list-helper-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled and put objects
        create_snapshot_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file1.txt", Body=b"data1")
        s3_client.put_object(Bucket=bucket_name, Key="file2.txt", Body=b"data2")

        # Create a snapshot and get version
        snapshot_response = create_snapshot(s3_client, bucket_name)
        snapshot_version = get_snapshot_version(snapshot_response)

        # List objects from snapshot using helper
        response = list_objects_from_snapshot(
            s3_client, bucket_name, snapshot_version
        )

        assert "Contents" in response
        assert len(response["Contents"]) == 2
        keys = [obj["Key"] for obj in response["Contents"]]
        assert "file1.txt" in keys
        assert "file2.txt" in keys

    def test_head_object_from_snapshot_helper(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test getting object metadata from snapshot using helper function."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "head-snap-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Put an object
        test_key = "metadata-test.txt"
        test_data = b"Test metadata"
        s3_client.put_object(Bucket=bucket_name, Key=test_key, Body=test_data)

        # Create a snapshot and get version
        snapshot_response = create_snapshot(s3_client, bucket_name)
        snapshot_version = get_snapshot_version(snapshot_response)

        # Get object metadata from snapshot using helper
        response = head_object_from_snapshot(
            s3_client, bucket_name, test_key, snapshot_version
        )

        assert "ContentLength" in response
        assert response["ContentLength"] == len(test_data)
        assert "ETag" in response

    def test_snapshot_context_with_version(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test accessing snapshot data using context manager with version."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "ctx-ver-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled and put objects
        create_snapshot_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="v1.txt", Body=b"Version 1")

        # Create snapshot
        snapshot_response = create_snapshot(s3_client, bucket_name, snapshot_name="snap1")
        snapshot_version = get_snapshot_version(snapshot_response)

        # Add more data after snapshot
        s3_client.put_object(Bucket=bucket_name, Key="v2.txt", Body=b"Version 2")

        # Access snapshot using context manager with version
        with TigrisSnapshot(s3_client, bucket_name, snapshot_version):
            response = s3_client.list_objects_v2(Bucket=bucket_name)

        # Snapshot should only have v1.txt, not v2.txt
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert "v1.txt" in keys


class TestSnapshotHelperFunctions:
    """Test snapshot helper functions comprehensively."""

    def test_create_snapshot_helper(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating snapshot with helper function."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "helper-snap-")
        cleanup_buckets.append(bucket_name)

        result = create_snapshot_bucket(s3_client, bucket_name)

        assert "Location" in result
        # Verify bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket_name in bucket_names

    def test_create_named_snapshot_helper(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating named snapshot and accessing version with helper."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "helper-named-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Create a named snapshot
        snapshot_response = create_snapshot(s3_client, bucket_name, snapshot_name="backup1")
        snapshot_version = get_snapshot_version(snapshot_response)

        assert snapshot_version is not None
        assert isinstance(snapshot_version, str)

    def test_list_snapshots_helper(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing snapshots with helper function."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "helper-list-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # List snapshots
        result = list_snapshots(s3_client, bucket_name)

        assert "Buckets" in result

    def test_snapshot_data_operations_helpers(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test snapshot data access helper functions."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "helper-data-")
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Add data
        test_key = "helper-test.txt"
        test_data = b"Helper test data"
        s3_client.put_object(Bucket=bucket_name, Key=test_key, Body=test_data)

        # Create snapshot
        snapshot_response = create_snapshot(s3_client, bucket_name)
        snapshot_version = get_snapshot_version(snapshot_response)

        # Test get_object_from_snapshot
        obj_response = get_object_from_snapshot(
            s3_client, bucket_name, test_key, snapshot_version
        )
        retrieved_data = obj_response["Body"].read()
        assert retrieved_data == test_data

        # Test list_objects_from_snapshot
        list_response = list_objects_from_snapshot(s3_client, bucket_name, snapshot_version)
        assert "Contents" in list_response
        keys = [obj["Key"] for obj in list_response["Contents"]]
        assert test_key in keys

        # Test head_object_from_snapshot
        head_response = head_object_from_snapshot(
            s3_client, bucket_name, test_key, snapshot_version
        )
        assert "ContentLength" in head_response
        assert head_response["ContentLength"] == len(test_data)

    def test_snapshot_context_with_helpers(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test using snapshot context with helper functions."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "helper-ctx-")
        bucket_name_2 = generate_bucket_name(test_bucket_prefix, "helper-ctx-")
        cleanup_buckets.extend([bucket_name, bucket_name_2])

        # Create first bucket with snapshot enabled
        create_snapshot_bucket(s3_client, bucket_name)

        # Use context manager to create another snapshot-enabled bucket
        with TigrisSnapshotEnabled(s3_client):
            result = s3_client.create_bucket(Bucket=bucket_name_2)

        assert "Location" in result and result["Location"] == f'/{bucket_name_2}'
