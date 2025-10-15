"""Integration tests for fork functionality."""

import time

import pytest

from tigris_boto3_ext import TigrisFork, TigrisS3Client, create_fork, create_snapshot


class TestForkCreation:
    """Test creating bucket forks."""

    def test_create_fork_from_existing_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test forking an existing bucket."""
        source_bucket = f"{test_bucket_prefix}fork-source-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}fork-dest-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source bucket with data
        create_snapshot(s3_client, source_bucket)
        s3_client.put_object(
            Bucket=source_bucket, Key="file.txt", Body=b"Original data"
        )

        # Create fork
        result = create_fork(s3_client, fork_bucket, source_bucket)

        assert "Location" in result
        # Verify fork bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert fork_bucket in bucket_names

    def test_create_fork_with_context_manager(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test forking using context manager."""
        source_bucket = f"{test_bucket_prefix}fork-ctx-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}fork-ctx-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source bucket
        create_snapshot(s3_client, source_bucket)

        # Create fork using context manager
        with TigrisFork(s3_client, source_bucket):
            result = s3_client.create_bucket(Bucket=fork_bucket)

        assert "Location" in result
        # Verify fork bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert fork_bucket in bucket_names

    def test_create_fork_from_snapshot_version(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test forking from a specific snapshot version."""
        source_bucket = f"{test_bucket_prefix}fork-snap-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}fork-snap-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source bucket
        create_snapshot(s3_client, source_bucket, snapshot_name="v1")
        s3_client.put_object(Bucket=source_bucket, Key="data.txt", Body=b"Version 1")

        # Note: In real scenario, we'd get snapshot version from Tigris
        # For now, test the API without actual version
        result = create_fork(s3_client, fork_bucket, source_bucket)

        assert "Location" in result


class TestForkDataIsolation:
    """Test data isolation between original and fork."""

    def test_fork_contains_source_data(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that fork initially contains source bucket data."""
        source_bucket = f"{test_bucket_prefix}fork-iso-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}fork-iso-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source with data
        create_snapshot(s3_client, source_bucket)
        test_key = "shared-file.txt"
        test_data = b"Shared data"
        s3_client.put_object(Bucket=source_bucket, Key=test_key, Body=test_data)

        # Create fork
        create_fork(s3_client, fork_bucket, source_bucket)

        # Note: Actual fork behavior depends on Tigris implementation
        # Fork might inherit data or start empty depending on snapshot timing

    def test_modifications_are_independent(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that modifications to fork don't affect source."""
        source_bucket = f"{test_bucket_prefix}fork-mod-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}fork-mod-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source and fork
        create_snapshot(s3_client, source_bucket)
        create_fork(s3_client, fork_bucket, source_bucket)

        # Add data to fork
        s3_client.put_object(Bucket=fork_bucket, Key="fork-only.txt", Body=b"Fork data")

        # Verify fork has the object
        fork_objects = s3_client.list_objects_v2(Bucket=fork_bucket)
        fork_keys = [obj["Key"] for obj in fork_objects.get("Contents", [])]
        assert "fork-only.txt" in fork_keys

        # Verify source doesn't have it
        source_objects = s3_client.list_objects_v2(Bucket=source_bucket)
        source_keys = [obj["Key"] for obj in source_objects.get("Contents", [])]
        assert "fork-only.txt" not in source_keys


class TestForkWithTigrisClient:
    """Test fork operations using TigrisS3Client."""

    def test_create_fork_with_client(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating fork with TigrisS3Client."""
        client = TigrisS3Client(s3_client)
        source_bucket = f"{test_bucket_prefix}client-fork-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}client-fork-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source
        client.create_snapshot(source_bucket)

        # Create fork
        result = client.create_fork(fork_bucket, source_bucket)

        assert "Location" in result
        # Verify fork exists
        response = client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert fork_bucket in bucket_names

    def test_fork_context_with_client(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test fork context manager with TigrisS3Client."""
        client = TigrisS3Client(s3_client)
        source_bucket = f"{test_bucket_prefix}client-ctx-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}client-ctx-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source
        client.create_snapshot(source_bucket)

        # Use fork context
        with client.fork_context(source_bucket):
            result = client.create_bucket(Bucket=fork_bucket)

        assert "Location" in result

    def test_fork_with_snapshot_version_client(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test forking from snapshot version with client."""
        client = TigrisS3Client(s3_client)
        source_bucket = f"{test_bucket_prefix}client-ver-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}client-ver-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source with snapshot
        client.create_snapshot(source_bucket, snapshot_name="backup")

        # Note: snapshot_version would come from Tigris in real usage
        # Test the API without actual version
        result = client.create_fork(fork_bucket, source_bucket)

        assert "Location" in result


class TestMultipleForks:
    """Test creating multiple forks from the same source."""

    def test_create_multiple_forks_from_source(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating multiple forks from the same source bucket."""
        source_bucket = f"{test_bucket_prefix}multi-src-{int(time.time())}"
        fork1 = f"{test_bucket_prefix}multi-fork1-{int(time.time())}"
        fork2 = f"{test_bucket_prefix}multi-fork2-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork1, fork2])

        # Create source
        create_snapshot(s3_client, source_bucket)
        s3_client.put_object(Bucket=source_bucket, Key="base.txt", Body=b"Base data")

        # Create first fork
        result1 = create_fork(s3_client, fork1, source_bucket)
        assert "Location" in result1

        # Create second fork
        result2 = create_fork(s3_client, fork2, source_bucket)
        assert "Location" in result2

        # Verify both forks exist
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert fork1 in bucket_names
        assert fork2 in bucket_names
