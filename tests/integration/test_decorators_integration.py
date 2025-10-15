"""Integration tests for decorators."""

from conftest import generate_bucket_name

import pytest

from tigris_boto3_ext import TigrisSnapshotEnabled, forked_from, snapshot_enabled, with_snapshot


class TestSnapshotEnabledDecorator:
    """Test @snapshot_enabled decorator."""

    def test_decorator_creates_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating bucket with decorator."""

        @snapshot_enabled
        def create_snapshot_bucket(client, bucket_name):
            return client.create_bucket(Bucket=bucket_name)

        bucket_name = generate_bucket_name(test_bucket_prefix, "dec-snap-")
        cleanup_buckets.append(bucket_name)

        result = create_snapshot_bucket(s3_client, bucket_name)

        assert "Location" in result
        # Verify bucket exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket_name in bucket_names

    def test_decorator_with_multiple_operations(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test decorator with multiple operations."""

        @snapshot_enabled
        def create_and_populate_bucket(client, bucket_name, files):
            client.create_bucket(Bucket=bucket_name)
            for filename, content in files.items():
                client.put_object(Bucket=bucket_name, Key=filename, Body=content)
            return bucket_name

        bucket_name = generate_bucket_name(test_bucket_prefix, "dec-multi-")
        cleanup_buckets.append(bucket_name)

        files = {
            "file1.txt": b"Content 1",
            "file2.txt": b"Content 2",
            "file3.txt": b"Content 3",
        }

        result = create_and_populate_bucket(s3_client, bucket_name, files)

        assert result == bucket_name
        # Verify files exist
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 3

    def test_decorator_reusable(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Test that decorated function can be called multiple times."""

        @snapshot_enabled
        def create_bucket(client, name):
            return client.create_bucket(Bucket=name)

        bucket1 = generate_bucket_name(test_bucket_prefix, "dec-reuse1-")
        bucket2 = generate_bucket_name(test_bucket_prefix, "dec-reuse2-")
        cleanup_buckets.extend([bucket1, bucket2])

        result1 = create_bucket(s3_client, bucket1)
        result2 = create_bucket(s3_client, bucket2)

        assert "Location" in result1
        assert "Location" in result2


class TestWithSnapshotDecorator:
    """Test @with_snapshot decorator."""

    def test_decorator_lists_snapshots(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing snapshots with decorator."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "dec-list-")
        cleanup_buckets.append(bucket_name)

        # Create bucket first
        s3_client.create_bucket(Bucket=bucket_name)

        @with_snapshot(bucket_name)
        def list_bucket_snapshots(client):
            return client.list_buckets()

        result = list_bucket_snapshots(s3_client)

        assert "Buckets" in result

    def test_decorator_accesses_objects(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test accessing objects with decorator."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "dec-obj-")
        cleanup_buckets.append(bucket_name)

        # Create bucket and add data
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"Test data")

        @with_snapshot(bucket_name)
        def read_object(client, key):
            response = client.get_object(Bucket=bucket_name, Key=key)
            return response["Body"].read()

        data = read_object(s3_client, "test.txt")

        assert data == b"Test data"

    def test_decorator_with_snapshot_version(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test decorator with snapshot version."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "dec-ver-")
        cleanup_buckets.append(bucket_name)

        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="data.txt", Body=b"V1 data")

        # Note: snapshot_version would come from Tigris in real usage
        @with_snapshot(bucket_name)
        def list_objects(client):
            response = client.list_objects_v2(Bucket=bucket_name)
            return [obj["Key"] for obj in response.get("Contents", [])]

        keys = list_objects(s3_client)

        assert "data.txt" in keys


class TestForkedFromDecorator:
    """Test @forked_from decorator."""

    def test_decorator_creates_fork(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating fork with decorator."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "dec-fork-src-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "dec-fork-dst-")
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source bucket with snapshot enabled
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=source_bucket)

        @forked_from(source_bucket)
        def create_fork(client, new_bucket_name):
            return client.create_bucket(Bucket=new_bucket_name)

        result = create_fork(s3_client, fork_bucket)

        assert "Location" in result
        # Verify fork exists
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert fork_bucket in bucket_names

    def test_decorator_fork_isolation(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that fork is isolated from source."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "dec-iso-src-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "dec-iso-dst-")
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source bucket with snapshot enabled and add data
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=source_bucket)
        s3_client.put_object(
            Bucket=source_bucket, Key="source.txt", Body=b"Source data"
        )

        @forked_from(source_bucket)
        def create_and_modify_fork(client, fork_name):
            client.create_bucket(Bucket=fork_name)
            # Add fork-specific data
            client.put_object(Bucket=fork_name, Key="fork.txt", Body=b"Fork data")
            return fork_name

        create_and_modify_fork(s3_client, fork_bucket)

        # Verify fork has its own data
        fork_objects = s3_client.list_objects_v2(Bucket=fork_bucket)
        fork_keys = [obj["Key"] for obj in fork_objects.get("Contents", [])]
        assert "fork.txt" in fork_keys

        # Verify source doesn't have fork's data
        source_objects = s3_client.list_objects_v2(Bucket=source_bucket)
        source_keys = [obj["Key"] for obj in source_objects.get("Contents", [])]
        assert "fork.txt" not in source_keys

    def test_decorator_multiple_forks(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating multiple forks with same decorator."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "dec-multi-src-")
        fork1 = generate_bucket_name(test_bucket_prefix, "dec-multi-fork1-")
        fork2 = generate_bucket_name(test_bucket_prefix, "dec-multi-fork2-")
        cleanup_buckets.extend([source_bucket, fork1, fork2])

        # Create source bucket with snapshot enabled
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket=source_bucket)

        @forked_from(source_bucket)
        def create_fork(client, fork_name):
            return client.create_bucket(Bucket=fork_name)

        result1 = create_fork(s3_client, fork1)
        result2 = create_fork(s3_client, fork2)

        assert "Location" in result1
        assert "Location" in result2
        # Verify both forks exist
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert fork1 in bucket_names
        assert fork2 in bucket_names


class TestDecoratorCombinations:
    """Test combining decorators in workflows."""

    def test_snapshot_then_fork(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Test workflow: create snapshot, then fork it."""
        source_bucket = generate_bucket_name(test_bucket_prefix, "wf-src-")
        fork_bucket = generate_bucket_name(test_bucket_prefix, "wf-fork-")
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source with snapshot
        @snapshot_enabled
        def create_source(client, name):
            return client.create_bucket(Bucket=name)

        create_source(s3_client, source_bucket)

        # Fork from it
        @forked_from(source_bucket)
        def create_fork(client, name):
            return client.create_bucket(Bucket=name)

        result = create_fork(s3_client, fork_bucket)

        assert "Location" in result

    def test_multiple_decorated_functions(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test using multiple decorated functions together."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "multi-dec-")
        cleanup_buckets.append(bucket_name)

        @snapshot_enabled
        def setup_bucket(client, name):
            client.create_bucket(Bucket=name)
            return name

        def verify_bucket(client, name):
            response = client.list_buckets()
            bucket_names = [b["Name"] for b in response.get("Buckets", [])]
            return name in bucket_names

        # Setup
        setup_bucket(s3_client, bucket_name)

        # Verify
        exists = verify_bucket(s3_client, bucket_name)
        assert exists
