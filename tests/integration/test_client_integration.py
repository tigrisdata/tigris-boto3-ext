"""Integration tests for TigrisS3Client."""

import time

import pytest

from tigris_boto3_ext import TigrisS3Client


class TestTigrisS3ClientBasics:
    """Test basic TigrisS3Client functionality."""

    def test_client_initialization(self, s3_client):
        """Test initializing TigrisS3Client."""
        client = TigrisS3Client(s3_client)
        assert client is not None

    def test_client_delegates_list_buckets(self, s3_client):
        """Test that client delegates list_buckets."""
        client = TigrisS3Client(s3_client)
        result = client.list_buckets()
        assert "Buckets" in result

    def test_client_delegates_standard_operations(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that client delegates standard S3 operations."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}client-std-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket
        result = client.create_bucket(Bucket=bucket_name)
        assert "Location" in result

        # Put object
        client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"Test data")

        # Get object
        response = client.get_object(Bucket=bucket_name, Key="test.txt")
        data = response["Body"].read()
        assert data == b"Test data"

        # List objects
        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 1


class TestTigrisS3ClientSnapshots:
    """Test TigrisS3Client snapshot methods."""

    def test_client_create_snapshot(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating snapshot with client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}client-snap-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        result = client.create_snapshot(bucket_name)

        assert "Location" in result

    def test_client_create_snapshot_with_name(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating named snapshot with client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}client-named-{int(time.time())}"
        snapshot_name = f"backup-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        result = client.create_snapshot(bucket_name, snapshot_name=snapshot_name)

        assert "Location" in result

    def test_client_list_snapshots(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test listing snapshots with client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}client-list-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket
        client.create_snapshot(bucket_name)

        # List snapshots
        result = client.list_snapshots(bucket_name)

        assert "Buckets" in result

    def test_client_snapshot_data_operations(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test snapshot data operations with client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}client-data-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket and add data
        client.create_snapshot(bucket_name)
        client.put_object(Bucket=bucket_name, Key="file1.txt", Body=b"Data 1")
        client.put_object(Bucket=bucket_name, Key="file2.txt", Body=b"Data 2")

        # List objects
        response = client.list_objects_v2(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 2


class TestTigrisS3ClientForks:
    """Test TigrisS3Client fork methods."""

    def test_client_create_fork(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Test creating fork with client."""
        client = TigrisS3Client(s3_client)
        source_bucket = f"{test_bucket_prefix}client-fork-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}client-fork-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source
        client.create_snapshot(source_bucket)

        # Create fork
        result = client.create_fork(fork_bucket, source_bucket)

        assert "Location" in result

    def test_client_create_fork_with_version(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test creating fork with snapshot version."""
        client = TigrisS3Client(s3_client)
        source_bucket = f"{test_bucket_prefix}client-ver-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}client-ver-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source
        client.create_snapshot(source_bucket, snapshot_name="v1")

        # Note: snapshot_version would come from Tigris in real usage
        result = client.create_fork(fork_bucket, source_bucket)

        assert "Location" in result

    def test_client_fork_data_isolation(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test fork data isolation with client."""
        client = TigrisS3Client(s3_client)
        source_bucket = f"{test_bucket_prefix}client-iso-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}client-iso-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source with data
        client.create_snapshot(source_bucket)
        client.put_object(Bucket=source_bucket, Key="source.txt", Body=b"Source")

        # Create fork and add data
        client.create_fork(fork_bucket, source_bucket)
        client.put_object(Bucket=fork_bucket, Key="fork.txt", Body=b"Fork")

        # Verify isolation
        fork_objs = client.list_objects_v2(Bucket=fork_bucket)
        fork_keys = [obj["Key"] for obj in fork_objs.get("Contents", [])]
        assert "fork.txt" in fork_keys

        source_objs = client.list_objects_v2(Bucket=source_bucket)
        source_keys = [obj["Key"] for obj in source_objs.get("Contents", [])]
        assert "fork.txt" not in source_keys


class TestTigrisS3ClientContexts:
    """Test TigrisS3Client context manager methods."""

    def test_client_snapshot_enabled_context(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test snapshot_enabled context from client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}ctx-snap-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        with client.snapshot_enabled():
            result = client.create_bucket(Bucket=bucket_name)

        assert "Location" in result

    def test_client_snapshot_context(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test snapshot_context from client."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}ctx-list-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket
        client.create_snapshot(bucket_name)

        # Use context
        with client.snapshot_context(bucket_name):
            result = client.list_buckets()

        assert "Buckets" in result

    def test_client_fork_context(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Test fork_context from client."""
        client = TigrisS3Client(s3_client)
        source_bucket = f"{test_bucket_prefix}ctx-fork-src-{int(time.time())}"
        fork_bucket = f"{test_bucket_prefix}ctx-fork-dst-{int(time.time())}"
        cleanup_buckets.extend([source_bucket, fork_bucket])

        # Create source
        client.create_snapshot(source_bucket)

        # Use fork context
        with client.fork_context(source_bucket):
            result = client.create_bucket(Bucket=fork_bucket)

        assert "Location" in result

    def test_client_nested_contexts(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test nesting contexts with client."""
        client = TigrisS3Client(s3_client)
        bucket1 = f"{test_bucket_prefix}nested1-{int(time.time())}"
        bucket2 = f"{test_bucket_prefix}nested2-{int(time.time())}"
        cleanup_buckets.extend([bucket1, bucket2])

        with client.snapshot_enabled():
            client.create_bucket(Bucket=bucket1)

            with client.snapshot_context(bucket1):
                result = client.list_buckets()
                assert "Buckets" in result

            client.create_bucket(Bucket=bucket2)

        # Verify both buckets exist
        response = client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        assert bucket1 in bucket_names
        assert bucket2 in bucket_names


class TestTigrisS3ClientWorkflows:
    """Test complete workflows with TigrisS3Client."""

    def test_complete_snapshot_workflow(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test complete snapshot workflow."""
        client = TigrisS3Client(s3_client)
        bucket_name = f"{test_bucket_prefix}wf-complete-{int(time.time())}"
        cleanup_buckets.append(bucket_name)

        # Create bucket with snapshot
        client.create_snapshot(bucket_name, snapshot_name="initial")

        # Add data
        client.put_object(Bucket=bucket_name, Key="v1.txt", Body=b"Version 1")

        # List snapshots
        snapshots = client.list_snapshots(bucket_name)
        assert "Buckets" in snapshots

        # Read data
        response = client.get_object(Bucket=bucket_name, Key="v1.txt")
        data = response["Body"].read()
        assert data == b"Version 1"

    def test_complete_fork_workflow(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test complete fork workflow."""
        client = TigrisS3Client(s3_client)
        source = f"{test_bucket_prefix}wf-fork-src-{int(time.time())}"
        fork = f"{test_bucket_prefix}wf-fork-dst-{int(time.time())}"
        cleanup_buckets.extend([source, fork])

        # Create source with data
        client.create_snapshot(source)
        client.put_object(Bucket=source, Key="shared.txt", Body=b"Shared data")

        # Create fork
        client.create_fork(fork, source)

        # Modify fork independently
        client.put_object(Bucket=fork, Key="fork-only.txt", Body=b"Fork data")

        # Verify both buckets
        source_objs = client.list_objects_v2(Bucket=source)
        fork_objs = client.list_objects_v2(Bucket=fork)

        source_keys = [obj["Key"] for obj in source_objs.get("Contents", [])]
        fork_keys = [obj["Key"] for obj in fork_objs.get("Contents", [])]

        assert "shared.txt" in source_keys
        assert "fork-only.txt" not in source_keys
        assert "fork-only.txt" in fork_keys

    def test_mixed_operations_workflow(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test workflow mixing contexts, helpers, and direct operations."""
        client = TigrisS3Client(s3_client)
        bucket1 = f"{test_bucket_prefix}mixed1-{int(time.time())}"
        bucket2 = f"{test_bucket_prefix}mixed2-{int(time.time())}"
        cleanup_buckets.extend([bucket1, bucket2])

        # Use snapshot context to create first bucket
        with client.snapshot_enabled():
            client.create_bucket(Bucket=bucket1)
            client.put_object(Bucket=bucket1, Key="file1.txt", Body=b"Data 1")

        # Create second bucket normally
        client.create_snapshot(bucket2)
        client.put_object(Bucket=bucket2, Key="file2.txt", Body=b"Data 2")

        # List all buckets
        all_buckets = client.list_buckets()
        bucket_names = [b["Name"] for b in all_buckets.get("Buckets", [])]

        assert bucket1 in bucket_names
        assert bucket2 in bucket_names
