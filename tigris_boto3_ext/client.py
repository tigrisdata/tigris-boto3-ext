"""TigrisS3Client wrapper for enhanced Tigris functionality."""

from typing import Any, Optional

from .context_managers import TigrisFork, TigrisSnapshot, TigrisSnapshotEnabled
from .helpers import (
    create_fork,
    create_snapshot,
    get_object_from_snapshot,
    head_object_from_snapshot,
    list_objects_from_snapshot,
    list_snapshots,
)


class TigrisS3Client:
    """
    Wrapper around boto3 S3 client that adds Tigris-specific methods.

    This class maintains full boto3 compatibility while providing convenient
    methods for Tigris-specific features like snapshots and forking.

    Usage:
        import boto3
        from tigris_boto3_ext import TigrisS3Client

        s3 = boto3.client('s3')
        tigris_s3 = TigrisS3Client(s3)

        # Use Tigris-specific methods
        tigris_s3.create_snapshot('my-bucket')
        snapshots = tigris_s3.list_snapshots('my-bucket')

        # Regular boto3 methods still work
        tigris_s3.list_buckets()
        tigris_s3.put_object(Bucket='my-bucket', Key='file.txt', Body=b'data')
    """

    def __init__(self, s3_client: Any):
        """
        Initialize TigrisS3Client wrapper.

        Args:
            s3_client: boto3 S3 client instance to wrap
        """
        self._client = s3_client

    def __getattr__(self, name: str) -> Any:
        """
        Delegate attribute access to underlying boto3 client.

        This allows full boto3 compatibility for all standard S3 operations.
        """
        return getattr(self._client, name)

    # Snapshot Operations

    def create_snapshot(
        self,
        bucket_name: str,
        snapshot_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Create a snapshot of a bucket.

        Args:
            bucket_name: Name of the bucket to snapshot
            snapshot_name: Optional name for the snapshot

        Returns:
            Response from create_bucket operation
        """
        return create_snapshot(self._client, bucket_name, snapshot_name)

    def list_snapshots(self, bucket_name: str) -> dict[str, Any]:
        """
        List all snapshots for a bucket.

        Args:
            bucket_name: Name of the bucket to list snapshots for

        Returns:
            Response containing snapshot information
        """
        return list_snapshots(self._client, bucket_name)

    def get_object_from_snapshot(
        self,
        bucket_name: str,
        key: str,
        snapshot_version: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Retrieve an object from a specific snapshot.

        Args:
            bucket_name: Name of the bucket
            key: Object key to retrieve
            snapshot_version: Snapshot version ID
            **kwargs: Additional arguments to pass to get_object

        Returns:
            Response from get_object operation
        """
        return get_object_from_snapshot(
            self._client,
            bucket_name,
            key,
            snapshot_version,
            **kwargs,
        )

    def list_objects_from_snapshot(
        self,
        bucket_name: str,
        snapshot_version: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        List objects in a bucket from a specific snapshot.

        Args:
            bucket_name: Name of the bucket
            snapshot_version: Snapshot version ID
            **kwargs: Additional arguments to pass to list_objects_v2

        Returns:
            Response from list_objects_v2 operation
        """
        return list_objects_from_snapshot(
            self._client,
            bucket_name,
            snapshot_version,
            **kwargs,
        )

    def head_object_from_snapshot(
        self,
        bucket_name: str,
        key: str,
        snapshot_version: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Retrieve object metadata from a specific snapshot.

        Args:
            bucket_name: Name of the bucket
            key: Object key
            snapshot_version: Snapshot version ID
            **kwargs: Additional arguments to pass to head_object

        Returns:
            Response from head_object operation
        """
        return head_object_from_snapshot(
            self._client,
            bucket_name,
            key,
            snapshot_version,
            **kwargs,
        )

    # Fork Operations

    def create_fork(
        self,
        new_bucket_name: str,
        source_bucket: str,
        snapshot_version: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Create a forked bucket from a source bucket.

        Args:
            new_bucket_name: Name for the new forked bucket
            source_bucket: Name of the bucket to fork from
            snapshot_version: Optional snapshot version to fork from

        Returns:
            Response from create_bucket operation
        """
        return create_fork(
            self._client,
            new_bucket_name,
            source_bucket,
            snapshot_version,
        )

    # Context Manager Methods

    def snapshot_enabled(self) -> TigrisSnapshotEnabled:
        """
        Get context manager for creating snapshot-enabled buckets.

        Returns:
            TigrisSnapshotEnabled context manager

        Usage:
            with tigris_s3.snapshot_enabled():
                tigris_s3.create_bucket(Bucket='my-bucket')
        """
        return TigrisSnapshotEnabled(self._client)

    def snapshot_context(
        self,
        bucket_name: str,
        snapshot_version: Optional[str] = None,
    ) -> TigrisSnapshot:
        """
        Get context manager for snapshot operations.

        Args:
            bucket_name: Name of the bucket
            snapshot_version: Optional snapshot version ID

        Returns:
            TigrisSnapshot context manager

        Usage:
            with tigris_s3.snapshot_context('my-bucket', '12345'):
                obj = tigris_s3.get_object(Bucket='my-bucket', Key='file.txt')
        """
        return TigrisSnapshot(self._client, bucket_name, snapshot_version)

    def fork_context(
        self,
        source_bucket: str,
        snapshot_version: Optional[str] = None,
    ) -> TigrisFork:
        """
        Get context manager for fork operations.

        Args:
            source_bucket: Name of the bucket to fork from
            snapshot_version: Optional snapshot version to fork from

        Returns:
            TigrisFork context manager

        Usage:
            with tigris_s3.fork_context('source-bucket'):
                tigris_s3.create_bucket(Bucket='forked-bucket')
        """
        return TigrisFork(self._client, source_bucket, snapshot_version)
