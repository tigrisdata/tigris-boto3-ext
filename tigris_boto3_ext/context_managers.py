"""Context managers for Tigris-specific S3 operations."""

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from ._internal import create_header_injector, create_multi_operation_injector


class TigrisSnapshotEnabled:
    """
    Context manager to enable snapshot support for bucket creation.

    Usage:
        with TigrisSnapshotEnabled(s3_client):
            s3_client.create_bucket(Bucket='my-bucket')
    """

    def __init__(self, s3_client: S3Client):
        """
        Initialize context manager.

        Args:
            s3_client: boto3 S3 client instance
        """
        self.client = s3_client
        self._injector = create_header_injector(
            s3_client,
            "CreateBucket",
            {"X-Tigris-Enable-Snapshot": "true"},
        )

    def __enter__(self) -> "TigrisSnapshotEnabled":
        """Enter context and register event handler."""
        self._injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister event handler."""
        self._injector.unregister()


class TigrisSnapshot:
    """
    Context manager for snapshot operations.

    Supports:
    - Listing snapshots for a bucket (via list_buckets)
    - Reading objects from a specific snapshot version

    Usage:
        # List snapshots
        with TigrisSnapshot(s3_client, 'my-bucket'):
            snapshots = s3_client.list_buckets()

        # Read from specific snapshot
        with TigrisSnapshot(s3_client, 'my-bucket', snapshot_version='12345'):
            obj = s3_client.get_object(Bucket='my-bucket', Key='file.txt')
            objects = s3_client.list_objects_v2(Bucket='my-bucket')
    """

    def __init__(
        self,
        s3_client: S3Client,
        bucket_name: str,
        snapshot_version: Optional[str] = None,
    ):
        """
        Initialize context manager.

        Args:
            s3_client: boto3 S3 client instance
            bucket_name: Name of the bucket to work with
            snapshot_version: Optional snapshot version ID for reading objects
        """
        self.client = s3_client
        self.bucket_name = bucket_name
        self.snapshot_version = snapshot_version
        self._injectors = []

        # For listing snapshots
        self._list_injector = create_header_injector(
            s3_client,
            "ListBuckets",
            {"X-Tigris-Snapshot": bucket_name},
        )
        self._injectors.append(self._list_injector)

        # For reading from snapshot version
        if snapshot_version:
            snapshot_ops = ["GetObject", "ListObjectsV2", "HeadObject", "ListObjects"]
            version_header = {"X-Tigris-Snapshot-Version": snapshot_version}
            self._version_injectors = create_multi_operation_injector(
                s3_client,
                snapshot_ops,
                version_header,
            )
            self._injectors.extend(self._version_injectors)

    def __enter__(self) -> "TigrisSnapshot":
        """Enter context and register event handlers."""
        for injector in self._injectors:
            injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister event handlers."""
        for injector in self._injectors:
            injector.unregister()


class TigrisFork:
    """
    Context manager for creating forked buckets.

    Usage:
        # Fork from current state of source bucket
        with TigrisFork(s3_client, 'source-bucket'):
            s3_client.create_bucket(Bucket='forked-bucket')

        # Fork from specific snapshot
        with TigrisFork(s3_client, 'source-bucket', snapshot_version='12345'):
            s3_client.create_bucket(Bucket='forked-bucket')
    """

    def __init__(
        self,
        s3_client: S3Client,
        source_bucket: str,
        snapshot_version: Optional[str] = None,
    ):
        """
        Initialize context manager.

        Args:
            s3_client: boto3 S3 client instance
            source_bucket: Name of the bucket to fork from
            snapshot_version: Optional snapshot version to fork from
        """
        self.client = s3_client
        self.source_bucket = source_bucket
        self.snapshot_version = snapshot_version

        headers = {"X-Tigris-Fork-Source-Bucket": source_bucket}
        if snapshot_version:
            headers["X-Tigris-Fork-Source-Bucket-Snapshot"] = snapshot_version

        self._injector = create_header_injector(s3_client, "CreateBucket", headers)

    def __enter__(self) -> "TigrisFork":
        """Enter context and register event handler."""
        self._injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister event handler."""
        self._injector.unregister()
