"""Bucket snapshots: enabling, creating, listing and deleting snapshots."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._internal import (
    _guard_lock,
    create_header_injector,
    create_multi_operation_injector,
    has_active_injector,
)

F = TypeVar("F", bound=Callable[..., Any])
""" Context Managers """


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


""" Decorators """


def snapshot_enabled(func: F) -> F:
    """
    Decorator to enable snapshot support for bucket creation operations.

    The decorated function must accept an s3_client as its first argument.

    Usage:
        @snapshot_enabled
        def create_my_bucket(s3_client, bucket_name):
            return s3_client.create_bucket(Bucket=bucket_name)

        result = create_my_bucket(s3_client, 'my-bucket')
    """

    @wraps(func)
    def wrapper(s3_client: Any, *args: Any, **kwargs: Any) -> Any:
        with TigrisSnapshotEnabled(s3_client):
            return func(s3_client, *args, **kwargs)

    return wrapper  # type: ignore


def with_snapshot(
    bucket_name: str,
    snapshot_version: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator for snapshot operations.

    Without snapshot_version: lists available snapshots for the bucket.
    With snapshot_version: operates on a specific snapshot (read objects, etc.).

    The decorated function must accept an s3_client as its first argument.

    Args:
        bucket_name: Name of the bucket
        snapshot_version: Optional snapshot version ID

    Usage:
        # List available snapshots
        @with_snapshot('my-bucket')
        def list_bucket_snapshots(s3_client):
            return s3_client.list_buckets()

        snapshots = list_bucket_snapshots(s3_client)

        # Read from specific snapshot
        @with_snapshot('my-bucket', snapshot_version='12345')
        def read_file(s3_client, key):
            return s3_client.get_object(Bucket='my-bucket', Key=key)

        obj = read_file(s3_client, 'file.txt')
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(s3_client: Any, *args: Any, **kwargs: Any) -> Any:
            with TigrisSnapshot(s3_client, bucket_name, snapshot_version):
                return func(s3_client, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


""" Helpers """


def create_snapshot_bucket(
    s3_client: S3Client,
    bucket_name: str,
) -> dict[str, Any]:
    """
    Create a bucket with snapshot support enabled.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to create

    Returns:
        Response from create_bucket operation

    Usage:
        result = create_snapshot_bucket(s3_client, 'my-bucket')
    """
    with TigrisSnapshotEnabled(s3_client):
        return cast("dict[str, Any]", s3_client.create_bucket(Bucket=bucket_name))


def create_snapshot(
    s3_client: S3Client,
    bucket_name: str,
    snapshot_name: Optional[str] = None,
) -> dict[str, Any]:
    """
    Create a snapshot of a bucket.

    This is a convenience wrapper around create_bucket that sets the
    X-Tigris-Snapshot header to create a snapshot instead of a regular bucket.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to snapshot
        snapshot_name: Optional name for the snapshot

    Returns:
        Response from create_bucket operation

    Usage:
        result = create_snapshot(s3_client, 'my-bucket')
        result = create_snapshot(s3_client, 'my-bucket', snapshot_name='backup-1')
    """
    header_value = "true"
    if snapshot_name:
        header_value = f"true; name={snapshot_name}"

    injector = create_header_injector(
        s3_client,
        "CreateBucket",
        {"X-Tigris-Snapshot": header_value},
    )

    try:
        injector.register()
        return cast("dict[str, Any]", s3_client.create_bucket(Bucket=bucket_name))
    finally:
        injector.unregister()


def get_snapshot_version(response: dict[str, Any]) -> Optional[str]:
    """
    Extract snapshot version from a create_snapshot response.

    Args:
        response: Response from create_snapshot operation

    Returns:
        Snapshot version ID, or None if not found

    Usage:
        result = create_snapshot(s3_client, 'my-bucket', snapshot_name='backup')
        version = get_snapshot_version(result)
        # Use version for forking or accessing snapshot data
        create_fork(s3_client, 'my-fork', 'my-bucket', snapshot_version=version)
    """
    version = (
        response.get("ResponseMetadata", {})
        .get("HTTPHeaders", {})
        .get("x-tigris-snapshot-version")
    )
    return cast(Optional[str], version)


def list_snapshots(s3_client: S3Client, bucket_name: str) -> dict[str, Any]:
    """
    List all snapshots for a bucket.

    This is a convenience wrapper around list_buckets that filters to show
    snapshots for a specific bucket.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to list snapshots for

    Returns:
        Response from list_buckets operation containing snapshot information

    Usage:
        snapshots = list_snapshots(s3_client, 'my-bucket')
        for bucket in snapshots.get('Buckets', []):
            print(bucket['Name'])
    """
    with TigrisSnapshot(s3_client, bucket_name):
        return cast("dict[str, Any]", s3_client.list_buckets())


def delete_snapshot(
    s3_client: S3Client,
    bucket_name: str,
    snapshot_version: str,
) -> dict[str, Any]:
    """
    Delete a specific snapshot of a bucket.

    The header is injected through the client's event system, so it applies to
    every ``delete_bucket`` issued through this client while the call is in
    flight. Do not run this helper concurrently on a shared client, and do not
    share the client with other threads that delete buckets during the call.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket that owns the snapshot
        snapshot_version: Version of the snapshot to delete, as returned by
            ``get_snapshot_version`` or listed by ``list_snapshots``

    Returns:
        Response from the underlying ``delete_bucket`` operation

    Raises:
        ValueError: If ``bucket_name`` or ``snapshot_version`` is empty
        RuntimeError: If another ``delete_snapshot`` call is in flight on this client

    Usage:
        result = create_snapshot(s3_client, 'my-bucket', snapshot_name='backup')
        version = get_snapshot_version(result)
        delete_snapshot(s3_client, 'my-bucket', version)
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    if not snapshot_version:
        msg = "snapshot_version is required"
        raise ValueError(msg)

    injector = create_header_injector(
        s3_client,
        "DeleteBucket",
        {"X-Tigris-Snapshot-Version": snapshot_version},
    )

    with _guard_lock:
        if has_active_injector(s3_client, "DeleteBucket"):
            msg = (
                "Cannot delete snapshot while another delete_snapshot call is in flight"
            )
            raise RuntimeError(msg)
        injector.register()

    try:
        return cast("dict[str, Any]", s3_client.delete_bucket(Bucket=bucket_name))
    finally:
        injector.unregister()
