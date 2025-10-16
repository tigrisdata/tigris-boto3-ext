"""Decorators for Tigris-specific S3 operations."""

from functools import wraps
from typing import Any, Callable, Optional, TypeVar

from .context_managers import TigrisFork, TigrisSnapshot, TigrisSnapshotEnabled

F = TypeVar("F", bound=Callable[..., Any])


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


def forked_from(
    source_bucket: str,
    snapshot_version: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator for creating forked buckets.

    The decorated function must accept an s3_client as its first argument.

    Args:
        source_bucket: Name of the bucket to fork from
        snapshot_version: Optional snapshot version to fork from

    Usage:
        @forked_from('source-bucket')
        def create_fork(s3_client, new_bucket_name):
            return s3_client.create_bucket(Bucket=new_bucket_name)

        result = create_fork(s3_client, 'forked-bucket')

        # Fork from specific snapshot
        @forked_from('source-bucket', snapshot_version='12345')
        def create_fork_from_snapshot(s3_client, new_bucket_name):
            return s3_client.create_bucket(Bucket=new_bucket_name)

        result = create_fork_from_snapshot(s3_client, 'forked-bucket')
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(s3_client: Any, *args: Any, **kwargs: Any) -> Any:
            with TigrisFork(s3_client, source_bucket, snapshot_version):
                return func(s3_client, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator
