"""Bucket forks: creating buckets that branch from another bucket or snapshot."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._internal import create_header_injector

F = TypeVar("F", bound=Callable[..., Any])
""" Context Managers """


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


""" Decorators """


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


""" Helpers """


def create_fork(
    s3_client: S3Client,
    new_bucket_name: str,
    source_bucket: str,
    snapshot_version: Optional[str] = None,
) -> dict[str, Any]:
    """
    Create a forked bucket from a source bucket.

    Args:
        s3_client: boto3 S3 client instance
        new_bucket_name: Name for the new forked bucket
        source_bucket: Name of the bucket to fork from
        snapshot_version: Optional snapshot version to fork from

    Returns:
        Response from create_bucket operation

    Usage:
        # Fork from current state
        result = create_fork(s3_client, 'my-fork', 'source-bucket')

        # Fork from specific snapshot
        result = create_fork(
            s3_client,
            'my-fork',
            'source-bucket',
            snapshot_version='12345'
        )
    """
    with TigrisFork(s3_client, source_bucket, snapshot_version):
        return cast("dict[str, Any]", s3_client.create_bucket(Bucket=new_bucket_name))
