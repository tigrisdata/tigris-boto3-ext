"""Soft delete: recoverable deletes with a retention window."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from ._internal import create_header_injector

F = TypeVar("F", bound=Callable[..., Any])


class TigrisSoftDeleteEnabled:
    """
    Context manager to enable soft delete for bucket creation.

    Objects deleted from the bucket, and the bucket itself when deleted, stay
    recoverable for the retention window instead of being removed at once.

    Usage:
        with TigrisSoftDeleteEnabled(s3_client):
            s3_client.create_bucket(Bucket='my-bucket')  # default 7-day window

        with TigrisSoftDeleteEnabled(s3_client, retention_days=30):
            s3_client.create_bucket(Bucket='my-bucket')
    """

    MIN_RETENTION_DAYS = 7
    MAX_RETENTION_DAYS = 90

    def __init__(self, s3_client: S3Client, retention_days: Optional[int] = None):
        """
        Initialize context manager.

        Args:
            s3_client: boto3 S3 client instance
            retention_days: Retention window in days, 7 to 90. None uses the
                default 7-day window.
        """
        self.client = s3_client
        self.retention_days = retention_days

        if retention_days is None:
            header_value = "true"
        elif self.MIN_RETENTION_DAYS <= retention_days <= self.MAX_RETENTION_DAYS:
            header_value = str(retention_days)
        else:
            msg = (
                f"retention_days must be between {self.MIN_RETENTION_DAYS} "
                f"and {self.MAX_RETENTION_DAYS}, got {retention_days}"
            )
            raise ValueError(msg)

        self._injector = create_header_injector(
            s3_client,
            "CreateBucket",
            {"X-Tigris-Soft-Delete": header_value},
        )

    def __enter__(self) -> "TigrisSoftDeleteEnabled":
        """Enter context and register event handler."""
        self._injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister event handler."""
        self._injector.unregister()


def soft_delete_enabled(
    retention_days: Optional[int] = None,
) -> Callable[[F], F]:
    """
    Decorator to enable soft delete for bucket creation operations.

    The decorated function must accept an s3_client as its first argument.

    Args:
        retention_days: Retention window in days, 7 to 90. None uses the
            default 7-day window.

    Usage:
        @soft_delete_enabled()
        def create_my_bucket(s3_client, bucket_name):
            return s3_client.create_bucket(Bucket=bucket_name)

        @soft_delete_enabled(retention_days=30)
        def create_archive_bucket(s3_client, bucket_name):
            return s3_client.create_bucket(Bucket=bucket_name)
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(s3_client: Any, *args: Any, **kwargs: Any) -> Any:
            with TigrisSoftDeleteEnabled(s3_client, retention_days):
                return func(s3_client, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


def create_soft_delete_bucket(
    s3_client: S3Client,
    bucket_name: str,
    retention_days: Optional[int] = None,
) -> dict[str, Any]:
    """
    Create a bucket with soft delete enabled.

    Objects deleted from the bucket, and the bucket itself when deleted, stay
    recoverable for the retention window instead of being removed at once.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to create
        retention_days: Retention window in days, 7 to 90. None uses the
            default 7-day window.

    Returns:
        Response from create_bucket operation

    Raises:
        ValueError: If ``retention_days`` is outside 7 to 90

    Usage:
        create_soft_delete_bucket(s3_client, 'my-bucket')
        create_soft_delete_bucket(s3_client, 'my-bucket', retention_days=30)
    """
    with TigrisSoftDeleteEnabled(s3_client, retention_days):
        return cast("dict[str, Any]", s3_client.create_bucket(Bucket=bucket_name))
