"""Bucket deletion: soft delete at creation, force delete, and recovering deleted buckets."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._internal import (
    _guard_lock,
    create_header_injector,
    has_active_injector,
)
from tigris_boto3_ext._transport import parse_timestamp, signed_request

F = TypeVar("F", bound=Callable[..., Any])
""" Context Managers """


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
            retention_days: Retention window in whole days, 7 to 90. None uses
                the default 7-day window.

        Raises:
            TypeError: If ``retention_days`` is not an int
            ValueError: If ``retention_days`` is outside 7 to 90
        """
        self.client = s3_client
        self.retention_days = retention_days

        if retention_days is None:
            header_value = "true"
        else:
            if isinstance(retention_days, bool) or not isinstance(retention_days, int):
                msg = f"retention_days must be a whole number of days, got {retention_days!r}"
                raise TypeError(msg)
            if not self.MIN_RETENTION_DAYS <= retention_days <= self.MAX_RETENTION_DAYS:
                msg = (
                    f"retention_days must be between {self.MIN_RETENTION_DAYS} "
                    f"and {self.MAX_RETENTION_DAYS}, got {retention_days}"
                )
                raise ValueError(msg)
            header_value = str(retention_days)

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


""" Decorators """


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


""" Helpers """


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


def force_delete_bucket(s3_client: S3Client, bucket_name: str) -> dict[str, Any]:
    """
    Delete a bucket even when it still contains objects.

    If the bucket has soft delete enabled, it and its objects move into the
    recoverable soft-deleted state for the retention window. Otherwise the
    bucket and every object in it are removed permanently and cannot be
    recovered, not even by support.

    The header is injected through the client's event system, so it applies to
    every ``delete_bucket`` issued through this client while the call is in
    flight. A second bucket deletion helper on the same client during that
    window is refused.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to delete

    Returns:
        Response from the underlying ``delete_bucket`` operation

    Raises:
        ValueError: If ``bucket_name`` is empty
        RuntimeError: If another bucket deletion helper is in flight on this client

    Usage:
        force_delete_bucket(s3_client, 'my-bucket')
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)

    injector = create_header_injector(
        s3_client, "DeleteBucket", {"X-Tigris-Force-Delete": "true"}
    )

    with _guard_lock:
        if has_active_injector(s3_client, "DeleteBucket"):
            msg = (
                "a DeleteBucket header injection is already active on this client; "
                "wait for the other call to finish or use a separate client"
            )
            raise RuntimeError(msg)
        injector.register()

    try:
        return cast("dict[str, Any]", s3_client.delete_bucket(Bucket=bucket_name))
    finally:
        injector.unregister()


def list_deleted_buckets(
    s3_client: S3Client,
    *,
    max_buckets: Optional[int] = None,
    continuation_token: Optional[str] = None,
) -> dict[str, Any]:
    """
    List one page of the account's soft-deleted buckets.

    These are buckets deleted while soft delete was enabled; each stays
    recoverable with ``restore_bucket`` until its retention window ends, and
    its name stays reserved meanwhile. This reads Tigris's
    ``GET /?OnlyDeleted=true`` endpoint, which is not an S3 operation. Pass the
    returned ``continuation_token`` back in to fetch the next page.

    Args:
        s3_client: boto3 S3 client instance
        max_buckets: Page size; the gateway default applies when None
        continuation_token: ``continuation_token`` from the previous page

    Returns:
        {
            'buckets': [{'name': str, 'created_at': datetime or None,
                         'modified_at': datetime or None, 'retention_days': int or None}],
            'continuation_token': str or None,   # None on the last page
            'raw': dict,                          # the full response body
        }
        ``modified_at`` is the gateway's ``CreationDate``: the last change to the
        bucket, in practice its deletion.

    Raises:
        ValueError: If ``max_buckets`` is not a positive integer
        TigrisAPIError: If the gateway answers with an error

    Usage:
        page = list_deleted_buckets(s3_client, max_buckets=50)
        for bucket in page['buckets']:
            print(bucket['name'], bucket['retention_days'])
    """
    query: dict[str, Optional[str]] = {"OnlyDeleted": "true"}
    if max_buckets is not None:
        if (
            isinstance(max_buckets, bool)
            or not isinstance(max_buckets, int)
            or max_buckets < 1
        ):
            msg = "max_buckets must be a positive integer"
            raise ValueError(msg)
        query["max-buckets"] = str(max_buckets)
    if continuation_token:
        query["continuation-token"] = continuation_token

    response = signed_request(
        s3_client, "GET", "", query=query, headers={"Accept": "application/json"}
    )
    raw = response.json()
    entries = (raw.get("Buckets") or {}).get("Bucket") or []
    if isinstance(entries, dict):  # a single entry can arrive unwrapped
        entries = [entries]

    return {
        "buckets": [
            {
                "name": entry.get("Name"),
                "created_at": parse_timestamp(entry.get("InitialCreatedDate")),
                "modified_at": parse_timestamp(entry.get("CreationDate")),
                "retention_days": (entry.get("SoftDeleteInfo") or {}).get(
                    "RetentionDays"
                ),
            }
            for entry in entries
        ],
        "continuation_token": raw.get("ContinuationToken") or None,
        "raw": raw,
    }


def restore_bucket(s3_client: S3Client, bucket_name: str) -> dict[str, Any]:
    """
    Bring a soft-deleted bucket back, with the objects it held when deleted.

    This reads Tigris's ``POST /{bucket}?restore`` endpoint, which is not an S3
    operation. The bucket must be in the soft-deleted state: deleted while soft
    delete was enabled and still within its retention window.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the soft-deleted bucket

    Returns:
        {'name': bucket_name, 'request_id': str or None}

    Raises:
        ValueError: If ``bucket_name`` is empty
        TigrisAPIError: 400 ``InvalidArgument`` when the bucket is not
            soft-deleted (for example it is live), 404 ``NoSuchBucket`` when it
            does not exist or its retention window has already expired

    Usage:
        restore_bucket(s3_client, 'my-bucket')
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)

    response = signed_request(s3_client, "POST", bucket_name, query="restore")
    return {"name": bucket_name, "request_id": response.headers.get("x-amz-request-id")}
