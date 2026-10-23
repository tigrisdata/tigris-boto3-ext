"""Bucket deletion: soft delete at creation, force delete, and recovering deleted buckets."""

import json
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
from tigris_boto3_ext._transport import paging_params, signed_request
from tigris_boto3_ext._utils import parse_timestamp
from tigris_boto3_ext.buckets._payload import listing_entries

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

        _validate_retention_days(retention_days)
        header_value = "true" if retention_days is None else str(retention_days)

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
    query.update(paging_params(max_buckets, continuation_token))

    response = signed_request(
        s3_client, "GET", "", query=query, headers={"Accept": "application/json"}
    )
    raw = response.json()
    entries = listing_entries(raw)

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


def set_bucket_soft_delete(
    s3_client: S3Client,
    bucket_name: str,
    enabled: bool,
    *,
    retention_days: Optional[int] = None,
) -> dict[str, Any]:
    """
    Turn soft delete on or off for an existing bucket, or change its window.

    This reads Tigris's ``PATCH /{bucket}`` endpoint, which is not an S3
    operation. Enabling makes future object and bucket deletions recoverable
    for the retention window; calling it again with another ``retention_days``
    changes the window; disabling makes future deletions permanent again.

    The change reaches the bucket's own deletion at once, but object deletes
    pick it up after a short, variable delay in either direction (observed from
    immediately to tens of seconds). After enabling, do not rely on a delete
    being recoverable until a throwaway delete shows up in
    ``list_deleted_object_versions``. Versions that were already soft-deleted
    stay listed until their own window ends, whatever the current setting.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        enabled: True to enable soft delete, False to disable it
        retention_days: Retention window in whole days, 7 to 90, when enabling.
            None uses the default 7-day window. Not allowed when disabling.

    Returns:
        {'name': bucket_name, 'request_id': str or None, 'raw': dict}, where
        ``raw`` is the gateway's response body
        (``{"Bucket": ..., "Update": "success"}``)

    Raises:
        TypeError: If ``enabled`` is not a bool or ``retention_days`` is not an int
        ValueError: If ``bucket_name`` is empty, ``retention_days`` is outside
            7 to 90, or ``retention_days`` is given while disabling
        TigrisAPIError: If the gateway answers with an error, for example
            ``NoSuchBucket`` (404)

    Usage:
        set_bucket_soft_delete(s3_client, 'my-bucket', True, retention_days=30)
        set_bucket_soft_delete(s3_client, 'my-bucket', False)
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    if not isinstance(enabled, bool):
        msg = f"enabled must be a bool, got {enabled!r}"
        raise TypeError(msg)
    if not enabled and retention_days is not None:
        msg = "retention_days only applies when enabling soft delete"
        raise ValueError(msg)
    _validate_retention_days(retention_days)

    settings: dict[str, Any] = {"enabled": enabled}
    if retention_days is not None:
        settings["retention_days"] = retention_days

    response = signed_request(
        s3_client,
        "PATCH",
        bucket_name,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        body=json.dumps({"soft_delete": settings}),
    )
    raw = response.json() if response.body.strip() else {}
    return {
        "name": bucket_name,
        "request_id": response.headers.get("x-amz-request-id"),
        "raw": raw,
    }


def _validate_retention_days(retention_days: Optional[int]) -> None:
    """Reject retention windows the gateway would reject, before any request is sent."""
    if retention_days is None:
        return
    if isinstance(retention_days, bool) or not isinstance(retention_days, int):
        msg = f"retention_days must be a whole number of days, got {retention_days!r}"
        raise TypeError(msg)
    low = TigrisSoftDeleteEnabled.MIN_RETENTION_DAYS
    high = TigrisSoftDeleteEnabled.MAX_RETENTION_DAYS
    if not low <= retention_days <= high:
        msg = f"retention_days must be between {low} and {high}, got {retention_days}"
        raise ValueError(msg)
