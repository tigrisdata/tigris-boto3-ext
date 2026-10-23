"""Archive restore: thaw objects stored in the GLACIER storage class and read their restore state."""

import re
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._internal import (
    _guard_lock,
    create_header_injector,
    has_active_injector,
)
from tigris_boto3_ext._utils import parse_timestamp

RESTORE_STATUSES = ("archived", "in-progress", "restored")

_EXPIRY_DATE = re.compile(r'expiry-date="([^"]+)"')


def restore_object(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    days: int = 1,
    *,
    version_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Make an archived object readable again for a number of days.

    Objects in the ``GLACIER`` storage class need a restore before they can be
    read; the restore runs in the background and typically takes about an hour.
    Follow it with ``get_restore_info``. After ``days`` the object returns to the
    archive. This is a plain ``restore_object`` with a ``RestoreRequest``; the
    soft-delete undelete is ``restore_deleted_object``.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Object key
        days: How many days the restored copy stays readable, at least 1
        version_id: Restore this version instead of the current one

    Returns:
        Response from the underlying ``restore_object`` operation

    Raises:
        ValueError: If ``bucket_name`` or ``key`` is empty, or ``days`` is below 1
        TypeError: If ``days`` is not an integer
        RuntimeError: If a ``restore_deleted_object`` is in flight on this client,
            whose header would turn this call into an undelete. The call holds
            the operation in turn, so an undelete started meanwhile is refused
        botocore.exceptions.ClientError: From the gateway, for example
            ``RestoreAlreadyInProgress`` (409), ``InvalidObjectState`` (403) for a
            ``GLACIER_IR`` object, or ``NoSuchKey`` (404)

    Usage:
        restore_object(s3_client, 'archive', 'reports/2024.parquet', days=3)
    """
    _require(bucket_name, key)
    if isinstance(days, bool) or not isinstance(days, int):
        msg = "days must be a whole number of days"
        raise TypeError(msg)
    if days < 1:
        msg = "days must be at least 1"
        raise ValueError(msg)

    reservation = create_header_injector(s3_client, "RestoreObject", {})

    with _guard_lock:
        if has_active_injector(s3_client, "RestoreObject"):
            msg = (
                "a RestoreObject header injection is active on this client; "
                "wait for the other restore to finish or use a separate client"
            )
            raise RuntimeError(msg)
        reservation.register()

    kwargs: dict[str, Any] = {"RestoreRequest": {"Days": days}}
    if version_id is not None:
        kwargs["VersionId"] = version_id
    try:
        return cast(
            "dict[str, Any]",
            s3_client.restore_object(Bucket=bucket_name, Key=key, **kwargs),
        )
    finally:
        reservation.unregister()


def get_restore_info(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    *,
    version_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """
    Where an archived object stands: still archived, being restored, or restored.

    Reads ``StorageClass`` and ``Restore`` from ``head_object``. Objects that are
    not in the ``GLACIER`` storage class and have no restore state return None.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Object key
        version_id: Look at this version instead of the current one

    Returns:
        None, or {'status': 'archived' | 'in-progress' | 'restored',
        'expires_at': datetime or None}. ``expires_at`` is when a restored copy
        returns to the archive.

    Raises:
        ValueError: If ``bucket_name`` or ``key`` is empty
        botocore.exceptions.ClientError: From the gateway, for example a 404 for
            a missing key

    Usage:
        info = get_restore_info(s3_client, 'archive', 'reports/2024.parquet')
        if info and info['status'] == 'archived':
            restore_object(s3_client, 'archive', 'reports/2024.parquet')
    """
    _require(bucket_name, key)
    kwargs: dict[str, Any] = {}
    if version_id is not None:
        kwargs["VersionId"] = version_id
    response = s3_client.head_object(Bucket=bucket_name, Key=key, **kwargs)

    restore = response.get("Restore")
    if restore:
        expiry = _EXPIRY_DATE.search(restore)
        return {
            "status": "in-progress"
            if 'ongoing-request="true"' in restore
            else "restored",
            "expires_at": parse_timestamp(expiry.group(1)) if expiry else None,
        }
    if response.get("StorageClass") == "GLACIER":
        return {"status": "archived", "expires_at": None}
    return None


def _require(bucket_name: str, key: str) -> None:
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    if not key:
        msg = "key is required"
        raise ValueError(msg)
