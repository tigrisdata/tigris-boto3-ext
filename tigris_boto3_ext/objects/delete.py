"""Object deletion: the soft-delete view, listing, purge and restore."""

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


class TigrisSoftDeleteView:
    """
    Context manager that points delete and list operations at a bucket's
    soft-deleted objects instead of its live ones.

    The header applies to every matching operation issued through this client
    while the block is active, from any thread. Do not share the client with
    other threads that delete or list objects during the block.

    Inside the block:
    - ``delete_object`` with a ``VersionId`` permanently removes that
      soft-deleted version before its retention window expires.
    - ``list_object_versions`` and ``list_objects_v2`` return soft-deleted
      objects instead of live ones.

    Usage:
        with TigrisSoftDeleteView(s3_client):
            deleted = s3_client.list_object_versions(Bucket='my-bucket')
            s3_client.delete_object(
                Bucket='my-bucket', Key='file.txt', VersionId='1787441627070249004'
            )
    """

    OPERATIONS = ("DeleteObject", "ListObjectVersions", "ListObjectsV2")

    def __init__(self, s3_client: S3Client):
        """
        Initialize context manager.

        Args:
            s3_client: boto3 S3 client instance
        """
        self.client = s3_client
        self._injectors = create_multi_operation_injector(
            s3_client,
            list(self.OPERATIONS),
            {"X-Tigris-Soft-Delete": "true"},
        )

    def __enter__(self) -> "TigrisSoftDeleteView":
        """Enter context and register event handlers."""
        for injector in self._injectors:
            injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister event handlers."""
        for injector in self._injectors:
            injector.unregister()


""" Decorators """


def with_soft_delete_view(func: F) -> F:
    """
    Decorator that points delete and list operations inside the function at the
    bucket's soft-deleted objects. See ``TigrisSoftDeleteView``.

    The decorated function must accept an s3_client as its first argument.

    Usage:
        @with_soft_delete_view
        def list_deleted(s3_client, bucket):
            return s3_client.list_object_versions(Bucket=bucket)
    """

    @wraps(func)
    def wrapper(s3_client: Any, *args: Any, **kwargs: Any) -> Any:
        with TigrisSoftDeleteView(s3_client):
            return func(s3_client, *args, **kwargs)

    return wrapper  # type: ignore


""" Helpers """


def list_deleted_objects(
    s3_client: S3Client,
    bucket_name: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    List the soft-deleted objects in a bucket.

    Only recoverable objects are returned; live objects are left out entirely.
    Use ``list_deleted_object_versions`` when you need version ids for
    ``purge_deleted_object`` or ``restore_deleted_object``.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        **kwargs: Additional arguments to pass to ``list_objects_v2``, such as
            ``Prefix``, ``Delimiter``, ``MaxKeys`` or ``ContinuationToken``

    Returns:
        Response from the underlying ``list_objects_v2`` operation

    Usage:
        deleted = list_deleted_objects(s3_client, 'my-bucket', Prefix='logs/')
        for obj in deleted.get('Contents', []):
            print(obj['Key'])
    """
    with TigrisSoftDeleteView(s3_client):
        return cast(
            "dict[str, Any]",
            s3_client.list_objects_v2(Bucket=bucket_name, **kwargs),
        )


def list_deleted_object_versions(
    s3_client: S3Client,
    bucket_name: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    List the recoverable versions of a bucket's soft-deleted objects.

    Each entry carries the ``VersionId`` that ``purge_deleted_object`` and
    ``restore_deleted_object`` take. Only soft-deleted versions are returned,
    on snapshot-enabled buckets as well; live versions never appear.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        **kwargs: Additional arguments to pass to ``list_object_versions``,
            such as ``Prefix``, ``MaxKeys``, ``KeyMarker`` or ``VersionIdMarker``

    Returns:
        Response from the underlying ``list_object_versions`` operation

    Usage:
        deleted = list_deleted_object_versions(s3_client, 'my-bucket')
        for version in deleted.get('Versions', []):
            print(version['Key'], version['VersionId'])
    """
    with TigrisSoftDeleteView(s3_client):
        return cast(
            "dict[str, Any]",
            s3_client.list_object_versions(Bucket=bucket_name, **kwargs),
        )


def purge_deleted_object(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    version_id: str,
) -> dict[str, Any]:
    """
    Permanently delete one soft-deleted version of an object.

    The version is removed before its retention window expires and can no
    longer be restored. ``version_id`` comes from the bucket's soft-delete view
    (``TigrisSoftDeleteView`` around ``list_object_versions``). It is required:
    without it the request would target the live object and soft-delete it
    again instead of purging a version.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Key of the soft-deleted object
        version_id: Version to purge, as listed in the soft-delete view

    Returns:
        Response from the underlying ``delete_object`` operation

    Raises:
        ValueError: If ``bucket_name``, ``key`` or ``version_id`` is empty

    Usage:
        purge_deleted_object(s3_client, 'my-bucket', 'file.txt', '1787441627070249004')
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    if not key:
        msg = "key is required"
        raise ValueError(msg)
    if not version_id:
        msg = "version_id is required"
        raise ValueError(msg)

    with TigrisSoftDeleteView(s3_client):
        return cast(
            "dict[str, Any]",
            s3_client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id),
        )


def restore_deleted_object(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    version_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Restore a soft-deleted object so it is live again.

    This is the undelete counterpart of ``purge_deleted_object``. It is
    unrelated to thawing an archived object from a cold storage tier, which is
    what a plain ``restore_object`` call does.

    The headers are injected through the client's event system, so they apply
    to every ``restore_object`` issued through this client while the call is in
    flight. A second restore on the same client during that window is refused
    rather than risk carrying the wrong version.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Key of the soft-deleted object
        version_id: Soft-deleted version to restore, as listed in the
            soft-delete view. None restores the most recent one.

    Returns:
        Response from the underlying ``restore_object`` operation

    Raises:
        ValueError: If ``bucket_name`` or ``key`` is empty, or ``version_id``
            is an empty string
        RuntimeError: If another restore is in flight on this client

    Usage:
        restore_deleted_object(s3_client, 'my-bucket', 'file.txt')
        restore_deleted_object(s3_client, 'my-bucket', 'file.txt', '1787441627070249004')
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    if not key:
        msg = "key is required"
        raise ValueError(msg)
    if version_id is not None and not version_id:
        msg = (
            "version_id must not be empty; pass None to restore the most recent version"
        )
        raise ValueError(msg)

    headers = {"X-Tigris-Restore-Type": "soft-delete"}
    if version_id is not None:
        headers["X-Tigris-Restore-Version"] = version_id

    injector = create_header_injector(s3_client, "RestoreObject", headers)

    # Check and register under one lock so two threads cannot both pass the check.
    with _guard_lock:
        if has_active_injector(s3_client, "RestoreObject"):
            msg = (
                "a RestoreObject header injection is already active on this client; "
                "wait for the other call to finish or use a separate client"
            )
            raise RuntimeError(msg)
        injector.register()

    try:
        return cast(
            "dict[str, Any]",
            s3_client.restore_object(Bucket=bucket_name, Key=key),
        )
    finally:
        injector.unregister()
