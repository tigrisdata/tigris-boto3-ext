"""Object migration from a bucket's shadow source: schedule it, check it, list either side."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._internal import create_header_injector

F = TypeVar("F", bound=Callable[..., Any])

LIST_SOURCES = ("tigris", "shadow")

""" Context Managers """


class TigrisListSource:
    """
    Context manager that makes ``list_objects_v2`` list one side of a bucket
    under migration: ``"tigris"`` for the objects already in Tigris,
    ``"shadow"`` for the objects in the source bucket.

    The header applies to every ``list_objects_v2`` issued through this client
    while the block is active, from any thread. Do not share the client with
    other threads that list objects during the block.

    Usage:
        with TigrisListSource(s3_client, "shadow"):
            remaining = s3_client.list_objects_v2(Bucket='my-bucket')
    """

    def __init__(self, s3_client: S3Client, source: str):
        """
        Initialize context manager.

        Args:
            s3_client: boto3 S3 client instance
            source: ``"tigris"`` or ``"shadow"``

        Raises:
            ValueError: If ``source`` is anything else
        """
        if source not in LIST_SOURCES:
            msg = f"source must be 'tigris' or 'shadow', got {source!r}"
            raise ValueError(msg)
        self.client = s3_client
        self.source = source
        self._injector = create_header_injector(
            s3_client, "ListObjectsV2", {"X-Tigris-List-Source": source}
        )

    def __enter__(self) -> "TigrisListSource":
        """Enter context and register event handler."""
        self._injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister event handler."""
        self._injector.unregister()


""" Decorators """


def with_list_source(source: str) -> Callable[[F], F]:
    """
    Decorator that makes ``list_objects_v2`` calls inside the function list one
    side of a bucket under migration. See ``TigrisListSource``.

    The decorated function must accept an s3_client as its first argument.

    Usage:
        @with_list_source("shadow")
        def remaining(s3_client, bucket):
            return s3_client.list_objects_v2(Bucket=bucket)
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(s3_client: Any, *args: Any, **kwargs: Any) -> Any:
            with TigrisListSource(s3_client, source):
                return func(s3_client, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


""" Helpers """


def migrate_object(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Ask Tigris to copy an object from the bucket's shadow source now, instead of
    waiting for the first read.

    This is a ``head_object`` with ``X-Tigris-Schedule-Migration: true``, which
    schedules the copy; the call does not wait for it. On a bucket without a
    migration source, or for an object already in Tigris, it is a plain
    ``head_object``.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Object key
        **kwargs: Additional arguments to pass to ``head_object``

    Returns:
        Response from the underlying ``head_object`` operation

    Raises:
        ValueError: If ``bucket_name`` or ``key`` is empty

    Usage:
        migrate_object(s3_client, 'assets', 'images/logo.png')
    """
    _require(bucket_name, key)
    injector = create_header_injector(
        s3_client, "HeadObject", {"X-Tigris-Schedule-Migration": "true"}
    )
    injector.register()
    try:
        return cast(
            "dict[str, Any]",
            s3_client.head_object(Bucket=bucket_name, Key=key, **kwargs),
        )
    finally:
        injector.unregister()


def is_object_migrated(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    **kwargs: Any,
) -> bool:
    """
    Whether an object is already stored in Tigris rather than only in the
    bucket's shadow source.

    Reads the ``X-Tigris-Read-Source`` header of a ``head_object`` response;
    ``block_shadow`` means the object still has to come from the source. Objects
    of a bucket without a migration source are always migrated.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Object key
        **kwargs: Additional arguments to pass to ``head_object``

    Raises:
        ValueError: If ``bucket_name`` or ``key`` is empty

    Usage:
        if not is_object_migrated(s3_client, 'assets', 'images/logo.png'):
            migrate_object(s3_client, 'assets', 'images/logo.png')
    """
    _require(bucket_name, key)
    response = s3_client.head_object(Bucket=bucket_name, Key=key, **kwargs)
    headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    return str(headers.get("x-tigris-read-source", "")).lower() != "block_shadow"


def list_objects_from_source(
    s3_client: S3Client,
    bucket_name: str,
    source: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    List one side of a bucket under migration: the objects already in Tigris
    (``"tigris"``) or the objects in the source bucket (``"shadow"``).

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        source: ``"tigris"`` or ``"shadow"``
        **kwargs: Additional arguments to pass to ``list_objects_v2``, such as
            ``Prefix``, ``MaxKeys`` or ``ContinuationToken``

    Returns:
        Response from the underlying ``list_objects_v2`` operation

    Raises:
        ValueError: If ``bucket_name`` is empty or ``source`` is not one of the
            two values

    Usage:
        remaining = list_objects_from_source(s3_client, 'assets', 'shadow', Prefix='images/')
        for obj in remaining.get('Contents', []):
            print(obj['Key'])
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    with TigrisListSource(s3_client, source):
        return cast(
            "dict[str, Any]",
            s3_client.list_objects_v2(Bucket=bucket_name, **kwargs),
        )


def _require(bucket_name: str, key: str) -> None:
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    if not key:
        msg = "key is required"
        raise ValueError(msg)
