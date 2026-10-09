"""Object rename: changing an object's key in place without rewriting its data."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._internal import create_header_injector

F = TypeVar("F", bound=Callable[..., Any])
""" Context Managers """


class TigrisRename:
    """
    Context manager that converts CopyObject calls into rename operations.

    Tigris implements object rename as a CopyObject request with the
    ``X-Tigris-Rename: true`` header. While this context manager is active,
    every ``copy_object`` call made on the wrapped client becomes a rename
    (no data is rewritten — only the key is updated). Scope the context
    tightly to the rename call(s) so unrelated copies are not affected.

    Usage:
        with TigrisRename(s3_client):
            s3_client.copy_object(
                Bucket='my-bucket',
                CopySource='my-bucket/old-key.txt',
                Key='new-key.txt',
            )
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
            "CopyObject",
            {"X-Tigris-Rename": "true"},
        )

    def __enter__(self) -> "TigrisRename":
        """Enter context and register event handler."""
        self._injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister event handler."""
        self._injector.unregister()


""" Decorators """


def with_rename(func: F) -> F:
    """
    Decorator that converts CopyObject calls inside the function into rename
    operations.

    The decorated function must accept an s3_client as its first argument.
    Any ``copy_object`` call made on that client during execution becomes a
    Tigris rename (X-Tigris-Rename: true), so keep the function scope narrow.

    Usage:
        @with_rename
        def rename_file(s3_client, bucket, old_key, new_key):
            return s3_client.copy_object(
                Bucket=bucket,
                CopySource=f"{bucket}/{old_key}",
                Key=new_key,
            )

        rename_file(s3_client, 'my-bucket', 'old.txt', 'new.txt')
    """

    @wraps(func)
    def wrapper(s3_client: Any, *args: Any, **kwargs: Any) -> Any:
        with TigrisRename(s3_client):
            return func(s3_client, *args, **kwargs)

    return wrapper  # type: ignore


""" Helpers """


def rename_object(
    s3_client: S3Client,
    bucket_name: str,
    source_key: str,
    destination_key: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Rename an object within a bucket without rewriting its data.

    Tigris implements rename as a ``copy_object`` request with the
    ``X-Tigris-Rename: true`` header. This helper scopes the header injection
    to a single call so unrelated ``copy_object`` invocations are unaffected.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket containing the object
        source_key: Current key of the object to rename
        destination_key: New key for the object
        **kwargs: Additional arguments to pass to ``copy_object``

    Returns:
        Response from the underlying ``copy_object`` operation

    Usage:
        rename_object(s3_client, 'my-bucket', 'old-name.txt', 'new-name.txt')
    """
    with TigrisRename(s3_client):
        return cast(
            "dict[str, Any]",
            s3_client.copy_object(
                Bucket=bucket_name,
                # Dict form lets botocore percent-encode the key. The string
                # form `bucket/key` is treated as already-encoded, so it
                # corrupts keys containing spaces, '+', '?', '#', etc.
                CopySource={"Bucket": bucket_name, "Key": source_key},
                Key=destination_key,
                **kwargs,
            ),
        )
