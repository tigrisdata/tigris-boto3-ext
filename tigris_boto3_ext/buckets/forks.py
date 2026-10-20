"""Bucket forks: creating buckets that branch from another bucket or snapshot, and listing them."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._internal import create_header_injector
from tigris_boto3_ext._payload import parse_listing_page
from tigris_boto3_ext._transport import paging_params, signed_request

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


def list_forks(
    s3_client: S3Client,
    source_bucket: str,
    *,
    max_buckets: Optional[int] = None,
    continuation_token: Optional[str] = None,
) -> dict[str, Any]:
    """
    List the buckets forked from a bucket.

    This reads Tigris's ``GET /`` listing with the ``X-Tigris-Fork`` header,
    which is not an S3 operation, so the result is a plain dict. Unlike the
    ``is_fork_parent`` flag of ``get_bucket_metadata``, this is the gateway
    enumerating the children and is current as soon as a fork exists.

    Args:
        s3_client: boto3 S3 client instance
        source_bucket: The bucket whose forks to list
        max_buckets: Page size; the gateway default applies when None
        continuation_token: ``continuation_token`` from the previous page

    Returns:
        {
            'buckets': [{'name', 'created_at', 'fork_parents': [{'bucket', 'snapshot',
                         'snapshot_name', 'forked_at', 'snapshot_created_at'}], ...}],
            'continuation_token': str or None,
            'raw': dict,
        }
        Entries have the shape of ``list_buckets`` with ``fork_parents`` and
        ``is_fork_parent`` filled; ``created_at`` is when the fork was made.

    Raises:
        ValueError: If ``source_bucket`` is empty or ``max_buckets`` is invalid
        TigrisAPIError: 403 ``AccessDenied`` when the source bucket does not
            exist or is not yours; a bucket without forks returns an empty list

    Usage:
        for fork in list_forks(s3_client, 'production-data')['buckets']:
            print(fork['name'], fork['fork_parents'][0]['snapshot'])
    """
    if not source_bucket:
        msg = "source_bucket is required"
        raise ValueError(msg)
    response = signed_request(
        s3_client,
        "GET",
        "",
        query=paging_params(max_buckets, continuation_token),
        headers={"Accept": "application/json", "X-Tigris-Fork": source_bucket},
    )
    page = parse_listing_page(response.json())
    del page["owner"]  # the gateway sends an empty owner on fork listings
    return page
