"""Bucket creation options: default storage class, regions and public listing."""

from collections.abc import Sequence
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, TypeVar, Union, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._internal import create_header_injector

F = TypeVar("F", bound=Callable[..., Any])

_STORAGE_CLASSES = ("STANDARD", "STANDARD_IA", "GLACIER", "GLACIER_IR")
_ACCESS_VALUES = ("public", "private")
""" Context Managers """


class TigrisBucketOptions:
    """
    Context manager that applies Tigris creation options to ``create_bucket``.

    Every ``create_bucket`` issued through the client while the block is active
    gets the options, from any thread. Options that are left as None are not
    sent, so the gateway defaults apply: STANDARD storage class, global
    placement, no anonymous listing.

    Args:
        s3_client: boto3 S3 client instance
        default_storage_class: Storage class new objects get unless the upload
            says otherwise: STANDARD, STANDARD_IA, GLACIER or GLACIER_IR
        regions: Where the data lives. A region code (``"iad"``), a multi-region
            code (``"usa"``, ``"eur"``), or a list of region codes for dual-region
            placement (``["iad", "sjc"]``)
        public_list_objects: Whether anonymous requests may list the bucket.
            Only meaningful on a bucket created with ``ACL="public-read"``

    Raises:
        ValueError: If ``default_storage_class`` is unknown or ``regions`` is empty
        TypeError: If ``public_list_objects`` is not a bool

    Usage:
        with TigrisBucketOptions(s3_client, default_storage_class="STANDARD_IA", regions="iad"):
            s3_client.create_bucket(Bucket='my-bucket')
    """

    def __init__(
        self,
        s3_client: S3Client,
        default_storage_class: Optional[str] = None,
        regions: Optional[Union[str, Sequence[str]]] = None,
        public_list_objects: Optional[bool] = None,
    ):
        self.client = s3_client
        headers = _bucket_option_headers(
            default_storage_class, regions, public_list_objects
        )
        self._injector = (
            create_header_injector(s3_client, "CreateBucket", headers)
            if headers
            else None
        )

    def __enter__(self) -> "TigrisBucketOptions":
        """Enter context and register the event handler."""
        if self._injector is not None:
            self._injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister the event handler."""
        if self._injector is not None:
            self._injector.unregister()


""" Decorators """


def with_bucket_options(
    default_storage_class: Optional[str] = None,
    regions: Optional[Union[str, Sequence[str]]] = None,
    public_list_objects: Optional[bool] = None,
) -> Callable[[F], F]:
    """
    Decorator that applies Tigris creation options to the ``create_bucket``
    calls made inside the decorated function.

    The decorated function must accept an s3_client as its first argument.
    Takes the same options as ``TigrisBucketOptions``.

    Usage:
        @with_bucket_options(default_storage_class="STANDARD_IA", regions="iad")
        def create_archive_bucket(s3_client, bucket_name):
            return s3_client.create_bucket(Bucket=bucket_name)
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(s3_client: Any, *args: Any, **kwargs: Any) -> Any:
            with TigrisBucketOptions(
                s3_client,
                default_storage_class=default_storage_class,
                regions=regions,
                public_list_objects=public_list_objects,
            ):
                return func(s3_client, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


""" Helpers """


def create_bucket(
    s3_client: S3Client,
    bucket_name: str,
    *,
    access: Optional[Literal["public", "private"]] = None,
    default_storage_class: Optional[str] = None,
    regions: Optional[Union[str, Sequence[str]]] = None,
    public_list_objects: Optional[bool] = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Create a bucket with Tigris creation options in one call.

    ``access="public"`` creates the bucket with the ``public-read`` ACL, so
    anonymous requests can read its objects at
    ``https://<bucket>.t3.tigrisfiles.io/<key>``. Listing stays private unless
    ``public_list_objects`` is True. The other options are sent as headers;
    see ``TigrisBucketOptions``. Snapshots, soft delete and forks are separate
    creation-time settings with their own context managers, and they combine
    with this helper::

        with TigrisSnapshotEnabled(s3_client):
            create_bucket(s3_client, 'my-bucket', access='public', regions='iad')

    Like every helper here, the headers are registered on the client for the
    duration of the call, so a ``create_bucket`` issued from another thread on
    the same client in that window gets them too. Use a client per thread for
    concurrent bucket creation, especially with ``public_list_objects``.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to create
        access: ``"public"`` for anonymous read access to objects, ``"private"``
            (the default) for none
        default_storage_class: Storage class new objects get unless the upload
            says otherwise: STANDARD, STANDARD_IA, GLACIER or GLACIER_IR
        regions: A region code, a multi-region code, or a list of region codes
            for dual-region placement
        public_list_objects: Whether anonymous requests may list the bucket;
            requires ``access="public"``
        **kwargs: Additional arguments to pass to ``create_bucket``

    Returns:
        Response from the underlying ``create_bucket`` operation

    Raises:
        ValueError: If ``bucket_name`` is empty, ``access`` is not public or
            private, ``ACL`` is also passed in ``kwargs``,
            ``public_list_objects`` is requested on a private bucket,
            ``default_storage_class`` is unknown or ``regions`` is empty
        TypeError: If ``public_list_objects`` is not a bool

    Usage:
        create_bucket(s3_client, 'my-bucket', access='public', regions=['iad', 'sjc'])
        create_bucket(s3_client, 'my-archive', default_storage_class='STANDARD_IA')
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    if access is not None and access not in _ACCESS_VALUES:
        msg = f"access must be 'public' or 'private', got {access!r}"
        raise ValueError(msg)
    if access is not None and "ACL" in kwargs:
        msg = "pass either access or ACL, not both"
        raise ValueError(msg)
    if public_list_objects and access != "public":
        msg = "public_list_objects requires access='public'"
        raise ValueError(msg)
    if access == "public":
        kwargs["ACL"] = "public-read"

    with TigrisBucketOptions(
        s3_client,
        default_storage_class=default_storage_class,
        regions=regions,
        public_list_objects=public_list_objects,
    ):
        return cast(
            "dict[str, Any]", s3_client.create_bucket(Bucket=bucket_name, **kwargs)
        )


def _bucket_option_headers(
    default_storage_class: Optional[str],
    regions: Optional[Union[str, Sequence[str]]],
    public_list_objects: Optional[bool],
) -> dict[str, str]:
    """Translate the creation options into the headers Tigris reads on CreateBucket."""
    headers: dict[str, str] = {}

    if default_storage_class is not None:
        if default_storage_class not in _STORAGE_CLASSES:
            msg = (
                f"default_storage_class must be one of {', '.join(_STORAGE_CLASSES)}, "
                f"got {default_storage_class!r}"
            )
            raise ValueError(msg)
        headers["X-Amz-Storage-Class"] = default_storage_class

    if regions is not None:
        names = [regions] if isinstance(regions, str) else list(regions)
        cleaned = [name.strip() for name in names if name and name.strip()]
        if not cleaned:
            msg = "regions must name at least one region"
            raise ValueError(msg)
        headers["X-Tigris-Regions"] = ",".join(cleaned)

    if public_list_objects is not None:
        if not isinstance(public_list_objects, bool):
            msg = "public_list_objects must be a bool"
            raise TypeError(msg)
        headers["X-Amz-Acl-Public-List-Objects-Enabled"] = (
            "true" if public_list_objects else "false"
        )

    return headers
