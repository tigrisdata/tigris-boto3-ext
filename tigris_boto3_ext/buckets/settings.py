"""Bucket settings: change access, storage class, placement and headers after creation."""

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._transport import signed_request
from tigris_boto3_ext.buckets import _validate


def update_bucket(
    s3_client: S3Client,
    bucket_name: str,
    *,
    access: Optional[Literal["public", "private"]] = None,
    public_list_objects: Optional[bool] = None,
    allow_object_acl: Optional[bool] = None,
    default_storage_class: Optional[str] = None,
    regions: Optional[Union[str, Sequence[str]]] = None,
    cache_control: Optional[str] = None,
    custom_domain: Optional[str] = None,
    nosniff: Optional[bool] = None,
    snapshot_enabled: Optional[bool] = None,
) -> dict[str, Any]:
    """
    Change a bucket's settings in one call. Only the settings you pass are sent.

    This reads Tigris's ``PATCH /{bucket}`` endpoint, which is not an S3
    operation, so the result is a plain dict. Read settings back with
    ``get_bucket_metadata`` (storage class, regions, cache control, custom
    domain, object ACL, headers, snapshot type), ``get_bucket_acl`` or
    ``list_buckets(include_visibility=True)`` (access). The public domain
    follows access changes asynchronously, usually within seconds.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        access: ``"public"`` for anonymous read access to objects, ``"private"``
            to remove it
        public_list_objects: Whether anonymous requests may list a public bucket
        allow_object_acl: Whether objects may carry their own ACLs
        default_storage_class: Storage class new objects get unless the upload
            says otherwise: STANDARD, STANDARD_IA, GLACIER or GLACIER_IR
        regions: Where the data lives, as for ``create_bucket``; ``[]`` or
            ``""`` returns the bucket to global placement
        cache_control: ``Cache-Control`` value served with objects; ``""`` clears it
        custom_domain: Domain the bucket is served from. The domain's CNAME must
            already point at Tigris, or the gateway rejects it
        nosniff: Whether objects are served with ``X-Content-Type-Options: nosniff``
        snapshot_enabled: ``True`` makes this a snapshot bucket, ``False`` a
            regular one. The gateway refuses to disable snapshots on a fork or
            a bucket with forks, and to enable them on a bucket with lifecycle
            or expiration rules. Existing snapshots are kept while disabled and
            usable again once re-enabled. HeadBucket, metadata and listings
            show the new type at once; object operations follow within seconds

    Returns:
        {'name': bucket_name, 'request_id': str or None, 'raw': dict}

    Raises:
        ValueError: If ``bucket_name`` is empty, nothing is passed, ``access`` or
            ``default_storage_class`` has an unknown value
        TypeError: If a boolean setting is not a bool
        TigrisAPIError: If the gateway answers with an error, for example
            ``NoSuchBucket`` (404), an unknown region, an unverified custom
            domain or a refused snapshot type change (400 ``InvalidArgument``)

    Usage:
        update_bucket(s3_client, 'assets', access='public', cache_control='max-age=3600')
        update_bucket(s3_client, 'archive', default_storage_class='STANDARD_IA', regions='iad')
        update_bucket(s3_client, 'production-data', snapshot_enabled=True)
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)

    headers: dict[str, str] = {}
    body: dict[str, Any] = {}

    if access is not None:
        _validate.access(access)
        headers["X-Amz-Acl"] = "public-read" if access == "public" else "private"
    if public_list_objects is not None:
        _validate.flag(public_list_objects, "public_list_objects")
        headers["X-Amz-Acl-Public-List-Objects-Enabled"] = (
            "true" if public_list_objects else "false"
        )
    if allow_object_acl is not None:
        _validate.flag(allow_object_acl, "allow_object_acl")
        body["acl_settings"] = {"allow_object_acl": allow_object_acl}
    if default_storage_class is not None:
        body["storage_class"] = _validate.storage_class(default_storage_class)
    if regions is not None:
        body["object_regions"] = _validate.regions(regions, allow_empty=True)
    if cache_control is not None:
        body["cache_control"] = cache_control
    if custom_domain is not None:
        body["website"] = {"domain_name": custom_domain}
    if nosniff is not None:
        _validate.flag(nosniff, "nosniff")
        body["additional_http_headers"] = (
            {"X-Content-Type-Options": "nosniff"} if nosniff else None
        )
    if snapshot_enabled is not None:
        _validate.flag(snapshot_enabled, "snapshot_enabled")
        body["type"] = 1 if snapshot_enabled else 0

    if not body and not headers:
        msg = "nothing to update: pass at least one setting"
        raise ValueError(msg)

    response = signed_request(
        s3_client,
        "PATCH",
        bucket_name,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            **headers,
        },
        body=json.dumps(body),
    )
    raw = response.json() if response.body.strip() else {}
    return {
        "name": bucket_name,
        "request_id": response.headers.get("x-amz-request-id"),
        "raw": raw,
    }
