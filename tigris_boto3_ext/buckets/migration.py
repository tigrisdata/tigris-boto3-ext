"""Data migration: lazily copy a bucket's objects from an S3 bucket outside Tigris."""

import json
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._transport import signed_request
from tigris_boto3_ext.buckets import _validate


def set_bucket_migration(
    s3_client: S3Client,
    bucket_name: str,
    source_bucket: Optional[str],
    *,
    endpoint: Optional[str] = None,
    region: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    write_through: bool = False,
) -> dict[str, Any]:
    """
    Migrate data into a bucket from an S3 bucket outside Tigris, or stop doing so.

    While a source (shadow bucket) is configured, a read of an object the bucket
    does not have yet is served from the source and the object is copied
    into Tigris in the background; nothing is transferred up front. With
    ``write_through`` enabled, writes go to both buckets. This uses Tigris's
    ``PATCH /{bucket}`` endpoint, which is not an S3 operation, so the result is
    a plain dict. The gateway checks the credentials against the source before
    it accepts the configuration. Pass ``source_bucket=None`` to remove it.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket in Tigris
        source_bucket: Name of the bucket to migrate from, or None to remove
            the configuration
        endpoint: ``https://`` S3 endpoint of the source, outside Tigris
        region: Region of the source bucket
        access_key: Access key for the source
        secret_key: Secret key for the source
        write_through: Also write new objects to the source

    Returns:
        {'name': bucket_name, 'request_id': str or None, 'raw': dict}

    Raises:
        ValueError: If ``bucket_name`` is empty, ``source_bucket`` is given
            without ``endpoint``, ``region``, ``access_key`` and ``secret_key``,
            ``endpoint`` is not an ``https://`` URL with a host, or settings
            are passed with ``source_bucket=None``
        TypeError: If ``write_through`` is not a bool
        TigrisAPIError: If the gateway answers with an error, for example a
            Tigris endpoint as source (400 ``InvalidArgument``), credentials the
            source rejects (403 ``InvalidAccessKeyId``) or ``NoSuchBucket`` (404)

    Usage:
        set_bucket_migration(
            s3_client, 'assets', 'legacy-assets',
            endpoint='https://s3.amazonaws.com', region='us-east-1',
            access_key=..., secret_key=...,
        )
        set_bucket_migration(s3_client, 'assets', None)
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)

    settings: dict[str, Any] = {}
    if source_bucket is None:
        if (
            any(
                value is not None
                for value in (endpoint, region, access_key, secret_key)
            )
            or write_through
        ):
            msg = "source_bucket=None removes the configuration; pass no other settings with it"
            raise ValueError(msg)
    else:
        required = {
            "source_bucket": source_bucket,
            "endpoint": endpoint,
            "region": region,
            "access_key": access_key,
            "secret_key": secret_key,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            msg = f"a migration source needs {', '.join(missing)}"
            raise ValueError(msg)
        settings = {
            "name": source_bucket,
            "endpoint": _validate.https_url(endpoint or "", "endpoint"),
            "region": region,
            "access_key": access_key,
            "secret_key": secret_key,
            "write_through": _validate.flag(write_through, "write_through"),
        }

    response = signed_request(
        s3_client,
        "PATCH",
        bucket_name,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        body=json.dumps({"shadow_bucket": settings}),
    )
    raw = response.json() if response.body.strip() else {}
    return {
        "name": bucket_name,
        "request_id": response.headers.get("x-amz-request-id"),
        "raw": raw,
    }
