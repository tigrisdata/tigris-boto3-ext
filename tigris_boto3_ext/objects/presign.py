"""Presigned URLs from Tigris's presign endpoint."""

import json
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._transport import client_credentials, signed_request
from tigris_boto3_ext.objects.snapshots import head_object_from_snapshot

PRESIGN_OPERATIONS = ("get", "put")


def presign(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    operation: str = "get",
    *,
    expires_in: int = 3600,
    snapshot_version: Optional[str] = None,
) -> dict[str, Any]:
    """
    Get a presigned URL for an object from Tigris's presign endpoint.

    The URL is signed with the client's own access key and served from the
    public domain (``https://<bucket>.t3.tigrisfiles.io/<key>?...``), on private
    buckets too. This uses ``POST /?func=presign``, which is not an S3
    operation. boto3's ``generate_presigned_url`` also works against Tigris; this
    endpoint adds a URL on the bucket's custom domain and reads pinned to the
    object as it was at a snapshot.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Object key
        operation: ``"get"`` to download, ``"put"`` to upload
        expires_in: Seconds the URL stays valid, at least 1
        snapshot_version: With ``"get"``, sign for the object as it was at this
            snapshot. The version is resolved with ``head_object_from_snapshot``

    Returns:
        {
            'url': str,
            'custom_domain_url': str or None,  # when the bucket has a custom domain
            'operation': 'get' or 'put',
            'expires_in': int,
            'version_id': str or None,         # the object version a snapshot resolved to
        }

    Raises:
        ValueError: If ``bucket_name`` or ``key`` is empty, ``operation`` is not
            ``"get"`` or ``"put"``, ``expires_in`` is below 1, or
            ``snapshot_version`` is empty or passed with ``"put"``
        TypeError: If ``expires_in`` is not an integer
        botocore.exceptions.ClientError: If the object did not exist at
            ``snapshot_version`` (404)
        TigrisAPIError: If the presign endpoint answers with an error

    Usage:
        link = presign(s3_client, 'assets', 'report.pdf', expires_in=600)['url']
        upload = presign(s3_client, 'assets', 'incoming/photo.jpg', 'put')['url']
        old = presign(s3_client, 'assets', 'config.json', snapshot_version='1792757954482613190')['url']
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)
    if not key:
        msg = "key is required"
        raise ValueError(msg)
    if operation not in PRESIGN_OPERATIONS:
        msg = f"operation must be 'get' or 'put', got {operation!r}"
        raise ValueError(msg)
    if isinstance(expires_in, bool) or not isinstance(expires_in, int):
        msg = "expires_in must be a whole number of seconds"
        raise TypeError(msg)
    if expires_in < 1:
        msg = "expires_in must be at least 1 second"
        raise ValueError(msg)
    if snapshot_version is not None and operation != "get":
        msg = (
            "snapshot_version only applies to operation='get'; snapshots are read-only"
        )
        raise ValueError(msg)
    if snapshot_version is not None and not snapshot_version:
        msg = "snapshot_version must not be empty; pass None for the live object"
        raise ValueError(msg)

    body: dict[str, Any] = {
        "bucket": bucket_name,
        "key": key,
        "key_id": client_credentials(s3_client).access_key,
        "type": operation,
        "expires_in": expires_in,
    }
    version_id: Optional[str] = None
    if snapshot_version is not None:
        version_id = head_object_from_snapshot(
            s3_client, bucket_name, key, snapshot_version
        ).get("VersionId")
        if not version_id:
            msg = f"the gateway reported no version for {key!r} at snapshot {snapshot_version}"
            raise ValueError(msg)
        body["version_id"] = version_id

    response = signed_request(
        s3_client,
        "POST",
        "",
        query={"func": "presign"},
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        body=json.dumps(body),
    )
    data = response.json()
    return {
        "url": data.get("url"),
        "custom_domain_url": data.get("custom_domain_url") or None,
        "operation": operation,
        "expires_in": expires_in,
        "version_id": version_id,
    }
