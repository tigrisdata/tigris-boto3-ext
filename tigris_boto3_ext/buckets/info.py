"""Bucket information from Tigris HeadBucket headers and the metadata endpoint."""

from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._transport import signed_request
from tigris_boto3_ext.buckets._payload import (
    parse_fork_info,
    parse_soft_delete,
    parse_timestamp,
)


def has_snapshot_enabled(s3_client: S3Client, bucket_name: str) -> bool:
    """
    Check if a bucket has snapshot support enabled.

    This function makes a HEAD request to the bucket and checks for the
    X-Tigris-Enable-Snapshot header in the response. Note that these are
    custom Tigris headers, not standard AWS S3 headers, and require
    accessing the raw HTTP response.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to check

    Returns:
        True if snapshots are enabled, False otherwise

    Usage:
        if has_snapshot_enabled(s3_client, 'my-bucket'):
            print("Snapshots are enabled")
        else:
            print("Snapshots are not enabled")
    """
    response = cast("dict[str, Any]", s3_client.head_bucket(Bucket=bucket_name))
    headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    return _is_snapshot_enabled_header_set(headers)


def get_bucket_info(s3_client: S3Client, bucket_name: str) -> dict[str, Any]:
    """
    Get comprehensive information about a bucket including Tigris-specific metadata.

    This function retrieves snapshot and fork information for a bucket by making
    a HEAD request and extracting custom Tigris headers from the response.

    The following Tigris-specific information is returned:
    - snapshot_enabled: Whether snapshots are enabled for the bucket
    - fork_source_bucket: The source bucket name if this is a fork
    - fork_source_snapshot: The snapshot version if forked from a snapshot

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket

    Returns:
        Dictionary containing bucket metadata with the following structure:
        {
            'snapshot_enabled': bool,
            'fork_source_bucket': str or None,
            'fork_source_snapshot': str or None,
            'response_metadata': dict  # Full response metadata
        }

    Usage:
        info = get_bucket_info(s3_client, 'my-bucket')
        if info['snapshot_enabled']:
            print("Snapshots are enabled")
        if info['fork_source_bucket']:
            print(f"Forked from: {info['fork_source_bucket']}")
        if info['fork_source_snapshot']:
            print(f"Snapshot version: {info['fork_source_snapshot']}")
    """
    response = cast("dict[str, Any]", s3_client.head_bucket(Bucket=bucket_name))
    headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})

    # Extract Tigris-specific headers
    snapshot_enabled = _is_snapshot_enabled_header_set(headers)

    fork_source_bucket = headers.get("x-tigris-fork-source-bucket")
    fork_source_snapshot = headers.get("x-tigris-fork-source-bucket-snapshot")

    return {
        "snapshot_enabled": snapshot_enabled,
        "fork_source_bucket": fork_source_bucket,
        "fork_source_snapshot": fork_source_snapshot,
        "response_metadata": response,
    }


def get_bucket_metadata(
    s3_client: S3Client, bucket_name: str, *, with_size: bool = False
) -> dict[str, Any]:
    """
    Get a bucket's Tigris metadata: placement, type, forks, soft delete, size.

    This reads Tigris's ``GET /{bucket}?metadata`` endpoint with one signed
    request; it is not an S3 operation, so the result is a plain dict
    rather than a boto3 response. ``get_bucket_info`` is the cheaper HeadBucket
    variant when snapshot and fork-source flags are all you need.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        with_size: Also ask the gateway for size estimates. Off by default
            because it is more work server-side.

    Returns:
        Dictionary with these keys:
        {
            'name': str,
            'created_at': datetime or None,
            'snapshot_enabled': bool,
            'storage_class': str,                # default storage class for new objects
            'regions': list[str],                # [] means global placement
            'is_fork_parent': bool,              # other buckets were forked from this one
            'fork_parents': [{'bucket', 'snapshot', 'snapshot_name', 'forked_at',
                              'snapshot_created_at'}],
            'soft_delete': {'enabled': bool, 'retention_days': int or None},
            'allow_object_acl': bool,
            'cache_control': str or None,
            'custom_domain': str or None,
            'size': {'objects', 'bytes', 'objects_all_versions'} or None,  # None unless with_size
            'raw': dict,                         # the full response body
        }
        Settings families (lifecycle rules, CORS, notifications, shadow bucket,
        shares) are available under ``raw`` in their wire shape. The size
        values are server-side estimates, updated asynchronously; they can
        trail recent writes by a few seconds. ``fork_parents`` is recorded with
        the fork itself; ``is_fork_parent`` on the source bucket is refreshed
        asynchronously and can lag a new fork, so do not rely on it right after
        forking.

    Raises:
        ValueError: If ``bucket_name`` is empty
        TigrisAPIError: If the gateway answers with an error, for example
            ``NoSuchBucket`` (404)

    Usage:
        info = get_bucket_metadata(s3_client, 'my-bucket')
        if info['is_fork_parent']:
            print("Other buckets were forked from this one")
        for parent in info['fork_parents']:
            print(f"Forked from {parent['bucket']} at snapshot {parent['snapshot']}")
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)

    query: dict[str, Optional[str]] = {"metadata": None}
    if with_size:
        query["with-size"] = "true"
    response = signed_request(
        s3_client,
        "GET",
        bucket_name,
        query=query,
        headers={"Accept": "application/json"},
    )
    raw = response.json()

    fork = parse_fork_info(raw.get("ForkInfo"))
    regions = raw.get("object_regions") or ""

    return {
        "name": raw.get("name", bucket_name),
        "created_at": parse_timestamp(raw.get("created_at")),
        "snapshot_enabled": raw.get("type") == 1,
        "storage_class": raw.get("storage_class"),
        "regions": [region.strip() for region in regions.split(",") if region.strip()],
        "is_fork_parent": fork["is_fork_parent"],
        "fork_parents": fork["fork_parents"],
        "soft_delete": parse_soft_delete(raw.get("soft_delete")),
        "allow_object_acl": bool(
            (raw.get("acl_settings") or {}).get("allow_object_acl", False)
        ),
        "cache_control": raw.get("cache_control") or None,
        "custom_domain": (raw.get("website") or {}).get("domain_name") or None,
        "size": {
            "objects": raw.get("estimated_unique_rows"),
            "bytes": raw.get("estimated_size"),
            "objects_all_versions": raw.get("estimated_rows"),
        }
        if with_size
        else None,
        "raw": raw,
    }


def _is_snapshot_enabled_header_set(headers: dict[str, Any]) -> bool:
    """Check if the snapshot enabled header is set in the response headers."""
    return str(headers.get("x-tigris-enable-snapshot", "")).lower() == "true"
