"""Bucket information read from Tigris-specific HeadBucket headers."""

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


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


def _is_snapshot_enabled_header_set(headers: dict[str, Any]) -> bool:
    """Check if the snapshot enabled header is set in the response headers."""
    return str(headers.get("x-tigris-enable-snapshot", "")).lower() == "true"
