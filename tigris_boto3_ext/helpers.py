"""High-level helper functions for Tigris-specific S3 operations."""

from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from ._internal import create_header_injector
from .context_managers import TigrisFork, TigrisSnapshot, TigrisSnapshotEnabled


def create_snapshot_bucket(
    s3_client: S3Client,
    bucket_name: str,
) -> dict[str, Any]:
    """
    Create a bucket with snapshot support enabled.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to create

    Returns:
        Response from create_bucket operation

    Usage:
        result = create_snapshot_bucket(s3_client, 'my-bucket')
    """
    with TigrisSnapshotEnabled(s3_client):
        return cast("dict[str, Any]", s3_client.create_bucket(Bucket=bucket_name))


def create_snapshot(
    s3_client: S3Client,
    bucket_name: str,
    snapshot_name: Optional[str] = None,
) -> dict[str, Any]:
    """
    Create a snapshot of a bucket.

    This is a convenience wrapper around create_bucket that sets the
    X-Tigris-Snapshot header to create a snapshot instead of a regular bucket.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to snapshot
        snapshot_name: Optional name for the snapshot

    Returns:
        Response from create_bucket operation

    Usage:
        result = create_snapshot(s3_client, 'my-bucket')
        result = create_snapshot(s3_client, 'my-bucket', snapshot_name='backup-1')
    """
    header_value = "true"
    if snapshot_name:
        header_value = f"true; name={snapshot_name}"

    injector = create_header_injector(
        s3_client,
        "CreateBucket",
        {"X-Tigris-Snapshot": header_value},
    )

    try:
        injector.register()
        return cast("dict[str, Any]", s3_client.create_bucket(Bucket=bucket_name))
    finally:
        injector.unregister()


def get_snapshot_version(response: dict[str, Any]) -> Optional[str]:
    """
    Extract snapshot version from a create_snapshot response.

    Args:
        response: Response from create_snapshot operation

    Returns:
        Snapshot version ID, or None if not found

    Usage:
        result = create_snapshot(s3_client, 'my-bucket', snapshot_name='backup')
        version = get_snapshot_version(result)
        # Use version for forking or accessing snapshot data
        create_fork(s3_client, 'my-fork', 'my-bucket', snapshot_version=version)
    """
    version = (
        response.get("ResponseMetadata", {})
        .get("HTTPHeaders", {})
        .get("x-tigris-snapshot-version")
    )
    return cast(Optional[str], version)


def list_snapshots(s3_client: S3Client, bucket_name: str) -> dict[str, Any]:
    """
    List all snapshots for a bucket.

    This is a convenience wrapper around list_buckets that filters to show
    snapshots for a specific bucket.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket to list snapshots for

    Returns:
        Response from list_buckets operation containing snapshot information

    Usage:
        snapshots = list_snapshots(s3_client, 'my-bucket')
        for bucket in snapshots.get('Buckets', []):
            print(bucket['Name'])
    """
    with TigrisSnapshot(s3_client, bucket_name):
        return cast("dict[str, Any]", s3_client.list_buckets())


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


def get_object_from_snapshot(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    snapshot_version: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Retrieve an object from a specific snapshot.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Object key to retrieve
        snapshot_version: Snapshot version ID
        **kwargs: Additional arguments to pass to get_object

    Returns:
        Response from get_object operation

    Usage:
        obj = get_object_from_snapshot(
            s3_client,
            'my-bucket',
            'file.txt',
            '12345'
        )
        content = obj['Body'].read()
    """
    with TigrisSnapshot(s3_client, bucket_name, snapshot_version):
        return cast(
            "dict[str, Any]",
            s3_client.get_object(Bucket=bucket_name, Key=key, **kwargs),
        )


def list_objects_from_snapshot(
    s3_client: S3Client,
    bucket_name: str,
    snapshot_version: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    List objects in a bucket from a specific snapshot.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        snapshot_version: Snapshot version ID
        **kwargs: Additional arguments to pass to list_objects_v2

    Returns:
        Response from list_objects_v2 operation

    Usage:
        result = list_objects_from_snapshot(
            s3_client,
            'my-bucket',
            '12345',
            Prefix='data/'
        )
        for obj in result.get('Contents', []):
            print(obj['Key'])
    """
    with TigrisSnapshot(s3_client, bucket_name, snapshot_version):
        return cast(
            "dict[str, Any]",
            s3_client.list_objects_v2(Bucket=bucket_name, **kwargs),
        )


def head_object_from_snapshot(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    snapshot_version: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Retrieve object metadata from a specific snapshot.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        key: Object key
        snapshot_version: Snapshot version ID
        **kwargs: Additional arguments to pass to head_object

    Returns:
        Response from head_object operation

    Usage:
        metadata = head_object_from_snapshot(
            s3_client,
            'my-bucket',
            'file.txt',
            '12345'
        )
        print(metadata['ContentLength'])
    """
    with TigrisSnapshot(s3_client, bucket_name, snapshot_version):
        return cast(
            "dict[str, Any]",
            s3_client.head_object(Bucket=bucket_name, Key=key, **kwargs),
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
    return is_snapshot_enabled_header_set(headers)


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
    snapshot_enabled = is_snapshot_enabled_header_set(headers)

    fork_source_bucket = headers.get("x-tigris-fork-source-bucket")
    fork_source_snapshot = headers.get("x-tigris-fork-source-bucket-snapshot")

    return {
        "snapshot_enabled": snapshot_enabled,
        "fork_source_bucket": fork_source_bucket,
        "fork_source_snapshot": fork_source_snapshot,
        "response_metadata": response,
    }


def is_snapshot_enabled_header_set(headers: dict[str, Any]) -> bool:
    """
    Check if the snapshot enabled header is set in the response headers.
    """
    return str(headers.get("x-tigris-enable-snapshot", "")).lower() == "true"
