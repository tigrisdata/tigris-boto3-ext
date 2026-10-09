"""Reading objects as they were at a bucket snapshot."""

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext.buckets.snapshots import TigrisSnapshot


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
