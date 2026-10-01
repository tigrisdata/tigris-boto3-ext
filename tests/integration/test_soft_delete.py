"""Integration tests for soft delete."""

from tigris_boto3_ext import create_soft_delete_bucket
from tigris_boto3_ext._internal import create_header_injector

from .conftest import bucket_exists, generate_bucket_name


def _soft_deleted_keys(s3_client, bucket_name):
    """Keys currently in the bucket's soft-delete view."""
    injector = create_header_injector(
        s3_client, "ListObjectVersions", {"X-Tigris-Soft-Delete": "true"}
    )
    try:
        injector.register()
        response = s3_client.list_object_versions(Bucket=bucket_name)
    finally:
        injector.unregister()
    return [version["Key"] for version in response.get("Versions", [])]


class TestSoftDeleteBucketCreation:
    """Test creating buckets with soft delete enabled."""

    def test_deleted_object_is_recoverable(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """An object deleted from a soft-delete bucket lands in the soft-delete view."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "soft-delete-")
        cleanup_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        assert bucket_exists(s3_client, bucket_name)

        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")
        s3_client.delete_object(Bucket=bucket_name, Key="file.txt")

        assert "file.txt" in _soft_deleted_keys(s3_client, bucket_name)

    def test_custom_retention_window(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """A custom retention window within 7 to 90 days is accepted."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "soft-delete-30-")
        cleanup_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name, retention_days=30)

        assert bucket_exists(s3_client, bucket_name)
