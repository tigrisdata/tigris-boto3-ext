"""Examples for soft delete with tigris-boto3-ext.

Soft delete keeps deleted objects, and the bucket itself when it is deleted,
recoverable for a retention window instead of removing them at once. It is
enabled when a bucket is created, with the ``X-Tigris-Soft-Delete`` header:
``true`` selects the default 7-day window, a number from 7 to 90 sets a custom
one. See https://www.tigrisdata.com/docs/buckets/soft-delete/
"""

import boto3
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import CreateBucketOutputTypeDef

from tigris_boto3_ext import (
    TigrisSoftDeleteEnabled,
    TigrisSoftDeleteView,
    create_soft_delete_bucket,
    purge_deleted_object,
    soft_delete_enabled,
    with_soft_delete_view,
    restore_deleted_object,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)


def example_create_bucket_helper():
    """Easiest path: one call creates the bucket with soft delete enabled."""
    print("\n=== create_soft_delete_bucket helper ===")

    # Default 7-day retention window
    create_soft_delete_bucket(s3, "my-bucket")
    print("Created my-bucket with the default 7-day retention window")

    # Custom window, anywhere from 7 to 90 days
    create_soft_delete_bucket(s3, "my-archive", retention_days=30)
    print("Created my-archive with a 30-day retention window")


def example_create_bucket_context_manager():
    """Use the context manager to create several buckets with the same window."""
    print("\n=== TigrisSoftDeleteEnabled context manager ===")

    with TigrisSoftDeleteEnabled(s3, retention_days=14):
        for bucket in ("reports-2026", "exports-2026"):
            s3.create_bucket(Bucket=bucket)
            print(f"Created {bucket} with a 14-day retention window")


def example_create_bucket_decorator():
    """Wrap an existing bucket-creation function without changing its body."""
    print("\n=== @soft_delete_enabled decorator ===")

    @soft_delete_enabled(retention_days=90)
    def create_compliance_bucket(
        client: S3Client, name: str
    ) -> CreateBucketOutputTypeDef:
        return client.create_bucket(Bucket=name)

    create_compliance_bucket(s3, "audit-logs")
    print("Created audit-logs with a 90-day retention window")


def example_retention_validation():
    """Retention windows outside 7 to 90 days are rejected before any request."""
    print("\n=== Validation ===")

    try:
        create_soft_delete_bucket(s3, "my-bucket", retention_days=365)
    except ValueError as e:
        print(f"Rejected: {e}")


def example_soft_delete_view():
    """List soft-deleted objects with the context manager or the decorator."""
    print("\n=== TigrisSoftDeleteView context manager ===")

    with TigrisSoftDeleteView(s3):
        deleted = s3.list_object_versions(Bucket="my-bucket")
    for version in deleted.get("Versions", []):
        print(f"Recoverable: {version['Key']} (version {version['VersionId']})")

    print("\n=== @with_soft_delete_view decorator ===")

    @with_soft_delete_view
    def deleted_keys(client: S3Client, bucket: str):
        response = client.list_object_versions(Bucket=bucket)
        return [v["Key"] for v in response.get("Versions", [])]

    print(f"Recoverable keys: {deleted_keys(s3, 'my-bucket')}")


def example_purge_deleted_version():
    """Permanently remove a soft-deleted version before its window expires."""
    print("\n=== purge_deleted_object helper ===")

    with TigrisSoftDeleteView(s3):
        deleted = s3.list_object_versions(Bucket="my-bucket")

    for version in deleted.get("Versions", []):
        if version["Key"] == "secrets.env":
            purge_deleted_object(s3, "my-bucket", "secrets.env", version["VersionId"])
            print(f"Purged secrets.env version {version['VersionId']}")


def example_restore_deleted_object():
    """Bring a soft-deleted object back within its retention window."""
    print("\n=== restore_deleted_object helper ===")

    # Most recent soft-deleted version
    restore_deleted_object(s3, "my-bucket", "report.pdf")
    print("Restored the most recent version of report.pdf")

    # A specific version, picked from the soft-delete view
    with TigrisSoftDeleteView(s3):
        deleted = s3.list_object_versions(Bucket="my-bucket", Prefix="contracts/")
    for version in deleted.get("Versions", []):
        restore_deleted_object(s3, "my-bucket", version["Key"], version["VersionId"])
        print(f"Restored {version['Key']} (version {version['VersionId']})")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Soft Delete Usage Examples")
    print("=" * 50)

    example_create_bucket_helper()
    example_create_bucket_context_manager()
    example_create_bucket_decorator()
    example_retention_validation()

    example_soft_delete_view()
    example_purge_deleted_version()
    example_restore_deleted_object()
