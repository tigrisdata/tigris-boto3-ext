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
    create_soft_delete_bucket,
    soft_delete_enabled,
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


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Soft Delete Usage Examples")
    print("=" * 50)

    example_create_bucket_helper()
    example_create_bucket_context_manager()
    example_create_bucket_decorator()
    example_retention_validation()
