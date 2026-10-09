"""Examples for force-deleting buckets with tigris-boto3-ext.

S3 refuses to delete a bucket that still contains objects. Tigris lets you
skip the empty-then-delete routine with one ``DeleteBucket`` request carrying
the ``X-Tigris-Force-Delete: true`` header. On a bucket with soft delete
enabled the deletion is recoverable for the retention window; on any other
bucket it is permanent.
"""

import boto3
from botocore.exceptions import ClientError

from tigris_boto3_ext import create_soft_delete_bucket, force_delete_bucket

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)


def example_force_delete_regular_bucket():
    """Delete a non-empty bucket. Without soft delete, the data is gone for good."""
    print("\n=== force_delete_bucket on a regular bucket ===")

    s3.create_bucket(Bucket="scratch-bucket")
    for key in ("a.txt", "b.txt", "nested/c.txt"):
        s3.put_object(Bucket="scratch-bucket", Key=key, Body=b"temporary")

    # A plain delete_bucket would fail with BucketNotEmpty
    try:
        s3.delete_bucket(Bucket="scratch-bucket")
    except ClientError as e:
        print(f"delete_bucket refused: {e.response['Error']['Code']}")

    force_delete_bucket(s3, "scratch-bucket")
    print("Force-deleted scratch-bucket and its three objects permanently")


def example_force_delete_soft_delete_bucket():
    """On a soft-delete bucket the same call is recoverable for the retention window."""
    print("\n=== force_delete_bucket on a soft-delete bucket ===")

    create_soft_delete_bucket(s3, "archive-bucket", retention_days=30)
    s3.put_object(Bucket="archive-bucket", Key="report.pdf", Body=b"quarterly numbers")

    force_delete_bucket(s3, "archive-bucket")
    print("Deleted archive-bucket; it and report.pdf stay recoverable for 30 days")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Force Delete Usage Examples")
    print("=" * 50)

    example_force_delete_regular_bucket()
    example_force_delete_soft_delete_bucket()
