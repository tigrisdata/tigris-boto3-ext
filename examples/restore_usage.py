"""Examples for archive restore with tigris-boto3-ext.

Objects in the GLACIER storage class need a restore before they can be read.
restore_object requests one; get_restore_info reports archived, in-progress or
restored. Both are plain S3 operations. A restore typically takes about an
hour, so this example shows the request and the in-progress state.
See https://www.tigrisdata.com/docs/objects/tiers/
"""

import boto3
from botocore.exceptions import ClientError

from tigris_boto3_ext import (
    create_bucket,
    force_delete_bucket,
    get_restore_info,
    restore_object,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)

BUCKET = "restore-demo"


def example_restore_archived_object():
    """Check an archived object, request its restore, check again."""
    print("\n=== Restore an archived object ===")

    s3.put_object(
        Bucket=BUCKET, Key="cold.txt", Body=b"archived", StorageClass="GLACIER"
    )
    print("before:", get_restore_info(s3, BUCKET, "cold.txt"))

    restore_object(s3, BUCKET, "cold.txt", days=3)
    print("after requesting a 3-day restore:", get_restore_info(s3, BUCKET, "cold.txt"))

    try:
        restore_object(s3, BUCKET, "cold.txt", days=3)
    except ClientError as e:
        print("second request:", e.response["Error"]["Code"])


def example_objects_without_restore_state():
    """Only GLACIER objects have restore state."""
    print("\n=== Objects that need no restore ===")

    s3.put_object(Bucket=BUCKET, Key="std.txt", Body=b"standard")
    s3.put_object(
        Bucket=BUCKET, Key="ir.txt", Body=b"instant", StorageClass="GLACIER_IR"
    )
    print("STANDARD:", get_restore_info(s3, BUCKET, "std.txt"))
    print("GLACIER_IR:", get_restore_info(s3, BUCKET, "ir.txt"))

    try:
        restore_object(s3, BUCKET, "ir.txt")
    except ClientError as e:
        print("restore of a GLACIER_IR object:", e.response["Error"]["Code"])


def example_validation():
    """Bad arguments are rejected before any request is sent."""
    print("\n=== Validation ===")

    for days in (0, 1.5):
        try:
            restore_object(s3, BUCKET, "cold.txt", days=days)
        except (ValueError, TypeError) as e:
            print(f"Rejected days={days!r}: {e}")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Archive Restore Usage Examples")
    print("=" * 50)

    create_bucket(s3, BUCKET)
    try:
        example_restore_archived_object()
        example_objects_without_restore_state()
        example_validation()
    finally:
        force_delete_bucket(s3, BUCKET)
