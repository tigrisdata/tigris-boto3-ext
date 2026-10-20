"""Example for account storage stats with tigris-boto3-ext.

Tigris reports storage counters with its bucket listing; get_stats sums them
for the whole account. It is a Tigris endpoint rather than an S3 operation.
"""

import boto3

from tigris_boto3_ext import get_stats

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)


def example_stats():
    """How much does the account hold?"""
    print("\n=== get_stats helper ===")

    stats = get_stats(s3)
    print(
        f"{stats['active_buckets']} buckets, {stats['total_objects']} objects "
        f"({stats['total_unique_objects']} unique), {stats['total_storage_bytes']} bytes"
    )


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Stats Usage Example")
    print("=" * 50)

    example_stats()
