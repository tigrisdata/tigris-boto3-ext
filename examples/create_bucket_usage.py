"""Examples for bucket creation options with tigris-boto3-ext.

S3's CreateBucket cannot say where the data lives, which storage class new
objects get, or whether the public may list the bucket. Tigris reads those
from headers on the request (``X-Tigris-Regions``, ``X-Amz-Storage-Class``,
``X-Amz-Acl-Public-List-Objects-Enabled``), and public read access is the
standard ``public-read`` ACL. Options you leave out keep the gateway defaults:
private, STANDARD, global, no anonymous listing.
See https://www.tigrisdata.com/docs/concepts/regions/
"""

import boto3

from tigris_boto3_ext import (
    TigrisBucketOptions,
    TigrisSnapshotEnabled,
    create_bucket,
    with_bucket_options,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)


def example_helper():
    """Easiest path: one call with the options you need."""
    print("\n=== create_bucket helper ===")

    create_bucket(s3, "assets", access="public", regions="iad")
    print("Created assets: public reads, data in iad")

    create_bucket(
        s3, "archive", default_storage_class="STANDARD_IA", regions=["iad", "sjc"]
    )
    print("Created archive: STANDARD_IA by default, dual-region iad+sjc")

    create_bucket(s3, "downloads", access="public", public_list_objects=True)
    print("Created downloads: public reads and anonymous listing")


def example_context_manager():
    """Use the context manager to create several buckets with the same options."""
    print("\n=== TigrisBucketOptions context manager ===")

    with TigrisBucketOptions(s3, regions="eur", default_storage_class="GLACIER_IR"):
        for bucket in ("cold-eu-1", "cold-eu-2"):
            s3.create_bucket(Bucket=bucket)
            print(f"Created {bucket}: GLACIER_IR by default, multi-region eur")


def example_decorator():
    """Wrap an existing bucket-creation function without changing its body."""
    print("\n=== @with_bucket_options decorator ===")

    @with_bucket_options(regions="usa")
    def create_us_bucket(client, name):
        return client.create_bucket(Bucket=name)

    create_us_bucket(s3, "us-data")
    print("Created us-data: multi-region usa")


def example_validation():
    """Bad options are rejected before any request is sent."""
    print("\n=== Validation ===")

    for kwargs in (
        {"default_storage_class": "COLD"},
        {"regions": []},
        {"public_list_objects": True},  # needs access="public"
    ):
        try:
            create_bucket(s3, "my-bucket", **kwargs)
        except ValueError as e:
            print(f"Rejected {kwargs}: {e}")


def example_combined_with_snapshots():
    """Creation-time settings nest: snapshots plus placement plus access in one call."""
    print("\n=== Combined with TigrisSnapshotEnabled ===")

    with TigrisSnapshotEnabled(s3):
        create_bucket(s3, "production-data", access="public", regions="iad")
    print("Created production-data: snapshots enabled, public reads, data in iad")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Create Bucket Usage Examples")
    print("=" * 50)

    example_helper()
    example_context_manager()
    example_decorator()
    example_validation()
    example_combined_with_snapshots()
