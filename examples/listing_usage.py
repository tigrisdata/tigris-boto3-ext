"""Examples for bucket listing and account stats with tigris-boto3-ext.

boto3's list_buckets returns names and creation dates. Tigris's listing can
also report each bucket's type, visibility, regions, fork lineage and owner,
and fork lineage. These helpers read that listing, which is a Tigris endpoint
rather than an S3 operation.
"""

import boto3

from tigris_boto3_ext import (
    create_bucket,
    create_fork,
    create_snapshot_bucket,
    list_buckets,
    list_forks,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)


def example_list_with_details():
    """Ask for the details you need; fields you do not ask for are None."""
    print("\n=== list_buckets helper ===")

    create_snapshot_bucket(s3, "production-data")
    create_bucket(s3, "assets", access="public", regions="iad")

    page = list_buckets(
        s3, include_type=True, include_visibility=True, include_regions=True
    )
    for bucket in page["buckets"]:
        kind = "snapshot" if bucket["snapshot_enabled"] else "regular"
        where = ",".join(bucket["regions"]) or "global"
        print(f"{bucket['name']}: {kind}, {bucket['visibility']}, {where}")


def example_paging():
    """Walk the account a page at a time."""
    print("\n=== Paging ===")

    token = None
    total = 0
    while True:
        page = list_buckets(s3, max_buckets=100, continuation_token=token)
        total += len(page["buckets"])
        token = page["continuation_token"]
        if not token:
            break
    print(f"{total} buckets in the account")


def example_forks():
    """Which buckets were forked from production-data?"""
    print("\n=== list_forks helper ===")

    create_fork(s3, "dev-environment", "production-data")
    for fork in list_forks(s3, "production-data")["buckets"]:
        lineage = fork["fork_parents"][0]
        print(
            f"{fork['name']} forked at {fork['created_at']} from snapshot {lineage['snapshot']}"
        )

    # Only forks, account-wide, with their lineage
    forks = list_buckets(s3, only_forks=True, include_fork_info=True)["buckets"]
    print(f"{len(forks)} forked buckets in the account")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Listing Usage Examples")
    print("=" * 50)

    example_list_with_details()
    example_paging()
    example_forks()
