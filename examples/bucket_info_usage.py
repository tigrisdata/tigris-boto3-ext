"""Example usage of bucket info helper functions."""

import boto3

from tigris_boto3_ext import (
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    get_bucket_info,
    get_snapshot_version,
    has_snapshot_enabled,
)


def main():
    # Create S3 client
    s3_client = boto3.client("s3")

    # Example 1: Check if a bucket has snapshots enabled
    print("Example 1: Checking if snapshots are enabled")
    bucket_name = "my-bucket"

    if has_snapshot_enabled(s3_client, bucket_name):
        print(f"✓ Snapshots are enabled for {bucket_name}")
    else:
        print(f"✗ Snapshots are not enabled for {bucket_name}")

    # Example 2: Get comprehensive bucket information
    print("\nExample 2: Getting bucket information")
    info = get_bucket_info(s3_client, bucket_name)

    print(f"Bucket: {bucket_name}")
    print(f"  Snapshot Enabled: {info['snapshot_enabled']}")
    print(f"  Fork Source Bucket: {info['fork_source_bucket']}")
    print(f"  Fork Source Snapshot: {info['fork_source_snapshot']}")

    # Example 3: Check snapshot status when creating buckets
    print("\nExample 3: Creating and checking buckets")

    # Create a snapshot-enabled bucket
    snapshot_bucket = "snapshot-enabled-bucket"
    create_snapshot_bucket(s3_client, snapshot_bucket)
    print(f"Created snapshot-enabled bucket: {snapshot_bucket}")

    # Verify it has snapshots enabled
    if has_snapshot_enabled(s3_client, snapshot_bucket):
        print(f"✓ Confirmed snapshots are enabled")

    # Example 4: Working with forks
    print("\nExample 4: Checking fork information")

    # Create a snapshot
    snapshot_response = create_snapshot(
        s3_client, snapshot_bucket, snapshot_name="backup-v1"
    )
    snapshot_version = get_snapshot_version(snapshot_response)
    print(f"Created snapshot: {snapshot_version}")

    # Create a forked bucket
    forked_bucket = "forked-bucket"
    create_fork(
        s3_client,
        forked_bucket,
        snapshot_bucket,
        snapshot_version=snapshot_version,
    )
    print(f"Created forked bucket: {forked_bucket}")

    # Check the fork information
    fork_info = get_bucket_info(s3_client, forked_bucket)
    print(f"\nForked bucket information:")
    print(f"  Source: {fork_info['fork_source_bucket']}")
    print(f"  Snapshot Version: {fork_info['fork_source_snapshot']}")

    # Example 5: Quick snapshot check before operations
    print("\nExample 5: Conditional operations based on snapshot status")

    def safe_snapshot(s3_client, bucket_name):
        """Only create snapshot if bucket supports it."""
        if has_snapshot_enabled(s3_client, bucket_name):
            result = create_snapshot(s3_client, bucket_name)
            print(f"✓ Created snapshot for {bucket_name}")
            return result
        else:
            print(f"✗ Cannot create snapshot - not enabled for {bucket_name}")
            return None

    # Try to create snapshot
    safe_snapshot(s3_client, snapshot_bucket)


if __name__ == "__main__":
    main()
