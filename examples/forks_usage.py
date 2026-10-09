"""Examples for bucket forks with tigris-boto3-ext.

A fork is a new bucket that starts as a copy of an existing bucket, either as
it is now or as it was at one of its snapshots. Nothing is copied: the fork
shares the source's objects and stores only what you change afterwards, and
writes to either bucket never affect the other. The source bucket must have
snapshots enabled. See https://www.tigrisdata.com/docs/buckets/snapshots-and-forks/
"""

import boto3

from tigris_boto3_ext import (
    TigrisFork,
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    forked_from,
    get_bucket_info,
    get_snapshot_version,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)

SOURCE = "production-data"


def setup_source_bucket():
    """Create the snapshot-enabled source bucket the examples fork from."""
    create_snapshot_bucket(s3, SOURCE)
    s3.put_object(Bucket=SOURCE, Key="config.json", Body=b'{"version": 1}')
    s3.put_object(Bucket=SOURCE, Key="data/users.csv", Body=b"id,name\n1,ada\n")
    print(f"Created {SOURCE} with two objects")


def example_create_fork():
    """Fork with the helper: from the current state, or from a snapshot."""
    print("\n=== create_fork helper ===")

    # From the current state of the source
    create_fork(s3, "dev-environment", SOURCE)
    print(f"Created dev-environment from the current state of {SOURCE}")

    # From a specific snapshot
    version = get_snapshot_version(create_snapshot(s3, SOURCE, snapshot_name="v1"))
    create_fork(s3, "test-environment", SOURCE, snapshot_version=version)
    print(f"Created test-environment from snapshot {version}")
    return version


def example_fork_context_manager(version):
    """Every create_bucket inside the block becomes a fork of the source."""
    print("\n=== TigrisFork context manager ===")

    with TigrisFork(s3, SOURCE):
        s3.create_bucket(Bucket="staging-environment")
        print("Created staging-environment from the current state")

    with TigrisFork(s3, SOURCE, snapshot_version=version):
        s3.create_bucket(Bucket="qa-environment")
        print(f"Created qa-environment from snapshot {version}")


def example_fork_decorator(version):
    """Wrap a bucket-creation function so it creates forks."""
    print("\n=== @forked_from decorator ===")

    @forked_from(SOURCE)
    def create_dev_environment(client, name):
        return client.create_bucket(Bucket=name)

    @forked_from(SOURCE, snapshot_version=version)
    def create_test_environment(client, name):
        return client.create_bucket(Bucket=name)

    create_dev_environment(s3, "dev-environment-2")
    create_test_environment(s3, "test-environment-2")
    print("Created dev-environment-2 and test-environment-2")


def example_fork_isolation():
    """Writes to a fork never reach the source, and vice versa."""
    print("\n=== Fork isolation ===")

    create_fork(s3, "experiment", SOURCE)
    s3.put_object(Bucket="experiment", Key="config.json", Body=b'{"version": 99}')
    s3.put_object(Bucket="experiment", Key="scratch.txt", Body=b"only in the fork")

    source_config = s3.get_object(Bucket=SOURCE, Key="config.json")["Body"].read()
    fork_config = s3.get_object(Bucket="experiment", Key="config.json")["Body"].read()
    print(f"Source config: {source_config!r}")
    print(f"Fork config:   {fork_config!r}")

    source_keys = [
        o["Key"] for o in s3.list_objects_v2(Bucket=SOURCE).get("Contents", [])
    ]
    print(f"Source keys, unchanged: {source_keys}")


def example_backup_and_restore():
    """Snapshot before a risky change, fork the snapshot to get the data back."""
    print("\n=== Backup and restore ===")

    version = get_snapshot_version(
        create_snapshot(s3, SOURCE, snapshot_name="before-migration")
    )
    print(f"Snapshot before-migration: {version}")

    # The migration goes wrong and corrupts the data
    s3.put_object(Bucket=SOURCE, Key="config.json", Body=b"corrupted")
    s3.delete_object(Bucket=SOURCE, Key="data/users.csv")

    # Restore: the fork is the bucket as it was, under a new name
    create_fork(s3, "production-data-restored", SOURCE, snapshot_version=version)
    restored = s3.get_object(Bucket="production-data-restored", Key="config.json")
    print(f"Restored config: {restored['Body'].read()!r}")
    keys = s3.list_objects_v2(Bucket="production-data-restored").get("Contents", [])
    print(f"Restored keys: {[o['Key'] for o in keys]}")


def example_inspect_fork():
    """A fork knows where it came from."""
    print("\n=== Inspecting a fork ===")

    info = get_bucket_info(s3, "test-environment")
    print(f"Forked from {info['fork_source_bucket']} at {info['fork_source_snapshot']}")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Fork Usage Examples")
    print("=" * 50)

    setup_source_bucket()
    version = example_create_fork()
    example_fork_context_manager(version)
    example_fork_decorator(version)
    example_fork_isolation()
    example_backup_and_restore()
    example_inspect_fork()
