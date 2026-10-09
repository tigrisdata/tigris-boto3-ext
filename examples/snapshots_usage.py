"""Examples for bucket snapshots with tigris-boto3-ext.

A snapshot captures the state of a whole bucket at a point in time. Creating
one is instant and zero-copy; afterwards you can list snapshots, read any
object as it was at a snapshot, and delete snapshots you no longer need.
Snapshots are enabled when the bucket is created, with the
``X-Tigris-Enable-Snapshot: true`` header.
See https://www.tigrisdata.com/docs/buckets/snapshots-and-forks/
"""

import boto3

from tigris_boto3_ext import (
    TigrisSnapshot,
    TigrisSnapshotEnabled,
    create_snapshot,
    create_snapshot_bucket,
    delete_snapshot,
    get_object_from_snapshot,
    get_snapshot_version,
    head_object_from_snapshot,
    list_objects_from_snapshot,
    list_snapshots,
    snapshot_enabled,
    with_snapshot,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)


def snapshot_versions(response):
    """Pull the versions out of a list_snapshots response.

    Each snapshot is listed as a pseudo-bucket named ``<version>; name=<name>``.
    """
    return [entry["Name"].split(";")[0] for entry in response.get("Buckets", [])]


def example_enable_snapshots():
    """Create buckets with snapshots enabled: helper, context manager, decorator."""
    print("\n=== Enabling snapshots on new buckets ===")

    # Helper: one call
    create_snapshot_bucket(s3, "my-bucket")
    print("Created my-bucket with snapshots enabled")

    # Context manager: every create_bucket inside gets the header
    with TigrisSnapshotEnabled(s3):
        for bucket in ("reports-2026", "exports-2026"):
            s3.create_bucket(Bucket=bucket)
            print(f"Created {bucket} with snapshots enabled")

    # Decorator: wrap an existing bucket-creation function
    @snapshot_enabled
    def create_backup_bucket(client, name):
        return client.create_bucket(Bucket=name)

    create_backup_bucket(s3, "backups")
    print("Created backups with snapshots enabled")


def example_create_snapshot():
    """Take a snapshot and keep its version; every other operation needs it."""
    print("\n=== create_snapshot helper ===")

    s3.put_object(Bucket="my-bucket", Key="config.json", Body=b'{"version": 1}')

    response = create_snapshot(s3, "my-bucket", snapshot_name="daily-backup")
    version = get_snapshot_version(response)
    print(f"Snapshot daily-backup has version {version}")

    # The name is optional
    unnamed = get_snapshot_version(create_snapshot(s3, "my-bucket"))
    print(f"Unnamed snapshot has version {unnamed}")
    return version


def example_list_snapshots():
    """List snapshots: helper, context manager, decorator."""
    print("\n=== list_snapshots helper ===")
    for entry in list_snapshots(s3, "my-bucket").get("Buckets", []):
        print(f"{entry['Name']} taken at {entry['CreationDate']}")

    print("\n=== TigrisSnapshot context manager ===")
    # Inside the block list_buckets lists the bucket's snapshots instead
    with TigrisSnapshot(s3, "my-bucket"):
        response = s3.list_buckets()
    print(f"Versions: {snapshot_versions(response)}")

    print("\n=== @with_snapshot decorator ===")

    @with_snapshot("my-bucket")
    def snapshots_of_my_bucket(client):
        return client.list_buckets()

    print(f"Versions: {snapshot_versions(snapshots_of_my_bucket(s3))}")


def example_read_from_snapshot(version):
    """Read objects as they were at a snapshot: helpers, context manager, decorator."""
    print("\n=== Reading from a snapshot ===")

    # Change the live object so the snapshot differs from it
    s3.put_object(Bucket="my-bucket", Key="config.json", Body=b'{"version": 2}')

    # Helpers, scoped to a single call each
    old = get_object_from_snapshot(s3, "my-bucket", "config.json", version)
    print(f"At the snapshot: {old['Body'].read()!r}")
    listing = list_objects_from_snapshot(s3, "my-bucket", version)
    print(f"Keys at the snapshot: {[o['Key'] for o in listing.get('Contents', [])]}")
    metadata = head_object_from_snapshot(s3, "my-bucket", "config.json", version)
    print(f"Size at the snapshot: {metadata['ContentLength']} bytes")

    # Context manager: plain boto3 reads, all going to the snapshot
    with TigrisSnapshot(s3, "my-bucket", snapshot_version=version):
        old = s3.get_object(Bucket="my-bucket", Key="config.json")
        print(f"Context manager read: {old['Body'].read()!r}")

    # Decorator
    @with_snapshot("my-bucket", snapshot_version=version)
    def read_historical(client, key):
        return client.get_object(Bucket="my-bucket", Key=key)["Body"].read()

    print(f"Decorator read: {read_historical(s3, 'config.json')!r}")

    # Outside any snapshot context reads are live again
    live = s3.get_object(Bucket="my-bucket", Key="config.json")
    print(f"Live: {live['Body'].read()!r}")


def example_compare_with_snapshot(version):
    """Detect what changed since a snapshot."""
    print("\n=== Comparing live data with a snapshot ===")

    live = s3.get_object(Bucket="my-bucket", Key="config.json")["Body"].read()
    with TigrisSnapshot(s3, "my-bucket", snapshot_version=version):
        then = s3.get_object(Bucket="my-bucket", Key="config.json")["Body"].read()

    if live == then:
        print("config.json is unchanged since the snapshot")
    else:
        print(f"config.json changed: {then!r} -> {live!r}")


class BucketHistory:
    """Decorators built inside methods, when the arguments are only known at run time."""

    def __init__(self, client, bucket_name):
        self.client = client
        self.bucket_name = bucket_name

    def read_at(self, key, version):
        @with_snapshot(self.bucket_name, snapshot_version=version)
        def _read(client):
            return client.get_object(Bucket=self.bucket_name, Key=key)["Body"].read()

        return _read(self.client)


def example_decorators_in_a_class(version):
    """Use a decorator whose arguments come from instance state."""
    print("\n=== Decorators inside a class ===")

    history = BucketHistory(s3, "my-bucket")
    print(f"config.json at {version}: {history.read_at('config.json', version)!r}")


def example_delete_snapshot(version):
    """Remove one snapshot; the others and the bucket are untouched."""
    print("\n=== delete_snapshot helper ===")

    before = snapshot_versions(list_snapshots(s3, "my-bucket"))
    delete_snapshot(s3, "my-bucket", version)
    after = snapshot_versions(list_snapshots(s3, "my-bucket"))
    print(f"Deleted {version}: {len(before)} snapshots -> {len(after)}")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Snapshot Usage Examples")
    print("=" * 50)

    example_enable_snapshots()
    version = example_create_snapshot()
    example_list_snapshots()
    example_read_from_snapshot(version)
    example_compare_with_snapshot(version)
    example_decorators_in_a_class(version)
    example_delete_snapshot(version)
