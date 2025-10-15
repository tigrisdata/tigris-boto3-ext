"""Basic usage examples for tigris-boto3-ext."""

import boto3
from tigris_boto3_ext import TigrisS3Client, TigrisSnapshotEnabled

# Initialize boto3 S3 client
s3 = boto3.client(
    's3',
    endpoint_url='https://fly.storage.tigris.dev',  # Tigris endpoint
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key',
)

# Wrap with TigrisS3Client for convenience
tigris_s3 = TigrisS3Client(s3)


def example_create_snapshot_enabled_bucket():
    """Example: Create a bucket with snapshot support enabled."""
    print("\n=== Creating Snapshot-Enabled Bucket ===")

    # Using context manager
    with TigrisSnapshotEnabled(s3):
        response = s3.create_bucket(Bucket='my-snapshot-bucket')
        print(f"Created bucket: {response}")

    # Or using the wrapper
    with tigris_s3.snapshot_enabled():
        response = tigris_s3.create_bucket(Bucket='another-snapshot-bucket')
        print(f"Created bucket: {response}")


def example_create_snapshot():
    """Example: Create a snapshot of a bucket."""
    print("\n=== Creating Snapshot ===")

    # Create a snapshot with a name
    response = tigris_s3.create_snapshot(
        'my-snapshot-bucket',
        snapshot_name='daily-backup-2024-01-01'
    )
    print(f"Created snapshot: {response}")

    # Create a snapshot without a specific name
    response = tigris_s3.create_snapshot('my-snapshot-bucket')
    print(f"Created snapshot: {response}")


def example_list_snapshots():
    """Example: List all snapshots for a bucket."""
    print("\n=== Listing Snapshots ===")

    snapshots = tigris_s3.list_snapshots('my-snapshot-bucket')

    print(f"Snapshots for 'my-snapshot-bucket':")
    for bucket in snapshots.get('Buckets', []):
        print(f"  - {bucket['Name']} (Created: {bucket['CreationDate']})")


def example_create_fork():
    """Example: Create a forked bucket."""
    print("\n=== Creating Forked Bucket ===")

    # Fork from current state of source bucket
    response = tigris_s3.create_fork(
        'forked-bucket',
        'my-snapshot-bucket'
    )
    print(f"Created fork: {response}")

    # Fork from specific snapshot version
    response = tigris_s3.create_fork(
        'forked-from-snapshot',
        'my-snapshot-bucket',
        snapshot_version='1234567890'
    )
    print(f"Created fork from snapshot: {response}")


def example_read_from_snapshot():
    """Example: Read objects from a specific snapshot."""
    print("\n=== Reading from Snapshot ===")

    snapshot_version = '1234567890'  # Use actual snapshot version

    # Get a specific object from snapshot
    try:
        obj = tigris_s3.get_object_from_snapshot(
            'my-snapshot-bucket',
            'file.txt',
            snapshot_version
        )
        content = obj['Body'].read()
        print(f"Object content: {content}")
    except Exception as e:
        print(f"Error reading object: {e}")

    # List objects in snapshot
    try:
        result = tigris_s3.list_objects_from_snapshot(
            'my-snapshot-bucket',
            snapshot_version,
            Prefix='data/'
        )
        print("Objects in snapshot:")
        for obj in result.get('Contents', []):
            print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    except Exception as e:
        print(f"Error listing objects: {e}")


def example_complete_workflow():
    """Example: Complete backup and restore workflow."""
    print("\n=== Complete Backup & Restore Workflow ===")

    bucket_name = 'production-data'

    # 1. Create a snapshot-enabled bucket
    print("1. Creating snapshot-enabled bucket...")
    with tigris_s3.snapshot_enabled():
        tigris_s3.create_bucket(Bucket=bucket_name)

    # 2. Add some data
    print("2. Adding data to bucket...")
    tigris_s3.put_object(
        Bucket=bucket_name,
        Key='important.txt',
        Body=b'This is critical production data'
    )
    tigris_s3.put_object(
        Bucket=bucket_name,
        Key='config.json',
        Body=b'{"version": "1.0", "environment": "production"}'
    )

    # 3. Create a snapshot
    print("3. Creating snapshot...")
    snapshot = tigris_s3.create_snapshot(bucket_name, snapshot_name='backup-v1')
    print(f"Snapshot created: {snapshot}")

    # 4. Modify data (simulate production changes)
    print("4. Modifying production data...")
    tigris_s3.put_object(
        Bucket=bucket_name,
        Key='important.txt',
        Body=b'This is updated production data'
    )

    # 5. List all snapshots
    print("5. Listing all snapshots...")
    snapshots = tigris_s3.list_snapshots(bucket_name)
    for s in snapshots.get('Buckets', []):
        print(f"  - {s['Name']}")

    # 6. Read from snapshot (time travel!)
    print("6. Reading original data from snapshot...")
    snapshot_version = '1234567890'  # Use actual version from snapshot metadata
    try:
        obj = tigris_s3.get_object_from_snapshot(
            bucket_name,
            'important.txt',
            snapshot_version
        )
        original_content = obj['Body'].read()
        print(f"Original content: {original_content}")
    except Exception as e:
        print(f"Note: {e}")

    # 7. Create a fork for testing
    print("7. Creating test fork from snapshot...")
    try:
        tigris_s3.create_fork(
            'test-environment',
            bucket_name,
            snapshot_version
        )
        print("Test environment created!")
    except Exception as e:
        print(f"Note: {e}")


if __name__ == '__main__':
    print("Tigris boto3 Extensions - Basic Usage Examples")
    print("=" * 50)

    # Run examples
    example_create_snapshot_enabled_bucket()
    example_create_snapshot()
    example_list_snapshots()
    example_create_fork()
    example_read_from_snapshot()
    example_complete_workflow()

    print("\n" + "=" * 50)
    print("Examples completed!")
