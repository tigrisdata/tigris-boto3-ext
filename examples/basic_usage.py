"""Basic usage examples for tigris-boto3-ext."""

import boto3
from tigris_boto3_ext import (
    TigrisSnapshotEnabled,
    create_snapshot_bucket,
    create_fork,
    create_snapshot,
    get_object_from_snapshot,
    get_snapshot_version,
    list_objects_from_snapshot,
    list_snapshots,
)

# Initialize boto3 S3 client
s3 = boto3.client(
    's3',
    endpoint_url='https://fly.storage.tigris.dev',  # Tigris endpoint
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key',
)


def example_create_snapshot_enabled_bucket():
    """Example: Create a bucket with snapshot support enabled."""
    print("\n=== Creating Snapshot-Enabled Bucket ===")

    # Using context manager
    with TigrisSnapshotEnabled(s3):
        response = s3.create_bucket(Bucket='my-snapshot-bucket')
        print(f"Created bucket: {response}")

    # Or using helper function
    response = create_snapshot_bucket(s3, 'another-snapshot-bucket')
    print(f"Created bucket: {response}")


def example_create_snapshot():
    """Example: Create a snapshot of a bucket."""
    print("\n=== Creating Snapshot ===")

    # First, ensure bucket has snapshots enabled
    create_snapshot_bucket(s3, 'my-snapshot-bucket')

    # Create a snapshot with a name
    response = create_snapshot(
        s3,
        'my-snapshot-bucket',
        snapshot_name='daily-backup-2024-01-01'
    )
    print(f"Created snapshot: {response}")

    # Extract snapshot version
    snapshot_version = get_snapshot_version(response)
    print(f"Snapshot version: {snapshot_version}")

    # Create a snapshot without a specific name
    response = create_snapshot(s3, 'my-snapshot-bucket')
    print(f"Created snapshot: {response}")


def example_list_snapshots():
    """Example: List all snapshots for a bucket."""
    print("\n=== Listing Snapshots ===")

    snapshots = list_snapshots(s3, 'my-snapshot-bucket')

    print(f"Snapshots for 'my-snapshot-bucket':")
    for bucket in snapshots.get('Buckets', []):
        print(f"  - {bucket['Name']} (Created: {bucket['CreationDate']})")


def example_create_fork():
    """Example: Create a forked bucket."""
    print("\n=== Creating Forked Bucket ===")

    # Fork from current state of source bucket
    response = create_fork(
        s3,
        'forked-bucket',
        'my-snapshot-bucket'
    )
    print(f"Created fork: {response}")

    # Fork from specific snapshot version
    response = create_fork(
        s3,
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
        obj = get_object_from_snapshot(
            s3,
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
        result = list_objects_from_snapshot(
            s3,
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
    create_snapshot_bucket(s3, bucket_name)

    # 2. Add some data
    print("2. Adding data to bucket...")
    s3.put_object(
        Bucket=bucket_name,
        Key='important.txt',
        Body=b'This is critical production data'
    )
    s3.put_object(
        Bucket=bucket_name,
        Key='config.json',
        Body=b'{"version": "1.0", "environment": "production"}'
    )

    # 3. Create a snapshot
    print("3. Creating snapshot...")
    snapshot_response = create_snapshot(s3, bucket_name, snapshot_name='backup-v1')
    snapshot_version = get_snapshot_version(snapshot_response)
    print(f"Snapshot created with version: {snapshot_version}")

    # 4. Modify data (simulate production changes)
    print("4. Modifying production data...")
    s3.put_object(
        Bucket=bucket_name,
        Key='important.txt',
        Body=b'This is updated production data'
    )

    # 5. List all snapshots
    print("5. Listing all snapshots...")
    snapshots = list_snapshots(s3, bucket_name)
    for s in snapshots.get('Buckets', []):
        print(f"  - {s['Name']}")

    # 6. Read from snapshot (time travel!)
    print("6. Reading original data from snapshot...")
    try:
        obj = get_object_from_snapshot(
            s3,
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
        create_fork(
            s3,
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
