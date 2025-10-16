"""Examples using context managers for tigris-boto3-ext."""

import boto3
from tigris_boto3_ext import TigrisSnapshotEnabled, TigrisSnapshot, TigrisFork

# Initialize boto3 S3 client
s3 = boto3.client(
    's3',
    endpoint_url='https://t3.storage.dev',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key',
)


def example_snapshot_enabled():
    """Example: Using TigrisSnapshotEnabled context manager."""
    print("\n=== TigrisSnapshotEnabled Context Manager ===")

    # Create multiple buckets with snapshot support
    bucket_names = ['bucket-1', 'bucket-2', 'bucket-3']

    with TigrisSnapshotEnabled(s3):
        for bucket_name in bucket_names:
            try:
                response = s3.create_bucket(Bucket=bucket_name)
                print(f"Created snapshot-enabled bucket: {bucket_name}")
            except Exception as e:
                print(f"Note: {e}")


def example_snapshot_listing():
    """Example: Listing snapshots using TigrisSnapshot context manager."""
    print("\n=== Listing Snapshots ===")

    bucket_name = 'production-data'

    with TigrisSnapshot(s3, bucket_name):
        try:
            snapshots = s3.list_buckets()
            print(f"Snapshots for {bucket_name}:")
            for bucket in snapshots.get('Buckets', []):
                print(f"  - {bucket['Name']} (Created: {bucket['CreationDate']})")
        except Exception as e:
            print(f"Note: {e}")


def example_snapshot_reading():
    """Example: Reading from snapshot using TigrisSnapshot context manager."""
    print("\n=== Reading from Snapshot ===")

    bucket_name = 'production-data'
    snapshot_version = '1234567890'

    with TigrisSnapshot(s3, bucket_name, snapshot_version):
        # Get a specific object
        try:
            obj = s3.get_object(Bucket=bucket_name, Key='config.json')
            content = obj['Body'].read()
            print(f"Object content: {content}")
        except Exception as e:
            print(f"Note: {e}")

        # List objects
        try:
            result = s3.list_objects_v2(Bucket=bucket_name, Prefix='data/')
            print(f"\nObjects in snapshot:")
            for obj in result.get('Contents', []):
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        except Exception as e:
            print(f"Note: {e}")

        # Head object (get metadata)
        try:
            metadata = s3.head_object(Bucket=bucket_name, Key='config.json')
            print(f"\nObject metadata:")
            print(f"  Content-Length: {metadata['ContentLength']}")
            print(f"  Last-Modified: {metadata['LastModified']}")
            print(f"  ETag: {metadata['ETag']}")
        except Exception as e:
            print(f"Note: {e}")


def example_forking():
    """Example: Creating forks using TigrisFork context manager."""
    print("\n=== Creating Forks ===")

    source_bucket = 'production-data'

    # Fork from current state
    print("1. Forking from current state...")
    with TigrisFork(s3, source_bucket):
        try:
            response = s3.create_bucket(Bucket='dev-environment')
            print(f"Created fork: dev-environment")
        except Exception as e:
            print(f"Note: {e}")

    # Fork from specific snapshot
    print("\n2. Forking from specific snapshot...")
    snapshot_version = '1234567890'
    with TigrisFork(s3, source_bucket, snapshot_version):
        try:
            response = s3.create_bucket(Bucket='test-environment')
            print(f"Created fork from snapshot: test-environment")
        except Exception as e:
            print(f"Note: {e}")


def example_comparison_workflow():
    """Example: Compare current state with snapshot."""
    print("\n=== Comparing Current State with Snapshot ===")

    bucket_name = 'data-bucket'
    snapshot_version = '1234567890'
    key = 'config.json'

    # Read current version
    try:
        current_obj = s3.get_object(Bucket=bucket_name, Key=key)
        current_content = current_obj['Body'].read()
        print(f"Current content: {current_content}")
    except Exception as e:
        print(f"Current version error: {e}")

    # Read snapshot version
    with TigrisSnapshot(s3, bucket_name, snapshot_version):
        try:
            snapshot_obj = s3.get_object(Bucket=bucket_name, Key=key)
            snapshot_content = snapshot_obj['Body'].read()
            print(f"Snapshot content: {snapshot_content}")
        except Exception as e:
            print(f"Snapshot version error: {e}")

    # Compare
    try:
        if current_content == snapshot_content:
            print("No changes detected")
        else:
            print("Content has changed since snapshot")
    except Exception:
        print("Unable to compare")


if __name__ == '__main__':
    print("Tigris boto3 Extensions - Context Manager Usage Examples")
    print("=" * 60)

    example_snapshot_enabled()
    example_snapshot_listing()
    example_snapshot_reading()
    example_forking()
    example_comparison_workflow()

    print("\n" + "=" * 60)
    print("Examples completed!")
