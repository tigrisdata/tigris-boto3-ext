"""Examples using decorators for tigris-boto3-ext."""

import boto3
from tigris_boto3_ext import snapshot_enabled, with_snapshot, forked_from

# Initialize boto3 S3 client
s3 = boto3.client(
    's3',
    endpoint_url='https://fly.storage.tigris.dev',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key',
)


@snapshot_enabled
def create_backup_bucket(s3_client, bucket_name):
    """Create a bucket with snapshot support enabled."""
    return s3_client.create_bucket(Bucket=bucket_name)


@with_snapshot('production-data')
def list_all_snapshots(s3_client):
    """List all snapshots for production-data bucket."""
    return s3_client.list_buckets()


@with_snapshot('production-data', snapshot_version='1234567890')
def get_historical_file(s3_client, key):
    """Get a file from a specific snapshot."""
    return s3_client.get_object(Bucket='production-data', Key=key)


@with_snapshot('production-data', snapshot_version='1234567890')
def list_historical_objects(s3_client, prefix=''):
    """List objects from a specific snapshot."""
    kwargs = {'Bucket': 'production-data'}
    if prefix:
        kwargs['Prefix'] = prefix
    return s3_client.list_objects_v2(**kwargs)


@forked_from('production-data')
def create_dev_environment(s3_client, env_name):
    """Create a development environment by forking production."""
    return s3_client.create_bucket(Bucket=env_name)


@forked_from('production-data', snapshot_version='1234567890')
def create_test_from_snapshot(s3_client, test_bucket_name):
    """Create a test environment from a specific snapshot."""
    return s3_client.create_bucket(Bucket=test_bucket_name)


def example_decorated_functions():
    """Demonstrate usage of decorated functions."""
    print("\n=== Using Decorated Functions ===")

    # Create snapshot-enabled bucket
    print("1. Creating snapshot-enabled bucket...")
    try:
        result = create_backup_bucket(s3, 'backup-bucket')
        print(f"Created: {result}")
    except Exception as e:
        print(f"Note: {e}")

    # List snapshots
    print("\n2. Listing snapshots...")
    try:
        snapshots = list_all_snapshots(s3)
        for bucket in snapshots.get('Buckets', []):
            print(f"  - {bucket['Name']}")
    except Exception as e:
        print(f"Note: {e}")

    # Get historical file
    print("\n3. Reading historical file...")
    try:
        obj = get_historical_file(s3, 'config.json')
        content = obj['Body'].read()
        print(f"Content: {content}")
    except Exception as e:
        print(f"Note: {e}")

    # List historical objects
    print("\n4. Listing historical objects...")
    try:
        result = list_historical_objects(s3, prefix='data/')
        for obj in result.get('Contents', []):
            print(f"  - {obj['Key']}")
    except Exception as e:
        print(f"Note: {e}")

    # Create dev environment
    print("\n5. Creating dev environment fork...")
    try:
        result = create_dev_environment(s3, 'dev-environment')
        print(f"Created: {result}")
    except Exception as e:
        print(f"Note: {e}")

    # Create test environment from snapshot
    print("\n6. Creating test environment from snapshot...")
    try:
        result = create_test_from_snapshot(s3, 'test-from-snapshot')
        print(f"Created: {result}")
    except Exception as e:
        print(f"Note: {e}")


class DataManager:
    """Example class using decorated methods."""

    def __init__(self, s3_client, bucket_name):
        self.s3_client = s3_client
        self.bucket_name = bucket_name

    @snapshot_enabled
    def create_backup_bucket(self, bucket_name):
        """Create a backup bucket with snapshots enabled."""
        return self.s3_client.create_bucket(Bucket=bucket_name)

    def read_from_snapshot(self, key, snapshot_version):
        """Read a file from a specific snapshot."""
        @with_snapshot(self.bucket_name, snapshot_version)
        def _read(s3_client):
            return s3_client.get_object(Bucket=self.bucket_name, Key=key)

        return _read(self.s3_client)

    def fork_to(self, new_bucket_name, snapshot_version=None):
        """Create a fork of this bucket."""
        @forked_from(self.bucket_name, snapshot_version)
        def _fork(s3_client):
            return s3_client.create_bucket(Bucket=new_bucket_name)

        return _fork(self.s3_client)


def example_class_usage():
    """Demonstrate usage within a class."""
    print("\n=== Using Decorators in Classes ===")

    manager = DataManager(s3, 'production-data')

    # Create backup bucket
    print("1. Creating backup bucket...")
    try:
        result = manager.create_backup_bucket('class-backup-bucket')
        print(f"Created: {result}")
    except Exception as e:
        print(f"Note: {e}")

    # Read from snapshot
    print("\n2. Reading from snapshot...")
    try:
        obj = manager.read_from_snapshot('config.json', '1234567890')
        content = obj['Body'].read()
        print(f"Content: {content}")
    except Exception as e:
        print(f"Note: {e}")

    # Create fork
    print("\n3. Creating fork...")
    try:
        result = manager.fork_to('class-fork', snapshot_version='1234567890')
        print(f"Created: {result}")
    except Exception as e:
        print(f"Note: {e}")


if __name__ == '__main__':
    print("Tigris boto3 Extensions - Decorator Usage Examples")
    print("=" * 50)

    example_decorated_functions()
    example_class_usage()

    print("\n" + "=" * 50)
    print("Examples completed!")
