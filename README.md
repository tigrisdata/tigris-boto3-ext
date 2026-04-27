# tigris-boto3-ext

[![CI](https://github.com/tigrisdata/tigris-boto3-ext/actions/workflows/ci.yml/badge.svg)](https://github.com/tigrisdata/tigris-boto3-ext/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/tigrisdata/tigris-boto3-ext/branch/main/graph/badge.svg)](https://codecov.io/gh/tigrisdata/tigris-boto3-ext)
[![Python Version](https://img.shields.io/pypi/pyversions/tigris-boto3-ext.svg)](https://pypi.org/project/tigris-boto3-ext/)
[![PyPI version](https://badge.fury.io/py/tigris-boto3-ext.svg)](https://badge.fury.io/py/tigris-boto3-ext)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Extend boto3 with Tigris-specific features like snapshots and bucket forking, while maintaining full boto3 compatibility.

## Features

- **Bundle API**: Fetch thousands of objects in a single request as a streaming tar archive — designed for ML training workloads
- **Snapshot Support**: Create, list, and read from bucket snapshots
- **Bucket Forking**: Create forked buckets from existing buckets or snapshots
- **Agent Kit**: High-level workflow helpers for AI agents — workspaces, parallel forks, checkpoints, and event-driven coordination
- **Multiple Usage Patterns**: Context managers, decorators, helper functions
- **Zero Configuration**: Works with existing boto3 code
- **Type Safe**: Full type hints for IDE support
- **Pythonic API**: Uses familiar Python patterns

## Installation

```bash
pip install tigris-boto3-ext
```

## Usage Patterns

### 1. Context Managers (Recommended)

#### Enable Snapshots for Bucket Creation

```python
from tigris_boto3_ext import TigrisSnapshotEnabled

with TigrisSnapshotEnabled(s3_client):
    s3_client.create_bucket(Bucket='my-snapshot-bucket')
```

#### Work with Snapshots

```python
from tigris_boto3_ext import TigrisSnapshot

# List snapshots for a bucket
with TigrisSnapshot(s3_client, 'my-bucket'):
    snapshots = s3_client.list_buckets()

# Read objects from a specific snapshot
with TigrisSnapshot(s3_client, 'my-bucket', snapshot_version='12345'):
    obj = s3_client.get_object(Bucket='my-bucket', Key='file.txt')
    objects = s3_client.list_objects_v2(Bucket='my-bucket')
```

#### Create Forked Buckets

```python
from tigris_boto3_ext import TigrisFork

# Fork from current state
with TigrisFork(s3_client, 'source-bucket'):
    s3_client.create_bucket(Bucket='forked-bucket')

# Fork from specific snapshot
with TigrisFork(s3_client, 'source-bucket', snapshot_version='12345'):
    s3_client.create_bucket(Bucket='forked-from-snapshot')
```

### 2. Decorators

```python
from tigris_boto3_ext import snapshot_enabled, with_snapshot, forked_from

@snapshot_enabled
def create_snapshot_enabled_bucket(s3_client, bucket_name):
    return s3_client.create_bucket(Bucket=bucket_name)

# List available snapshots
@with_snapshot('my-bucket')
def list_snapshots(s3_client):
    return s3_client.list_buckets()

# Read from specific snapshot
@with_snapshot('my-bucket', snapshot_version='12345')
def read_from_snapshot(s3_client, key):
    return s3_client.get_object(Bucket='my-bucket', Key=key)

@forked_from('source-bucket', snapshot_version='12345')
def create_my_fork(s3_client, new_bucket):
    return s3_client.create_bucket(Bucket=new_bucket)

# Use the decorated functions
create_snapshot_enabled_bucket(s3_client, 'my-bucket')
snapshots = list_snapshots(s3_client)
obj = read_from_snapshot(s3_client, 'file.txt')
create_my_fork(s3_client, 'my-fork')
```

### 3. Helper Functions

```python
from tigris_boto3_ext import (
    create_snapshot_bucket,
    create_snapshot,
    list_snapshots,
    create_fork,
    get_object_from_snapshot,
    get_snapshot_version,
    list_objects_from_snapshot,
    head_object_from_snapshot,
    has_snapshot_enabled,
    get_bucket_info,
)

# Create snapshot-enabled bucket
create_snapshot_bucket(s3_client, 'my-bucket')

# Check if bucket has snapshots enabled
if has_snapshot_enabled(s3_client, 'my-bucket'):
    print("Snapshots are enabled!")

# Get comprehensive bucket information
info = get_bucket_info(s3_client, 'my-bucket')
print(f"Snapshot enabled: {info['snapshot_enabled']}")

# Create snapshots
result = create_snapshot(s3_client, 'my-bucket', snapshot_name='backup-1')
version = get_snapshot_version(result)

# List snapshots
snapshots = list_snapshots(s3_client, 'my-bucket')

# Create forks
create_fork(s3_client, 'new-bucket', 'source-bucket', snapshot_version=version)

# Access snapshot data
obj = get_object_from_snapshot(s3_client, 'my-bucket', 'file.txt', version)
objects = list_objects_from_snapshot(s3_client, 'my-bucket', '12345', Prefix='data/')
metadata = head_object_from_snapshot(s3_client, 'my-bucket', 'file.txt', '12345')
```

### 4. Agent Kit

Higher-level workflows for AI agents — workspaces, parallel forks, checkpoints,
and event-driven coordination — composed on top of snapshots and forks. Mirrors
the [`@tigrisdata/agent-kit`](https://github.com/tigrisdata/storage/tree/main/packages/agent-kit) TypeScript package.

#### Workspaces

A workspace is a dedicated bucket for a single agent, with optional TTL for
auto-cleanup and optional snapshot support for checkpointing.

```python
from tigris_boto3_ext import create_workspace, teardown_workspace

ws = create_workspace(
    s3_client,
    'agent-abc',
    ttl_days=1,           # auto-expire objects after 1 day
    enable_snapshots=True, # allow checkpointing later
)

# ... agent reads/writes to ws.bucket ...

teardown_workspace(s3_client, ws)  # empties and deletes the bucket
```

#### Forks (parallel agent runs)

Snapshot a bucket, then fork it `count` times. Each fork is its own bucket,
created instantly via copy-on-write — agents can read/write without affecting
the base bucket or each other.

```python
from tigris_boto3_ext import create_forks, teardown_forks

forks = create_forks(s3_client, 'training-data', count=3, prefix='exp-42')

for fork in forks.forks:
    print(fork.bucket)  # 'exp-42-0', 'exp-42-1', 'exp-42-2'

teardown_forks(s3_client, forks)
```

#### Checkpoints

Capture a labeled snapshot you can later restore from. Restoring creates a new
fork at that point in time; the original bucket is untouched.

```python
from tigris_boto3_ext import checkpoint, restore, list_checkpoints

ck = checkpoint(s3_client, 'training-data', name='epoch-50')
print(ck.snapshot_id)

# Later — restore into a fresh fork
restored_bucket = restore(
    s3_client, 'training-data', ck.snapshot_id, fork_name='retry-1'
)

# List all checkpoints on a bucket
for c in list_checkpoints(s3_client, 'training-data'):
    print(c.snapshot_id, c.name, c.created_at)
```

#### Coordination

Configure webhook notifications on object events to drive event-driven
multi-agent pipelines without polling.

```python
from tigris_boto3_ext import setup_coordination, teardown_coordination

setup_coordination(
    s3_client,
    'pipeline-bucket',
    webhook_url='https://my-service.example/webhook',
    event_filter='WHERE `key` REGEXP "^results/"',
    auth_token='my-webhook-secret',
)

# Disable
teardown_coordination(s3_client, 'pipeline-bucket')
```

> **Note**: TTL on workspaces and webhook coordination use a Tigris-specific
> `PATCH /{bucket}` REST endpoint (not part of the S3 API). Per-workspace and
> per-fork scoped credentials are not yet supported — they require Tigris IAM
> integration outside the boto3 surface.

## Complete Examples

### Example 1: Backup and Restore Workflow

```python
import boto3
from tigris_boto3_ext import (
    create_snapshot_bucket,
    create_snapshot,
    list_snapshots,
    create_fork,
    get_snapshot_version,
)

s3 = boto3.client('s3')

# Create a snapshot-enabled bucket
create_snapshot_bucket(s3, 'production-data')

# Add some data
s3.put_object(Bucket='production-data', Key='important.txt', Body=b'critical data')

# Create a snapshot
snapshot_result = create_snapshot(s3, 'production-data', snapshot_name='daily-backup')
snapshot_version = get_snapshot_version(snapshot_result)

# List all snapshots
snapshots = list_snapshots(s3, 'production-data')
for bucket in snapshots.get('Buckets', []):
    print(f"Snapshot: {bucket['Name']}")

# Restore from snapshot by creating a fork
create_fork(s3, 'restored-data', 'production-data', snapshot_version=snapshot_version)
```

### Example 2: Testing with Snapshot Isolation

```python
import boto3
from tigris_boto3_ext import create_fork, create_snapshot, get_snapshot_version

s3 = boto3.client('s3')

# Create a snapshot of production data
snapshot_result = create_snapshot(s3, 'production-data', snapshot_name='test-snapshot')
snapshot_version = get_snapshot_version(snapshot_result)

# Fork for testing (isolated copy)
create_fork(s3, 'test-data', 'production-data', snapshot_version=snapshot_version)

# Run tests against test-db without affecting production
s3.put_object(Bucket='test-data', Key='test-data.txt', Body=b'test data')

# Clean up test bucket when done
s3.delete_bucket(Bucket='test-data')
```

### Example 3: Time-Travel Queries

```python
import boto3
from tigris_boto3_ext import get_object_from_snapshot, list_objects_from_snapshot

s3 = boto3.client('s3')

# Get object as it was at a specific snapshot
historical_obj = get_object_from_snapshot(
    s3,
    'my-bucket',
    'config.json',
    snapshot_version='12345'
)
old_config = historical_obj['Body'].read()

# List all objects in historical snapshot
historical_objects = list_objects_from_snapshot(
    s3,
    'my-bucket',
    snapshot_version='12345',
    Prefix='logs/2024/'
)

for obj in historical_objects.get('Contents', []):
    print(f"Historical object: {obj['Key']}")
```

### Example 4: Retrieving Bucket Snapshot and Fork Information

```python
import boto3
from tigris_boto3_ext import (
    create_snapshot_bucket,
    create_snapshot,
    create_fork,
    get_snapshot_version,
    has_snapshot_enabled,
    get_bucket_info,
)

s3 = boto3.client('s3')

# Check if a bucket has snapshots enabled
bucket_name = 'my-bucket'

create_snapshot_bucket(s3, bucket_name)

if has_snapshot_enabled(s3, bucket_name):
    print(f"✓ Snapshots are enabled for {bucket_name}")
else:
    print(f"✗ Snapshots are not enabled for {bucket_name}")

# Get comprehensive bucket information
info = get_bucket_info(s3, bucket_name)
print(f"Snapshot enabled: {info['snapshot_enabled']}")

# Example: Check fork lineage
source_bucket = 'production-data'
create_snapshot_bucket(s3, source_bucket)

# Create a snapshot
snapshot_result = create_snapshot(s3, source_bucket, snapshot_name='v1')
snapshot_version = get_snapshot_version(snapshot_result)

# Create a fork
forked_bucket = 'test-data'
create_fork(s3, forked_bucket, source_bucket, snapshot_version=snapshot_version)

# Inspect the fork
fork_info = get_bucket_info(s3, forked_bucket)
print(f"Forked from: {fork_info['fork_source_bucket']}")
print(f"Snapshot version: {fork_info['fork_source_snapshot']}")
```

### Example 5: Bundle API — Fetch Multiple Objects in One Request

```python
import tarfile
import boto3
from tigris_boto3_ext import bundle_objects, BundleError, BUNDLE_ON_ERROR_FAIL

s3 = boto3.client('s3')

# Fetch a batch of training images as a streaming tar archive
keys = [f"dataset/train/img_{i:05d}.jpg" for i in range(1000)]
response = bundle_objects(s3, 'my-dataset-bucket', keys)

with tarfile.open(fileobj=response, mode="r|") as tar:
    for member in tar:
        if member.name == "__bundle_errors.json":
            continue  # skip the error manifest
        f = tar.extractfile(member)
        if f is not None:
            image_bytes = f.read()
            # feed to training pipeline

# Use fail mode for inference where every object must be present
try:
    response = bundle_objects(
        s3, 'my-bucket', keys, on_error=BUNDLE_ON_ERROR_FAIL
    )
except BundleError as e:
    print(f"Bundle failed (HTTP {e.status_code}): {e.body}")
```

See [`examples/bundle_usage.py`](examples/bundle_usage.py) for more patterns including error handling, response metadata, and ML training batches.

## How It Works

This library uses boto3's event system to inject Tigris-specific headers into S3 API requests:

### Request Headers (Sent to Tigris)

- **`X-Tigris-Enable-Snapshot: true`** - Enables snapshot support for bucket creation
- **`X-Tigris-Snapshot: true; name=<name>`** - Creates a snapshot
- **`X-Tigris-Snapshot: <bucket_name>`** - Lists snapshots for a bucket
- **`X-Tigris-Snapshot-Version: <version>`** - Reads from specific snapshot version
- **`X-Tigris-Fork-Source-Bucket: <bucket>`** - Specifies fork source
- **`X-Tigris-Fork-Source-Bucket-Snapshot: <version>`** - Forks from specific snapshot

### Response Headers (Returned by Tigris)

The following custom headers are returned in HeadBucket responses and can be accessed via `get_bucket_info()` and `has_snapshot_enabled()`:

- **`X-Tigris-Enable-Snapshot: true`** - Present when snapshots are enabled for the bucket
- **`X-Tigris-Fork-Source-Bucket: <bucket_name>`** - Present on forked buckets, indicates the parent bucket
- **`X-Tigris-Fork-Source-Bucket-Snapshot: <version>`** - Present on forked buckets, indicates the snapshot version

The library registers event handlers on `before-sign.s3.*` events to add request headers transparently.

## Requirements

- Python 3.9+
- boto3 >= 1.26.0

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/tigrisdata/tigris-boto3-ext.git
cd tigris-boto3-ext

# Install with dev dependencies using uv
uv sync --all-extras

# Or with pip
pip install -e ".[dev]"
```

### Running Tests

#### Integration Tests

Integration tests run against a real Tigris S3 service. See [`tests/integration/README.md`](tests/integration/README.md) for detailed setup instructions.

```bash
# Set up environment variables
export AWS_ENDPOINT_URL_S3="https://t3.storage.dev"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"

# Run integration tests
uv run pytest tests/integration/ -v
```

### Code Quality

```bash
# Type checking
uv run mypy tigris_boto3_ext

# Linting
uv run ruff check tigris_boto3_ext

# Auto-fix linting issues
uv run ruff check --fix tigris_boto3_ext

# Code formatting
uv run ruff format tigris_boto3_ext

# Check formatting without making changes
uv run ruff format --check tigris_boto3_ext
```

## License

Apache-2.0

## Contributing

Contributions welcome! Please open an issue or PR on GitHub.

## Support

For issues and questions:

- GitHub Issues: <https://github.com/tigrisdata/tigris-boto3-ext/issues>
- Documentation: <https://www.tigrisdata.com/docs>
