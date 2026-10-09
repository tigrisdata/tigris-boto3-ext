# Forks

A fork is a new bucket that starts as a copy of an existing bucket, either as it is now or as it was at one of its [snapshots](snapshots.md). Nothing is copied: the fork shares the source's objects by reference and stores only what you add or change afterwards. Writes to either bucket never affect the other, which makes forks a cheap way to restore from a snapshot, spin up a test environment, or try a migration.

Tigris docs: [Bucket snapshots and forks](https://www.tigrisdata.com/docs/buckets/snapshots-and-forks/).
Runnable example: [`examples/forks_usage.py`](../examples/forks_usage.py).

All names below are imported from `tigris_boto3_ext`. The source bucket must have snapshots enabled.

## Fork a bucket

```python
from tigris_boto3_ext import TigrisFork, create_fork, forked_from

# Helper: fork the source as it is now
create_fork(s3, "dev-environment", "production-data")

# Helper: fork a specific snapshot
create_fork(s3, "restored-data", "production-data", snapshot_version=version)

# Context manager: every create_bucket inside becomes a fork of the source
with TigrisFork(s3, "production-data"):
    s3.create_bucket(Bucket="dev-environment")

with TigrisFork(s3, "production-data", snapshot_version=version):
    s3.create_bucket(Bucket="test-environment")

# Decorator: the wrapped function must take the client as its first argument
@forked_from("production-data", snapshot_version=version)
def create_test_environment(s3_client, name):
    return s3_client.create_bucket(Bucket=name)
```

Without `snapshot_version`, Tigris takes a new snapshot of the source at that moment and forks from it.

## Restore from a snapshot

Forking is how you restore: the fork is the bucket as it was, under a new name.

```python
from tigris_boto3_ext import create_fork, create_snapshot, get_snapshot_version

version = get_snapshot_version(create_snapshot(s3, "production-data", snapshot_name="before-migration"))
# ... the migration goes wrong ...
create_fork(s3, "production-data-restored", "production-data", snapshot_version=version)
```

## Inspect a fork

[`get_bucket_info`](bucket-info.md) reports where a fork came from:

```python
from tigris_boto3_ext import get_bucket_info

info = get_bucket_info(s3, "dev-environment")
info["fork_source_bucket"]    # "production-data"
info["fork_source_snapshot"]  # the snapshot version the fork was taken from
```

## Headers

| Operation | Header | Meaning |
|---|---|---|
| `CreateBucket` | `X-Tigris-Fork-Source-Bucket: <bucket>` | Create the bucket as a fork of `<bucket>` |
| `CreateBucket` | `X-Tigris-Fork-Source-Bucket-Snapshot: <version>` | Fork from that snapshot instead of the current state |

## See also

- [Snapshots](snapshots.md): create the point in time to fork from.
- [Bucket info](bucket-info.md): read a fork's source bucket and snapshot.
