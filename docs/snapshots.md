# Snapshots

A snapshot captures the state of a whole bucket at a point in time. Creating one is instant and zero-copy regardless of bucket size, and snapshots are immutable. Once a bucket has snapshots you can list them, read any object as it was at a snapshot, delete snapshots you no longer need, and [fork](forks.md) a new bucket from one.

Tigris docs: [Bucket snapshots and forks](https://www.tigrisdata.com/docs/buckets/snapshots-and-forks/).
Runnable example: [`examples/snapshots_usage.py`](../examples/snapshots_usage.py).

All names below are imported from `tigris_boto3_ext`.

## Enable snapshots on a bucket

Snapshots are switched on when the bucket is created. Existing buckets can be switched in the Tigris dashboard under Bucket Settings.

```python
from tigris_boto3_ext import TigrisSnapshotEnabled, create_snapshot_bucket, snapshot_enabled

# Helper: one call
create_snapshot_bucket(s3, "my-bucket")

# Context manager: every create_bucket inside gets the header
with TigrisSnapshotEnabled(s3):
    s3.create_bucket(Bucket="bucket-1")
    s3.create_bucket(Bucket="bucket-2")

# Decorator: the wrapped function must take the client as its first argument
@snapshot_enabled
def create_backup_bucket(s3_client, bucket_name):
    return s3_client.create_bucket(Bucket=bucket_name)
```

Use [`has_snapshot_enabled`](bucket-info.md) to check an existing bucket.

## Create a snapshot

```python
from tigris_boto3_ext import create_snapshot, get_snapshot_version

response = create_snapshot(s3, "my-bucket", snapshot_name="daily-backup")
version = get_snapshot_version(response)  # e.g. "1787441627070249004"
```

`snapshot_name` is optional. The version is what every other snapshot operation takes; keep it.

## List snapshots

```python
from tigris_boto3_ext import TigrisSnapshot, list_snapshots, with_snapshot

# Helper
snapshots = list_snapshots(s3, "my-bucket")

# Context manager: list_buckets lists snapshots instead of buckets
with TigrisSnapshot(s3, "my-bucket"):
    snapshots = s3.list_buckets()

# Decorator
@with_snapshot("my-bucket")
def snapshots_of_my_bucket(s3_client):
    return s3_client.list_buckets()
```

The response has the shape of `list_buckets`. Each snapshot appears under `Buckets` as a pseudo-bucket whose `Name` is `<version>; name=<snapshot name>` and whose `CreationDate` is when the snapshot was taken:

```python
for entry in snapshots.get("Buckets", []):
    version = entry["Name"].split(";")[0]
    print(version, entry["CreationDate"])
```

## Read from a snapshot

Pass the snapshot version and the read goes to that point in time instead of the live bucket. Reads are `get_object`, `head_object`, `list_objects_v2` and `list_objects`; writes are not affected.

```python
from tigris_boto3_ext import (
    TigrisSnapshot,
    get_object_from_snapshot,
    head_object_from_snapshot,
    list_objects_from_snapshot,
    with_snapshot,
)

# Helpers; extra keyword arguments go to the underlying boto3 call
obj = get_object_from_snapshot(s3, "my-bucket", "config.json", version)
old_config = obj["Body"].read()
listing = list_objects_from_snapshot(s3, "my-bucket", version, Prefix="logs/2024/")
metadata = head_object_from_snapshot(s3, "my-bucket", "config.json", version)

# Context manager: plain boto3 calls, all reading from the snapshot
with TigrisSnapshot(s3, "my-bucket", snapshot_version=version):
    obj = s3.get_object(Bucket="my-bucket", Key="config.json")
    listing = s3.list_objects_v2(Bucket="my-bucket")

# Decorator
@with_snapshot("my-bucket", snapshot_version=version)
def read_historical(s3_client, key):
    return s3_client.get_object(Bucket="my-bucket", Key=key)
```

## Delete a snapshot

```python
from tigris_boto3_ext import delete_snapshot

delete_snapshot(s3, "my-bucket", version)
```

Other snapshots and the bucket itself are untouched. Deleting an unknown version raises a `ClientError`.

`delete_snapshot` sends a `DeleteBucket` request with a snapshot header, so it refuses to run while another bucket-deleting helper (`delete_snapshot`, [`force_delete_bucket`](force-delete.md)) is in flight on the same client. Use one client per thread if you delete concurrently.

## Headers

| Operation | Header | Meaning |
|---|---|---|
| `CreateBucket` | `X-Tigris-Enable-Snapshot: true` | Create the bucket with snapshots enabled |
| `CreateBucket` | `X-Tigris-Snapshot: true; name=<name>` | Create a snapshot of the bucket |
| `ListBuckets` | `X-Tigris-Snapshot: <bucket>` | List the bucket's snapshots |
| `GetObject`, `HeadObject`, `ListObjectsV2`, `ListObjects` | `X-Tigris-Snapshot-Version: <version>` | Read from that snapshot |
| `DeleteBucket` | `X-Tigris-Snapshot-Version: <version>` | Delete that snapshot |

## See also

- [Forks](forks.md): restore or branch a bucket from a snapshot.
- [Bucket info](bucket-info.md): check whether a bucket has snapshots enabled.
