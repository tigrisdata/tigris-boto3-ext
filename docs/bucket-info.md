# Bucket info

Tigris returns its bucket-level settings as custom headers on `HeadBucket`. boto3 does not surface them as response fields, so these helpers read the raw HTTP headers for you: whether a bucket has [snapshots](snapshots.md) enabled, and if it is a [fork](forks.md), where it came from.

Runnable example: [`examples/bucket_info_usage.py`](../examples/bucket_info_usage.py).

All names below are imported from `tigris_boto3_ext`. Both helpers make one `HeadBucket` request.

## Check for snapshots

```python
from tigris_boto3_ext import has_snapshot_enabled

if has_snapshot_enabled(s3, "my-bucket"):
    ...
```

## Read all Tigris metadata

```python
from tigris_boto3_ext import get_bucket_info

info = get_bucket_info(s3, "dev-environment")
info["snapshot_enabled"]      # bool
info["fork_source_bucket"]    # "production-data", or None if not a fork
info["fork_source_snapshot"]  # snapshot version the fork was taken from, or None
info["response_metadata"]     # the full HeadBucket response, for anything else
```

Both fork fields are `None` on a bucket that is not a fork.

## Headers

Read from the `HeadBucket` response:

| Header | Present when |
|---|---|
| `X-Tigris-Enable-Snapshot: true` | Snapshots are enabled on the bucket |
| `X-Tigris-Fork-Source-Bucket: <bucket>` | The bucket is a fork; names the source bucket |
| `X-Tigris-Fork-Source-Bucket-Snapshot: <version>` | The bucket is a fork; the source snapshot version |

## See also

- [Snapshots](snapshots.md) and [Forks](forks.md): the features these headers describe.
