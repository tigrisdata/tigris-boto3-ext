# Bucket info

Two ways to ask Tigris about a bucket. `HeadBucket` returns a few settings as custom headers, which boto3 does not surface as response fields; `has_snapshot_enabled` and `get_bucket_info` read them for you: whether a bucket has [snapshots](snapshots.md) enabled, and if it is a [fork](forks.md), where it came from. For everything else — placement, default storage class, fork children, soft delete, size — `get_bucket_metadata` reads Tigris's metadata endpoint.

Runnable example: [`examples/bucket_info_usage.py`](../examples/bucket_info_usage.py).

All names below are imported from `tigris_boto3_ext`. Each helper makes one request.

## Check for snapshots

```python
from tigris_boto3_ext import has_snapshot_enabled

if has_snapshot_enabled(s3, "my-bucket"):
    ...
```

## Read the HeadBucket metadata

```python
from tigris_boto3_ext import get_bucket_info

info = get_bucket_info(s3, "dev-environment")
info["snapshot_enabled"]      # bool
info["fork_source_bucket"]    # "production-data", or None if not a fork
info["fork_source_snapshot"]  # snapshot version the fork was taken from, or None
info["response_metadata"]     # the full HeadBucket response, for anything else
```

Both fork fields are `None` on a bucket that is not a fork.

## Read everything: the metadata endpoint

`get_bucket_metadata` calls `GET /{bucket}?metadata`, a Tigris endpoint rather than an S3 operation, so the request is signed and sent directly with the client's credentials and the result is a plain dict:

```python
from tigris_boto3_ext import TigrisAPIError, get_bucket_metadata

info = get_bucket_metadata(s3, "production-data")
info["snapshot_enabled"]   # True
info["storage_class"]      # "STANDARD", the default for new objects
info["regions"]            # ["iad", "sjc"]; [] means global placement
info["is_fork_parent"]     # True once another bucket has been forked from this one
info["soft_delete"]        # {"enabled": True, "retention_days": 30} or {"enabled": False, "retention_days": None}
info["created_at"]         # datetime, UTC

child = get_bucket_metadata(s3, "dev-environment")
for parent in child["fork_parents"]:
    parent["bucket"], parent["snapshot"], parent["snapshot_name"]   # snapshot_name is None for an auto snapshot
    parent["forked_at"], parent["snapshot_created_at"]

sized = get_bucket_metadata(s3, "production-data", with_size=True)
sized["size"]              # {"objects": 3, "bytes": 3072, "objects_all_versions": 3}; None without with_size
```

The size values are server-side estimates that are updated asynchronously; right after writes they can trail by a few seconds, so treat them as approximate rather than as an inventory. `fork_parents` on a fork is recorded with the fork itself; `is_fork_parent` on the source bucket is refreshed asynchronously and can lag a new fork, so read the child's `fork_parents` when you need the lineage right away.

Also present: `name`, `allow_object_acl`, `cache_control`, `custom_domain`. Settings families that have no read helper of their own yet — lifecycle rules, CORS, notifications, shadow bucket, shares — are available under `info["raw"]`, the full response body in its wire shape.

A gateway error raises `TigrisAPIError`, with `status_code`, `code` (such as `NoSuchBucket`), `message` and `request_id`:

```python
try:
    get_bucket_metadata(s3, "no-such-bucket")
except TigrisAPIError as e:
    print(e.status_code, e.code)   # 404 NoSuchBucket
```

## Headers

Read from the `HeadBucket` response:

| Header | Present when |
|---|---|
| `X-Tigris-Enable-Snapshot: true` | Snapshots are enabled on the bucket |
| `X-Tigris-Fork-Source-Bucket: <bucket>` | The bucket is a fork; names the source bucket |
| `X-Tigris-Fork-Source-Bucket-Snapshot: <version>` | The bucket is a fork; the source snapshot version |

## See also

- [Snapshots](snapshots.md) and [Forks](forks.md): the features these headers describe.
- [Create bucket options](create-bucket.md): where `storage_class` and `regions` are set.
