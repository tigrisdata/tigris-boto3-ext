# Data migration

Tigris can take over a bucket that lives at another S3-compatible provider without an upfront copy. You point the bucket in Tigris at the source (the *shadow bucket*); a read of an object Tigris does not have yet is served from the source and copied into Tigris in the background, so only data that is actually used moves. With write-through on, new writes go to both buckets, so the source stays current until you decommission it. `set_bucket_migration` configures the source; the object helpers schedule, check and list the migration.

Tigris docs: [Migrate to Tigris](https://www.tigrisdata.com/docs/migration/), with per-provider guides for the endpoint, region and credential formats.
Runnable example: [`examples/migration_usage.py`](../examples/migration_usage.py).

All names are imported from `tigris_boto3_ext`.

## Configure the source

```python
import os

from tigris_boto3_ext import set_bucket_migration

set_bucket_migration(
    s3, "assets", "legacy-assets",
    endpoint="https://s3.amazonaws.com", region="us-east-1",
    access_key=os.environ["LEGACY_ACCESS_KEY"], secret_key=os.environ["LEGACY_SECRET_KEY"],
    write_through=True,
)

# Later: stop migrating
set_bucket_migration(s3, "assets", None)
```

It is a Tigris endpoint rather than an S3 operation, so it returns a plain dict and raises `TigrisAPIError` on gateway errors. The gateway tries the source with the credentials before it stores anything, so a typo in a key fails here (`403 InvalidAccessKeyId`, relayed from the source) rather than at the first read. The source must be outside Tigris: a Tigris endpoint is refused (`400 InvalidArgument`, "Migration source cannot be Tigris endpoint"); to copy between Tigris buckets, [fork](forks.md) instead. For providers without regions, such as R2 or GCS, use `region="auto"`.

| Argument | Values | Effect |
|---|---|---|
| `source_bucket` | bucket name, or `None` | The bucket to migrate from; `None` removes the configuration |
| `endpoint` | `https://…` | The source's S3 endpoint |
| `region` | region code, or `"auto"` | The source bucket's region |
| `access_key`, `secret_key` | strings | Credentials for the source |
| `write_through` | `True`, `False` (default) | Also write new objects to the source |

`endpoint` must be `https://` with a host; all four source settings are required together. These are checked before any request is sent (`ValueError`). The stored configuration is available under [`get_bucket_metadata`](bucket-info.md)`["raw"]["shadow_bucket"]`.

## Drive the migration

Objects move when they are read. To move them without waiting for a read, or to see how far the migration has come:

```python
from tigris_boto3_ext import TigrisListSource, is_object_migrated, list_objects_from_source, migrate_object, with_list_source

# What is still only at the source?
remaining = list_objects_from_source(s3, "assets", "shadow", Prefix="images/")

# Schedule an object's copy instead of waiting for its first read
for obj in remaining.get("Contents", []):
    if not is_object_migrated(s3, "assets", obj["Key"]):
        migrate_object(s3, "assets", obj["Key"])

# Context manager and decorator: plain list_objects_v2 calls list the chosen side
with TigrisListSource(s3, "tigris"):
    migrated = s3.list_objects_v2(Bucket="assets")

@with_list_source("shadow")
def remaining_keys(s3_client, bucket):
    return [o["Key"] for o in s3_client.list_objects_v2(Bucket=bucket).get("Contents", [])]
```

`list_objects_from_source` (and the context manager and decorator behind it) lists one side of the bucket: `"tigris"` for objects already in Tigris, `"shadow"` for objects in the source. Without it, a plain listing shows the full source contents when write-through is on and only the migrated objects when it is off. `migrate_object` is a `head_object` that schedules the object's copy; `is_object_migrated` reads the `X-Tigris-Read-Source` response header of a `head_object` and is `False` while the object still has to come from the source. On a bucket without a source, all three are ordinary `head_object` / `list_objects_v2` calls.

## Headers and endpoint

| Operation | Header | Meaning |
|---|---|---|
| `ListObjectsV2` | `X-Tigris-List-Source: tigris` / `shadow` | Which side of the bucket to list |
| `HeadObject` | `X-Tigris-Schedule-Migration: true` | Copy the object from the source now |
| `HeadObject` response | `X-Tigris-Read-Source: block_shadow` | The object is not in Tigris yet |
| `PATCH /{bucket}` | `{"shadow_bucket": {"name", "endpoint", "region", "access_key", "secret_key", "write_through"}}` | Configure the source; `{}` removes it |

**Concurrency.** As with every header feature here, `TigrisListSource` and `migrate_object` register the header on the client for the duration of the call or block, so a `list_objects_v2` or `head_object` issued from another thread on the same client in that window picks it up too. Use a client per thread; see [Thread safety](../README.md#thread-safety).

## See also

- [Bucket settings](settings.md): the other settings on the same endpoint.
- [Forks](forks.md): copying between Tigris buckets.
