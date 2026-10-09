# Soft delete

On a bucket with soft delete enabled, deleting an object does not remove it. The deleted version stays recoverable for the bucket's retention window, 7 to 90 days, after which it is removed for good. During the window you can list what is recoverable, restore it, or purge it early. The bucket itself is covered too: deleting the bucket moves it to a recoverable state instead of destroying it.

Tigris docs: [Soft delete](https://www.tigrisdata.com/docs/buckets/soft-delete/).
Runnable example: [`examples/soft_delete_usage.py`](../examples/soft_delete_usage.py).

All names below are imported from `tigris_boto3_ext`.

## Enable soft delete on a bucket

Soft delete is switched on when the bucket is created. `retention_days` is a whole number from 7 to 90; leave it out for the default 7-day window. Out-of-range values raise `ValueError`, non-integers raise `TypeError`, both before any request is sent.

```python
from tigris_boto3_ext import TigrisSoftDeleteEnabled, create_soft_delete_bucket, soft_delete_enabled

# Helper: one call
create_soft_delete_bucket(s3, "my-bucket")                      # 7 days
create_soft_delete_bucket(s3, "my-archive", retention_days=30)  # 30 days

# Context manager: every create_bucket inside gets the same window
with TigrisSoftDeleteEnabled(s3, retention_days=14):
    s3.create_bucket(Bucket="reports-2026")
    s3.create_bucket(Bucket="exports-2026")

# Decorator (called, even without arguments); the wrapped function takes the client first
@soft_delete_enabled(retention_days=90)
def create_compliance_bucket(s3_client, name):
    return s3_client.create_bucket(Bucket=name)
```

## See what is recoverable

Soft-deleted objects do not show up in normal listings. The soft-delete view switches `list_objects_v2` and `list_object_versions` to return soft-deleted versions instead of live ones; live objects are left out entirely.

```python
from tigris_boto3_ext import (
    TigrisSoftDeleteView,
    list_deleted_object_versions,
    list_deleted_objects,
    with_soft_delete_view,
)

# Helpers; extra keyword arguments go to the underlying boto3 call
deleted = list_deleted_objects(s3, "my-bucket", Prefix="logs/")       # list_objects_v2 shape
versions = list_deleted_object_versions(s3, "my-bucket")              # list_object_versions shape
for v in versions.get("Versions", []):
    print(v["Key"], v["VersionId"])

# Context manager: plain boto3 calls, all in the soft-delete view
with TigrisSoftDeleteView(s3):
    deleted = s3.list_object_versions(Bucket="my-bucket")

# Decorator
@with_soft_delete_view
def deleted_keys(s3_client, bucket):
    return [v["Key"] for v in s3_client.list_object_versions(Bucket=bucket).get("Versions", [])]
```

`list_deleted_object_versions` is the one that gives you version ids, which restore and purge need.

## Restore an object

```python
from tigris_boto3_ext import restore_deleted_object

restore_deleted_object(s3, "my-bucket", "report.pdf")                        # most recent soft-deleted version
restore_deleted_object(s3, "my-bucket", "report.pdf", "1787441627070249004") # a specific version
```

This is the undelete counterpart of purge. It is unrelated to a plain `restore_object`, which thaws an object from an archive tier.

## Purge an object early

Purging removes a soft-deleted version before its window ends. It cannot be undone.

```python
from tigris_boto3_ext import purge_deleted_object

purge_deleted_object(s3, "my-bucket", "secrets.env", version_id)
```

The version id is required. Inside the soft-delete view, a `delete_object` *without* `VersionId` would target the live object and soft-delete it again rather than purge anything, so the helper refuses to run without one.

## Delete the bucket

`s3.delete_bucket` works once the bucket is empty of live objects. To delete a bucket that still has objects, use [`force_delete_bucket`](force-delete.md). Either way, on a soft-delete bucket the deletion is recoverable for the retention window.

Restoring a deleted *bucket* is not in this library yet; use the Tigris dashboard (Buckets → Deleted) in the meantime.

## Notes

- **Scope of the view.** While `TigrisSoftDeleteView` or `@with_soft_delete_view` is active, every `delete_object`, `list_objects_v2` and `list_object_versions` on that client is in the view, including calls from other threads. Prefer the helpers, which scope the view to one call.
- **One restore at a time per client.** `restore_deleted_object` injects the version through the client's event system and refuses to overlap with another restore on the same client (`RuntimeError`) rather than risk carrying the wrong version. Use a client per thread if you restore concurrently.
- **Snapshot buckets.** The view returns only soft-deleted versions there too; live versions are never mixed in.
- **Test cleanup.** A deleted soft-delete bucket stays in the deleted list until its window ends, so test suites that create such buckets should give them unique names.

## Headers

| Operation | Header | Meaning |
|---|---|---|
| `CreateBucket` | `X-Tigris-Soft-Delete: true` or `<days>` | Enable soft delete with the default or a 7–90 day window |
| `ListObjectsV2`, `ListObjectVersions` | `X-Tigris-Soft-Delete: true` | List soft-deleted objects instead of live ones |
| `DeleteObject` (with `VersionId`) | `X-Tigris-Soft-Delete: true` | Purge that soft-deleted version |
| `RestoreObject` | `X-Tigris-Restore-Type: soft-delete` | Restore a soft-deleted object instead of thawing an archived one |
| `RestoreObject` | `X-Tigris-Restore-Version: <version>` | Restore that version rather than the most recent |

## See also

- [Force delete](force-delete.md): delete a bucket that still contains objects.
