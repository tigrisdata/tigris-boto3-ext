# Archive restore

Objects in the `GLACIER` storage class are archived: they need a restore before they can be read, the restore runs in the background and typically takes about an hour, and the restored copy returns to the archive after the number of days you asked for. `restore_object` requests the restore and `get_restore_info` tells you where an object stands. `GLACIER_IR` objects are readable at once and need neither.

Tigris docs: [Storage tiers](https://www.tigrisdata.com/docs/objects/tiers/), including restoring from the Archive tier.
Runnable example: [`examples/restore_usage.py`](../examples/restore_usage.py).

Both helpers are plain S3 operations (`RestoreObject`, `HeadObject`), so gateway errors are boto3's `ClientError`. All names are imported from `tigris_boto3_ext`.

## Restore an archived object

```python
from tigris_boto3_ext import get_restore_info, restore_object

info = get_restore_info(s3, "archive", "reports/2024.parquet")
# {'status': 'archived', 'expires_at': None}

restore_object(s3, "archive", "reports/2024.parquet", days=3)

get_restore_info(s3, "archive", "reports/2024.parquet")
# {'status': 'in-progress', 'expires_at': None}
# ... about an hour later:
# {'status': 'restored', 'expires_at': datetime(2026, 10, 26, ..., tzinfo=timezone.utc)}
```

`get_restore_info` returns `None` for an object that is not archived and has no restore state (`STANDARD`, `STANDARD_IA`, `GLACIER_IR`); a missing key raises the gateway's 404. Both take `version_id=` to look at a specific version.

| Status | Meaning |
|---|---|
| `archived` | In `GLACIER`, no restore requested |
| `in-progress` | A restore is running |
| `restored` | Readable until `expires_at`, then back to the archive |

What the gateway answers: a second `restore_object` while one is running is refused with `RestoreAlreadyInProgress` (409); a restore on a `GLACIER_IR` object with `InvalidObjectState` (403); `days` must be at least 1, checked before the request is sent.

`restore_object` here is the archive thaw. The soft-delete undelete is [`restore_deleted_object`](soft-delete.md); it uses the same S3 operation with a Tigris header, so `restore_object` refuses to run while a `restore_deleted_object` is in flight on the same client.

## Headers

| Operation | Field | Meaning |
|---|---|---|
| `RestoreObject` | `RestoreRequest.Days` | How long the restored copy stays readable |
| `HeadObject` response | `x-amz-restore: ongoing-request="true"` | Restore in progress |
| `HeadObject` response | `x-amz-restore: ongoing-request="false", expiry-date="…"` | Restored, with the return date |
| `HeadObject` response | `x-amz-storage-class: GLACIER` | Archived (no restore header) |

## See also

- [Lifecycle rules](lifecycle.md): moving objects into `GLACIER` automatically.
- [Soft delete](soft-delete.md): the other kind of restore.
