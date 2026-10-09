# Force delete

S3 refuses to delete a bucket that still contains objects, so the usual routine is to list, delete everything, then delete the bucket. Tigris lets you skip that with one `DeleteBucket` request carrying the `X-Tigris-Force-Delete: true` header.

Runnable example: [`examples/force_delete_usage.py`](../examples/force_delete_usage.py).

## Delete a non-empty bucket

```python
from tigris_boto3_ext import force_delete_bucket

force_delete_bucket(s3, "my-bucket")
```

What happens to the data depends on the bucket:

| Bucket | Outcome |
|---|---|
| [Soft delete](soft-delete.md) enabled | The bucket and its objects move to the recoverable state for the retention window |
| Soft delete not enabled | The bucket and every object in it are removed permanently; this cannot be recovered, not even by support |

After the call, `head_bucket` on the name returns 404.

## Notes

- **One bucket deletion at a time per client.** The header is injected through the client's event system, so it applies to every `delete_bucket` issued through that client while the call is in flight. `force_delete_bucket` refuses to overlap with another bucket-deleting helper on the same client (`delete_snapshot`, another `force_delete_bucket`) and raises `RuntimeError`. Use a client per thread if you delete concurrently.
- **Check before you force.** On a bucket without soft delete this is irreversible, and it takes every object in the bucket with it. `get_bucket_info` does not report soft delete yet; if you are unsure how the bucket was created, empty it with normal deletes first.

## Headers

| Operation | Header | Meaning |
|---|---|---|
| `DeleteBucket` | `X-Tigris-Force-Delete: true` | Delete the bucket together with its contents |

## See also

- [Soft delete](soft-delete.md): make bucket deletion recoverable.
- [Snapshots](snapshots.md): `delete_snapshot` shares the same one-at-a-time rule.
