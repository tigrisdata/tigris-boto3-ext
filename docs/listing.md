# Bucket listing and stats

boto3's `list_buckets` gives you names and creation dates. Tigris's own listing can also say which buckets have snapshots enabled, which are public, where their data lives, which are forks and of what, and who owns them. `list_buckets` asks for exactly the details you want.

Tigris docs: [Bucket snapshots and forks](https://www.tigrisdata.com/docs/buckets/snapshots-and-forks/) for what the type and fork fields mean, and the [`tigris buckets`](https://www.tigrisdata.com/docs/cli/buckets/) CLI, whose `list` is this same listing.
Runnable example: [`examples/listing_usage.py`](../examples/listing_usage.py).

It is a Tigris endpoint rather than an S3 operation, so it returns a plain dict and raises `TigrisAPIError` on gateway errors. All names are imported from `tigris_boto3_ext`.

## List buckets with details

```python
from tigris_boto3_ext import list_buckets

page = list_buckets(s3, include_type=True, include_regions=True, include_visibility=True, include_fork_info=True)
for bucket in page["buckets"]:
    print(bucket["name"], bucket["snapshot_enabled"], bucket["regions"] or "global", bucket["visibility"])
    for parent in bucket["fork_parents"]:
        print("  forked from", parent["bucket"], "at snapshot", parent["snapshot"])

# Paging: continuation_token is None on the last page
page = list_buckets(s3, max_buckets=100)
while page["continuation_token"]:
    page = list_buckets(s3, max_buckets=100, continuation_token=page["continuation_token"])
```

Each entry always has `name`, `created_at`, `modified_at` and `soft_delete` (`{"enabled", "retention_days"}`). The other fields are filled only when you ask, and are `None` otherwise:

| Flag | Fills | Values |
|---|---|---|
| `include_type` | `snapshot_enabled` | `True` / `False` |
| `include_visibility` | `visibility` | `"public"` / `"private"` |
| `include_regions` | `regions` | list of region codes; `[]` means global |
| `include_fork_info` | `is_fork_parent`, `fork_parents` | as in [bucket info](bucket-info.md) |
| `include_owner` | `owner` | `{"user_id", "human_user_id"}` |
| `include_stats` | `page["stats"]` | storage counters for the buckets on this page; see [stats](stats.md) for the account |

Filters: `only_forks=True` lists only buckets that are forks; `owned_by="<username>"` only that user's buckets. `page["owner"]` is the account owner; `page["raw"]` the full response.

## Forks of a bucket

`list_forks` is on the [forks page](forks.md): same entry shape, filtered by the gateway to one bucket's children.

## Endpoints

| Request | Meaning |
|---|---|
| `GET /?IncludeTypeInfo=true&IncludeRegionsInfo=true&IncludeVisibility=true&IncludeForkInfo=true&IncludeOwnerInfo=true` | Listing with details; each flag adds a field |
| `GET /?OnlyForked=true`, `GET /?OwnedBy=<username>` | Filters |
| `max-buckets`, `continuation-token` | Paging |

Sent as signed requests with `Accept: application/json`.

## See also

- [Bucket info](bucket-info.md): the same details for one bucket, plus size estimates and settings.
- [Storage stats](stats.md): account totals, summed from this listing.
- [Soft delete](soft-delete.md): `list_deleted_buckets` is this listing with `OnlyDeleted=true`.
