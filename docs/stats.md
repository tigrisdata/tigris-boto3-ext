# Storage stats

How much the account holds: buckets, objects, unique objects and bytes — the counters Tigris's dashboard shows.

Tigris docs: the [`tigris buckets`](https://www.tigrisdata.com/docs/cli/buckets/) CLI, whose `list` is the same listing these counters come with.

Runnable example: [`examples/stats_usage.py`](../examples/stats_usage.py).

`get_stats` is a Tigris endpoint rather than an S3 operation, so it returns a plain dict and raises `TigrisAPIError` on gateway errors. Imported from `tigris_boto3_ext`.

```python
from tigris_boto3_ext import get_stats

stats = get_stats(s3)
stats["active_buckets"]        # buckets in the account
stats["total_objects"]         # objects across all versions
stats["total_unique_objects"]  # distinct objects
stats["total_storage_bytes"]
```

## How it is computed

Tigris returns `Stats` with a bucket listing (`GET /?IncludeStats=true`), and the numbers describe the buckets *in that response*, not the account. The gateway computes them per request, and a single request for a few hundred buckets exceeds boto3's default 60-second read timeout, so `get_stats` walks the account 50 buckets per request and sums — about one request per 50 buckets, each bounded by your client's `read_timeout`. For the per-page numbers of a listing you are already making, use `list_buckets(..., include_stats=True)["stats"]` (see [listing](listing.md)).

The counters are the gateway's estimates, like the size fields of [bucket info](bucket-info.md): close, not an inventory.

## Endpoint

| Request | Meaning |
|---|---|
| `GET /?IncludeStats=true` | Bucket listing with `Stats` for the returned buckets |
