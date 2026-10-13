# Create bucket options

S3's `CreateBucket` has no way to say where a bucket's data should live, what storage class new objects get, or whether the public may list it. Tigris takes those as headers on the request, and `access="public"` is the standard `public-read` ACL. This feature sets them in one call.

Runnable example: [`examples/create_bucket_usage.py`](../examples/create_bucket_usage.py).

All names below are imported from `tigris_boto3_ext`. Options you leave out are not sent, so the gateway defaults apply: private, `STANDARD`, global placement, no anonymous listing.

## Create a bucket with options

```python
from tigris_boto3_ext import TigrisBucketOptions, create_bucket, with_bucket_options

# Helper: one call
create_bucket(s3, "assets", access="public", regions="iad")
create_bucket(s3, "archive", default_storage_class="STANDARD_IA", regions=["iad", "sjc"])
create_bucket(s3, "downloads", access="public", public_list_objects=True)

# Context manager: every create_bucket inside gets the same options
with TigrisBucketOptions(s3, regions="eur", default_storage_class="GLACIER_IR"):
    s3.create_bucket(Bucket="cold-eu-1")
    s3.create_bucket(Bucket="cold-eu-2", ACL="public-read")

# Decorator (called): the wrapped function takes the client first
@with_bucket_options(regions="iad")
def create_us_bucket(s3_client, name):
    return s3_client.create_bucket(Bucket=name)
```

Extra keyword arguments to `create_bucket` go to boto3's `create_bucket`; pass either `access` or your own `ACL`, not both.

## The options

| Option | Values | Effect |
|---|---|---|
| `access` | `"public"`, `"private"` | `public` adds the `public-read` ACL: anonymous `GET https://<bucket>.t3.tigrisfiles.io/<key>` works (the [public bucket domains](https://www.tigrisdata.com/docs/buckets/public-bucket/), not the API endpoint). Private is the default and is also what you get when you say nothing. |
| `default_storage_class` | `STANDARD`, `STANDARD_IA`, `GLACIER`, `GLACIER_IR` | Storage class for objects uploaded without one of their own |
| `regions` | a region code (`"iad"`), a multi-region code (`"usa"`, `"eur"`), or a list of region codes (`["iad", "sjc"]`) | Single, multi or dual-region placement; omitted means global. Codes are validated by the gateway, see the [Tigris regions list](https://www.tigrisdata.com/docs/concepts/regions/). |
| `public_list_objects` | `True`, `False` | Whether anonymous requests may list the bucket. Off by default even on public buckets; requires `access="public"`. |

Unknown storage classes and empty region lists are rejected before any request is sent.

**Concurrency.** As with every feature here, the options are registered on the client for the duration of the call or block, so a `create_bucket` issued from another thread on the same client in that window picks them up too. Use a client per thread for concurrent bucket creation, especially when enabling `public_list_objects`; see [Thread safety](../README.md#thread-safety).

## Combining with snapshots, soft delete and forks

Those are creation-time settings too, each with its own context manager, and they nest with this feature — the headers merge:

```python
from tigris_boto3_ext import TigrisSnapshotEnabled, TigrisSoftDeleteEnabled, create_bucket

with TigrisSnapshotEnabled(s3), TigrisSoftDeleteEnabled(s3, retention_days=30):
    create_bucket(s3, "production-data", access="private", regions="usa")
```

## Headers

| Operation | Header | Meaning |
|---|---|---|
| `CreateBucket` | `X-Amz-Storage-Class: <class>` | Default storage class for new objects |
| `CreateBucket` | `X-Tigris-Regions: <code>[,<code>...]` | Data placement |
| `CreateBucket` | `X-Amz-Acl-Public-List-Objects-Enabled: true/false` | Anonymous listing on a public bucket |

`access="public"` is not a header: it is the `ACL=public-read` request parameter, readable back with `get_bucket_acl` as a `READ` grant to `AllUsers`.

## See also

- [Snapshots](snapshots.md), [Soft delete](soft-delete.md), [Forks](forks.md): the other creation-time settings.
- [Bucket info](bucket-info.md): what `HeadBucket` reports about a bucket.
