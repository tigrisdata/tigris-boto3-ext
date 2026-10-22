# Bucket settings

A bucket's access, default storage class, data placement, `Cache-Control`, custom domain, response headers and snapshot type can all be changed after it exists. S3 has no single call for that; Tigris takes them as one update to the bucket. `update_bucket` sends exactly the settings you pass and leaves the rest alone.

Tigris docs: [Bucket settings](https://www.tigrisdata.com/docs/buckets/settings/), with the pages it links for [public access](https://www.tigrisdata.com/docs/buckets/public-bucket/), [storage tiers](https://www.tigrisdata.com/docs/objects/tiers/), [regions](https://www.tigrisdata.com/docs/concepts/regions/), [custom domains](https://www.tigrisdata.com/docs/buckets/custom-domain/) and [snapshots and forks](https://www.tigrisdata.com/docs/buckets/snapshots-and-forks/).
Runnable example: [`examples/settings_usage.py`](../examples/settings_usage.py).

It is a Tigris endpoint rather than an S3 operation, so it returns a plain dict and raises `TigrisAPIError` on gateway errors. All names are imported from `tigris_boto3_ext`.

## Update a bucket

```python
from tigris_boto3_ext import update_bucket

update_bucket(s3, "assets", access="public", cache_control="public, max-age=3600")
update_bucket(s3, "archive", default_storage_class="STANDARD_IA", regions=["iad", "sjc"])
update_bucket(s3, "downloads", public_list_objects=True, nosniff=True)
update_bucket(s3, "www", custom_domain="files.example.com")
update_bucket(s3, "production-data", snapshot_enabled=True)

# Back to the defaults
update_bucket(s3, "assets", access="private", cache_control="", regions=[])
```

The result is `{"name", "request_id", "raw"}`. Read the new values back with [`get_bucket_metadata`](bucket-info.md) (`storage_class`, `regions`, `cache_control`, `custom_domain`, `allow_object_acl`, `snapshot_enabled`, and `additional_http_headers` under `raw`), and boto3's `get_bucket_acl` or [`list_buckets(include_visibility=True)`](listing.md) for access.

## The settings

| Setting | Values | Effect |
|---|---|---|
| `access` | `"public"`, `"private"` | `public` grants anonymous reads on `https://<bucket>.t3.tigrisfiles.io/<key>`, the [public bucket domain](https://www.tigrisdata.com/docs/buckets/public-bucket/); `private` revokes them. |
| `public_list_objects` | `True`, `False` | Whether anonymous requests may list the bucket. Only takes effect on a public bucket. |
| `allow_object_acl` | `True`, `False` | Whether individual objects may carry their own public/private ACL. |
| `default_storage_class` | `STANDARD`, `STANDARD_IA`, `GLACIER`, `GLACIER_IR` | Storage class for objects uploaded without one of their own. Existing objects keep theirs. |
| `regions` | a region code (`"iad"`), a multi-region code (`"usa"`, `"eur"`), a list of codes (`["iad", "sjc"]`), or `[]` / `""` for global | Where new data is placed. Codes are validated by the gateway. |
| `cache_control` | a `Cache-Control` value, or `""` to clear | Default `Cache-Control` header served with objects that have none of their own. |
| `custom_domain` | a hostname | Serve the bucket from your own domain. The domain's CNAME must already point at the bucket, or the update is rejected with `InvalidArgument`. |
| `nosniff` | `True`, `False` | Serve objects with `X-Content-Type-Options: nosniff`. |
| `snapshot_enabled` | `True`, `False` | Switch between a [snapshot bucket](snapshots.md) and a regular one. Refused with `InvalidArgument` when disabling on a fork or a bucket with forks, and when enabling on a bucket with [lifecycle rules](lifecycle.md); a bucket that was ever snapshot-enabled cannot take lifecycle rules afterwards. Existing snapshots are kept while disabled and usable again once re-enabled. `HeadBucket`, metadata and listings show the new type at once; object operations follow within seconds. |

Unknown storage classes and access values raise `ValueError`, non-boolean flags `TypeError`, and a call with no settings `ValueError`, all before any request is sent.

Access changes are visible in `get_bucket_acl` and `list_buckets` at once; the public domain follows asynchronously, usually within seconds.

## Endpoint

| Request | Meaning |
|---|---|
| `PATCH /{bucket}` with a JSON body | `storage_class`, `object_regions`, `cache_control`, `acl_settings.allow_object_acl`, `website.domain_name`, `additional_http_headers`, `type` (`1` snapshot, `0` regular) |
| `X-Amz-Acl: public-read` / `private` | Access |
| `X-Amz-Acl-Public-List-Objects-Enabled: true` / `false` | Anonymous listing |

Sent as one signed request with `Content-Type: application/json`.

## See also

- [Create bucket options](create-bucket.md): access, storage class, placement and listing at creation time.
- [Bucket info](bucket-info.md): reading the settings back.
- [Snapshots](snapshots.md): what a snapshot bucket can do.
- [Soft delete](soft-delete.md): `set_bucket_soft_delete` is the same update for the soft-delete setting.
