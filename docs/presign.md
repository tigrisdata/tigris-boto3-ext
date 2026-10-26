# Presigned URLs

A presigned URL lets someone download or upload one object for a limited time without credentials. boto3's `generate_presigned_url` and `generate_presigned_post` work against Tigris as they do on S3. Tigris also has its own presign endpoint, which `presign` calls: the URL it returns is served from the public domain (`https://<bucket>.t3.tigrisfiles.io/<key>?…`), private buckets included, it comes with a second URL on the bucket's custom domain when one is configured, and a download can be pinned to the object as it was at a snapshot.

Tigris docs: [Presigned URLs](https://www.tigrisdata.com/docs/objects/presigned/), which covers SDK-side presigning, the 90-day maximum lifetime and signed headers.
Runnable example: [`examples/presign_usage.py`](../examples/presign_usage.py).

It is a Tigris endpoint rather than an S3 operation, so it returns a plain dict and raises `TigrisAPIError` on gateway errors. All names are imported from `tigris_boto3_ext`.

## Sign a download or an upload

```python
from tigris_boto3_ext import presign

download = presign(s3, "assets", "report.pdf", expires_in=600)
download["url"]                 # https://assets.t3.tigrisfiles.io/report.pdf?X-Amz-Algorithm=...
download["custom_domain_url"]   # https://files.example.com/report.pdf?... or None

upload = presign(s3, "assets", "incoming/photo.jpg", "put")
requests.put(upload["url"], data=photo_bytes)

# The object as it was at a snapshot
old = presign(s3, "assets", "config.json", snapshot_version="1792757954482613190")
old["version_id"]               # the object version the snapshot resolved to
```

The result is `{"url", "custom_domain_url", "operation", "expires_in", "version_id"}`. The URL is signed with the client's own access key.

| Argument | Values | Effect |
|---|---|---|
| `operation` | `"get"` (default), `"put"` | Download or upload |
| `expires_in` | seconds, at least 1 (default 3600) | How long the URL stays valid; Tigris allows up to 90 days |
| `snapshot_version` | a snapshot version, with `"get"` only | Sign for the object as it was at that snapshot |

`snapshot_version` is resolved with [`head_object_from_snapshot`](snapshots.md), so it is the gateway that decides which version the object had at that moment; a key that did not exist at the snapshot raises the gateway's 404. The endpoint signs without checking that the key exists, so a download URL for a missing key is returned and fails with 404 when used.

## Endpoint

| Request | Meaning |
|---|---|
| `POST /?func=presign` with `{"bucket", "key", "key_id", "type": "get" / "put", "expires_in", "version_id"?}` | Sign a URL; `version_id` pins a download to one object version |

Sent as one signed request with `Content-Type: application/json`. The response includes the signing key's secret; `presign` does not return it.

## See also

- [Snapshots](snapshots.md): reading objects as they were.
- [Bucket settings](settings.md): the custom domain that `custom_domain_url` uses.
