# Object notifications

Tigris can tell your application when objects in a bucket change: it POSTs a JSON payload to a webhook of yours for every create, delete, rename and storage-class transition, optionally filtered to the events you care about and authenticated with a bearer token or basic credentials. `set_bucket_notifications` configures that webhook for a bucket.

Tigris docs: [Object notifications](https://www.tigrisdata.com/docs/buckets/object-notifications/) for the payload format, delivery guarantees and pricing; [metadata querying](https://www.tigrisdata.com/docs/objects/query-metadata/) for the filter syntax.
Runnable example: [`examples/notifications_usage.py`](../examples/notifications_usage.py).

It is a Tigris endpoint rather than an S3 operation, so it returns a plain dict and raises `TigrisAPIError` on gateway errors. All names are imported from `tigris_boto3_ext`.

## Configure a webhook

```python
from tigris_boto3_ext import get_bucket_metadata, set_bucket_notifications

# Every event, authenticated with a bearer token
set_bucket_notifications(s3, "uploads", "https://api.example.com/hooks/tigris", token="s3cr3t")

# Read it back; credentials come back masked
get_bucket_metadata(s3, "uploads")["notifications"]
# {'enabled': True, 'url': 'https://api.example.com/hooks/tigris', 'filter': None, 'auth': 'token'}

# Only deletes, with basic authentication
set_bucket_notifications(
    s3, "uploads", "https://api.example.com/hooks/tigris",
    event_filter='WHERE `Event-Type` = "OBJECT_DELETED"',
    username="tigris", password="s3cr3t",
)

# Pause delivery, keeping the configuration
set_bucket_notifications(s3, "uploads", "https://api.example.com/hooks/tigris", enabled=False, token="s3cr3t")

# Remove the configuration
set_bucket_notifications(s3, "uploads", None)
```

Each call replaces the whole configuration, so repeat the URL and credentials when you change one thing. `url` must be `https://` with a host; `token` and `username`/`password` are mutually exclusive, and `username` and `password` go together. These are checked before any request is sent (`ValueError`); the gateway checks the filter syntax (`InvalidArgument`).

| Argument | Values | Effect |
|---|---|---|
| `url` | `https://…`, or `None` | Where events are POSTed; `None` removes the configuration |
| `enabled` | `True` (default), `False` | `False` keeps the configuration but stops delivery |
| `event_filter` | Tigris's SQL-like filter | Only matching events are delivered; omitted means all events |
| `token` | string | Sent as `Authorization: Bearer <token>` |
| `username`, `password` | strings | Sent as HTTP basic authentication |

Filters use the same syntax as [metadata querying](https://www.tigrisdata.com/docs/objects/query-metadata/), plus an `Event-Type` field with the event names `OBJECT_CREATED_PUT`, `OBJECT_CREATED_MULTIPART`, `OBJECT_CREATED_COPY`, `OBJECT_DELETED`, `OBJECT_TRANSITIONED` and `OBJECT_RENAME`:

```sql
WHERE `key` REGEXP "^images/"
WHERE `Event-Type` = "OBJECT_DELETED"
```

## Delivery

Tigris POSTs a JSON body with an `events` array; each event carries `eventName`, `eventTime`, `bucket` and `object` (`key`, `size`, `eTag`). A 2xx response acknowledges the batch; anything else, or no answer within 10 seconds, is retried. Delivery is at least once and events can arrive out of order, so make your handler idempotent and use the object's `Last-Modified` when order matters.

## Endpoint

| Request | Meaning |
|---|---|
| `PATCH /{bucket}` with `{"object_notifications": {"enabled", "web_hook", "filter", "auth"}}` | Configure; `auth` is `{"token"}` or `{"basic_user", "basic_pass"}` |
| `PATCH /{bucket}` with `{"object_notifications": {}}` | Remove |

Sent as one signed request with `Content-Type: application/json`.

## See also

- [Bucket info](bucket-info.md): `get_bucket_metadata` reads the configuration back under `notifications`.
- [Bucket settings](settings.md): the other settings on the same endpoint.
