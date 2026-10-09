# Bundle API

The Bundle API fetches many objects in one HTTP request. You send a list of keys and get back a tar archive streamed as it is assembled, with one entry per object named by its full key. It exists for workloads that read thousands of small objects per step, such as ML data loaders, where per-object requests are the bottleneck.

Tigris docs: [Bundle API](https://www.tigrisdata.com/docs/objects/bundle/).
Runnable example: [`examples/bundle_usage.py`](../examples/bundle_usage.py).

Unlike the rest of this library, the bundle endpoint is not an S3 operation, so `bundle_objects` does not go through boto3's event system. It signs a `POST /{bucket}?bundle` request with the client's own credentials, region and endpoint and streams the response with urllib3. Everything below is imported from `tigris_boto3_ext`.

## Fetch a bundle

```python
import tarfile
from tigris_boto3_ext import bundle_objects

keys = [f"dataset/train/img_{i:05d}.jpg" for i in range(1000)]

with bundle_objects(s3, "my-dataset-bucket", keys) as response:
    with tarfile.open(fileobj=response, mode="r|") as tar:
        for member in tar:
            if member.name == "__bundle_errors.json":
                continue
            f = tar.extractfile(member)
            if f is not None:
                image_bytes = f.read()
```

`BundleResponse` is file-like (`read`, `close`, context manager), so it drops straight into `tarfile.open(mode="r|")`, the streaming mode. Up to `MAX_BUNDLE_KEYS` (5,000) keys per call; the library raises `ValueError` beyond that, and for an empty bucket name or key list.

## Missing objects: skip or fail

`on_error` decides what happens when a key does not exist.

- `BUNDLE_ON_ERROR_SKIP` (default): the bundle is returned without the missing objects, and the archive ends with an `__bundle_errors.json` entry listing them. Read it to know what was skipped:

  ```python
  import json

  errors = None
  with bundle_objects(s3, "my-bucket", keys) as response, tarfile.open(fileobj=response, mode="r|") as tar:
      for member in tar:
          f = tar.extractfile(member)
          if f is None:
              continue
          if member.name == "__bundle_errors.json":
              errors = json.loads(f.read())
          else:
              ...
  for entry in (errors or {}).get("skipped", []):
      print(entry["key"], entry["reason"])
  ```

- `BUNDLE_ON_ERROR_FAIL`: the request fails as a whole and `bundle_objects` raises `BundleError`, which carries `status_code` and the server's `body`. Use it for inference, where every object must be present.

  ```python
  from tigris_boto3_ext import BUNDLE_ON_ERROR_FAIL, BundleError

  try:
      response = bundle_objects(s3, "my-bucket", keys, on_error=BUNDLE_ON_ERROR_FAIL)
  except BundleError as e:
      print(f"HTTP {e.status_code}: {e.body}")
  ```

## Compression

`compression` is `BUNDLE_COMPRESSION_NONE` (default), `BUNDLE_COMPRESSION_GZIP` or `BUNDLE_COMPRESSION_ZSTD`. The archive is compressed as a whole, so decompress before handing it to `tarfile`:

```python
import gzip

with bundle_objects(s3, "my-bucket", keys, compression="gzip") as response:
    with gzip.open(response, "rb") as gz, tarfile.open(fileobj=gz, mode="r|") as tar:
        ...
```

zstd is not in the standard library; use the [`zstandard`](https://pypi.org/project/zstandard/) package (`zstandard.ZstdDecompressor().stream_reader(response)`).

## Response metadata

After the stream has been read, the response exposes the server's counters, each `None` if the header was not sent:

| Property | Header | Meaning |
|---|---|---|
| `response.object_count` | `x-tigris-bundle-count` | Objects included in the bundle |
| `response.bundle_bytes` | `x-tigris-bundle-bytes` | Total bytes of object data |
| `response.skipped_count` | `x-tigris-bundle-skipped` | Keys skipped in skip mode |

`response.status_code`, `response.content_type` and `response.headers` (lower-cased keys) are also available.

## Headers

| Header | Value |
|---|---|
| `X-Tigris-Bundle-Format` | `tar` |
| `X-Tigris-Bundle-Compression` | `none`, `gzip` or `zstd` |
| `X-Tigris-Bundle-On-Error` | `skip` or `fail` |

Server-side limits (see the Tigris docs for current values): 5,000 keys per request, 5 MB request body, 50 GB assembled archive, 15-minute request timeout.
