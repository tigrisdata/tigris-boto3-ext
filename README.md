# tigris-boto3-ext

[![CI](https://github.com/tigrisdata/tigris-boto3-ext/actions/workflows/ci.yml/badge.svg)](https://github.com/tigrisdata/tigris-boto3-ext/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/tigrisdata/tigris-boto3-ext/branch/main/graph/badge.svg)](https://codecov.io/gh/tigrisdata/tigris-boto3-ext)
[![Python Version](https://img.shields.io/pypi/pyversions/tigris-boto3-ext.svg)](https://pypi.org/project/tigris-boto3-ext/)
[![PyPI version](https://badge.fury.io/py/tigris-boto3-ext.svg)](https://badge.fury.io/py/tigris-boto3-ext)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Extend boto3 with Tigris-specific features — snapshots, forks, soft delete, in-place rename and the Bundle API — while keeping full boto3 compatibility. You keep your `boto3.client("s3")`; this library adds the Tigris headers to the right requests and otherwise stays out of the way.

## Installation

```bash
pip install tigris-boto3-ext
```

Requires Python 3.9+ and boto3 >= 1.26.0.

## Quick start

```python
import boto3
from tigris_boto3_ext import (
    create_snapshot,
    create_snapshot_bucket,
    get_object_from_snapshot,
    get_snapshot_version,
)

s3 = boto3.client("s3", endpoint_url="https://t3.storage.dev")

create_snapshot_bucket(s3, "my-bucket")
s3.put_object(Bucket="my-bucket", Key="config.json", Body=b'{"version": 1}')
version = get_snapshot_version(create_snapshot(s3, "my-bucket", snapshot_name="v1"))

s3.put_object(Bucket="my-bucket", Key="config.json", Body=b'{"version": 2}')
old = get_object_from_snapshot(s3, "my-bucket", "config.json", version)["Body"].read()
# b'{"version": 1}'
```

Credentials come from the usual boto3 sources: environment variables, a profile, or keyword arguments to `boto3.client`.

## Features

Everything is imported from `tigris_boto3_ext`. Each page lists the feature's helpers, context manager and decorator, the headers it sends, and its caveats.

| Feature | What it does | Start with | Docs | Example |
|---|---|---|---|---|
| Snapshots | Point-in-time copies of a bucket: create, list, read from, delete | `create_snapshot_bucket`, `create_snapshot`, `get_object_from_snapshot` | [docs/snapshots.md](docs/snapshots.md) | [snapshots_usage.py](examples/snapshots_usage.py) |
| Forks | A new bucket from an existing bucket or one of its snapshots, zero-copy | `create_fork` | [docs/forks.md](docs/forks.md) | [forks_usage.py](examples/forks_usage.py) |
| Bucket info | Snapshot and fork metadata from `HeadBucket` | `get_bucket_info`, `has_snapshot_enabled` | [docs/bucket-info.md](docs/bucket-info.md) | [bucket_info_usage.py](examples/bucket_info_usage.py) |
| Object rename | Move an object to a new key without rewriting its data | `rename_object` | [docs/rename.md](docs/rename.md) | [rename_usage.py](examples/rename_usage.py) |
| Soft delete | Deleted objects stay recoverable for 7–90 days: list, restore, purge | `create_soft_delete_bucket`, `restore_deleted_object` | [docs/soft-delete.md](docs/soft-delete.md) | [soft_delete_usage.py](examples/soft_delete_usage.py) |
| Force delete | Delete a bucket that still contains objects | `force_delete_bucket` | [docs/force-delete.md](docs/force-delete.md) | [force_delete_usage.py](examples/force_delete_usage.py) |
| Bundle API | Thousands of objects in one request, as a streaming tar archive | `bundle_objects` | [docs/bundle.md](docs/bundle.md) | [bundle_usage.py](examples/bundle_usage.py) |

## Usage patterns

Snapshots, forks, rename and soft delete each come in three shapes. They do the same thing; pick by how much code the Tigris behaviour should cover. Bucket info, force delete and the Bundle API are single calls and come as helpers only.

### Helper functions

One call, one request. The header is registered on the client for the duration of that call and removed before it returns, so sequential code never sees it; concurrent code on the same client can, see [Thread safety](#thread-safety). The default choice.

```python
from tigris_boto3_ext import rename_object

rename_object(s3, "my-bucket", "old-name.txt", "new-name.txt")
```

### Context managers

Several plain boto3 calls under one Tigris setting. Reach for this when you have a batch to do and the helpers would mean repeating yourself.

```python
from tigris_boto3_ext import TigrisSnapshot

with TigrisSnapshot(s3, "my-bucket", snapshot_version=version):
    config = s3.get_object(Bucket="my-bucket", Key="config.json")
    listing = s3.list_objects_v2(Bucket="my-bucket", Prefix="data/")
```

### Decorators

Wrap an existing function so its boto3 calls run under the setting, without changing its body. The wrapped function must take the client as its first argument.

```python
from tigris_boto3_ext import forked_from

@forked_from("production-data")
def create_dev_environment(s3_client, name):
    return s3_client.create_bucket(Bucket=name)

create_dev_environment(s3, "dev-environment")
```

Decorators without options (`@snapshot_enabled`, `@with_rename`, `@with_soft_delete_view`) are applied bare. The others are called: `@with_snapshot("bucket")` and `@forked_from("source-bucket")` need their bucket, and `@soft_delete_enabled(retention_days=30)` takes an optional window, so `@soft_delete_enabled()` with empty parentheses is also valid.

## How it works

boto3 lets you hook into a request just before it is signed. This library registers handlers on the client's `before-sign.s3.<Operation>` events and adds the `X-Tigris-*` header the feature needs, so the header is covered by the SigV4 signature and the request is otherwise a normal S3 call. When the helper returns, or the context manager or decorated function exits, the handler is removed. Each feature page lists its headers.

Contexts nest. When two active contexts set the same header on the same operation, the inner one wins.

The Bundle API is the exception: `/{bucket}?bundle` is not an S3 operation, so `bundle_objects` signs and sends the request itself, using the client's credentials, region and endpoint.

### Thread safety

Header injection is registered on the boto3 client, not on a single request. While a context manager, decorator or helper from this library is active, every matching operation on that client carries the injected headers, including operations issued from other threads. Use a separate client per thread when combining these features with concurrent work.

Helpers whose header changes what a destructive request does go one step further: they raise `RuntimeError` instead of running while another header injector is active for the same operation on the same client, rather than risk the wrong header on the wrong request. `delete_snapshot` and `force_delete_bucket` both send `DeleteBucket`, so they exclude each other; `restore_deleted_object` guards `RestoreObject`. Unrelated operations are not blocked.

## Development

### Setup

```bash
git clone https://github.com/tigrisdata/tigris-boto3-ext.git
cd tigris-boto3-ext

# Install with dev dependencies using uv
uv sync --all-extras

# Or with pip
pip install -e ".[dev]"
```

### Running tests

```bash
# Unit tests: mocked clients, no network
uv run pytest tests/ --ignore=tests/integration -v

# Integration tests run against Tigris and are skipped when these are not set
export AWS_ENDPOINT_URL_S3="https://t3.storage.dev"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
uv run pytest tests/integration/ -v
```

See [`tests/integration/README.md`](tests/integration/README.md) for the integration test setup in detail.

### Code quality

```bash
# Type checking
uv run mypy tigris_boto3_ext

# Linting
uv run ruff check tigris_boto3_ext

# Auto-fix linting issues
uv run ruff check --fix tigris_boto3_ext

# Code formatting
uv run ruff format tigris_boto3_ext

# Check formatting without making changes
uv run ruff format --check tigris_boto3_ext
```

## License

Apache-2.0

## Contributing

Contributions welcome! Please open an issue or PR on GitHub.

A feature ships as a set: its module under `tigris_boto3_ext/buckets/` or `tigris_boto3_ext/objects/`, a unit test, an integration test, a page under `docs/`, an example under `examples/`, and a row in the table above. [`CLAUDE.md`](CLAUDE.md) describes the module layout.

## Support

For issues and questions:

- GitHub Issues: <https://github.com/tigrisdata/tigris-boto3-ext/issues>
- Documentation: <https://www.tigrisdata.com/docs>
