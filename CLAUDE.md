# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync --all-extras

# Run all tests
uv run pytest tests/ -v

# Run a single test file or test
uv run pytest tests/test_bundle.py -v
uv run pytest tests/test_bundle.py::TestBundleResponse::test_read_delegates -v

# Run integration tests (requires AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
uv run pytest tests/integration/ -v

# Lint and format
uv run ruff check tigris_boto3_ext
uv run ruff check --fix tigris_boto3_ext
uv run ruff format tigris_boto3_ext

# Type checking
uv run mypy tigris_boto3_ext

# Build
uv build
```

## Architecture

This library extends boto3's S3 client with Tigris-specific features (snapshots, forks, bundle API) without modifying boto3 itself.

### Header injection via boto3 events (snapshots + forks)

The snapshot and fork features work by injecting custom `X-Tigris-*` headers into S3 requests through boto3's event system:

1. **`_internal.py`** provides the `HeaderInjector` infrastructure. It registers handlers on `before-sign.s3.<Operation>` events so headers are included in the SigV4 signature. A global registry tracks `(client_id, event_name)` pairs to allow safe nesting of multiple context managers on the same client.

2. **`context_managers.py`** defines `TigrisSnapshotEnabled`, `TigrisSnapshot`, and `TigrisFork` — each wraps `HeaderInjector` to add the appropriate headers for their operation.

3. **`decorators.py`** wraps the context managers as function decorators (`@snapshot_enabled`, `@with_snapshot`, `@forked_from`).

4. **`helpers.py`** provides high-level convenience functions (`create_snapshot_bucket`, `create_fork`, `get_bucket_info`, etc.) that use context managers internally.

### Bundle API (direct HTTP)

`bundle.py` bypasses boto3 entirely. It uses urllib3 directly with manual SigV4 signing to POST to `/{bucket}?bundle`. This is because the bundle endpoint is a Tigris extension (not an S3 operation) that streams a tar archive response. `BundleResponse` wraps the streaming response as a file-like object compatible with `tarfile.open(mode="r|")`.

### Key Tigris headers

- `X-Tigris-Enable-Snapshot: true` — enable snapshots on bucket creation
- `X-Tigris-Snapshot: <bucket>` / `X-Tigris-Snapshot: true; name=<name>` — list/create snapshots
- `X-Tigris-Snapshot-Version: <version>` — read from a specific snapshot
- `X-Tigris-Fork-Source-Bucket` / `X-Tigris-Fork-Source-Bucket-Snapshot` — fork source
- `X-Tigris-Bundle-Format`, `X-Tigris-Bundle-Compression`, `X-Tigris-Bundle-On-Error` — bundle request config

## Test structure

- **Unit tests** (`tests/test_*.py`): Mock boto3 clients and urllib3. Fast, no network.
- **Integration tests** (`tests/integration/`): Run against real Tigris. Skipped automatically when env vars are not set. Use `cleanup_buckets` fixture for automatic teardown. Bucket names are prefixed with `tigris-boto3-ext-test-` plus a UUID.

## Release process

1. Bump version in `pyproject.toml` and `tigris_boto3_ext/__init__.py`
2. Merge via PR (main is protected)
3. Tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z"` and push tag
4. GitHub Actions builds, publishes to PyPI, and creates a GitHub release
